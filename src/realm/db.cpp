/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/transaction.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <sstream>
#include <type_traits>
#include <random>
#include <deque>
#include <thread>
#include <chrono>
#include <condition_variable>

#include <realm/disable_sync_to_disk.hpp>
#include <realm/group_writer.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/replication.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/features.h>
#include <realm/util/file_mapper.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/to_string.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>
#else
#include <windows.h>
#include <process.h>
#endif

// #define REALM_ENABLE_LOGFILE


using namespace realm;
using namespace realm::util;
using Durability = DBOptions::Durability;

namespace {

// value   change
// --------------------
//  4      Unknown
//  5      Introduction of SharedInfo::file_format_version and
//         SharedInfo::history_type.
//  6      Using new robust mutex emulation where applicable
//  7      Introducing `commit_in_critical_phase` and `sync_agent_present`, and
//         changing `daemon_started` and `daemon_ready` from 1-bit to 8-bit
//         fields.
//  8      Placing the commitlog history inside the Realm file.
//  9      Fair write transactions requires an additional condition variable,
//         `write_fairness`
// 10      Introducing SharedInfo::history_schema_version.
// 11      New impl of InterprocessCondVar on windows.
// 12      Change `number_of_versions` to an atomic rather than guarding it
//         with a lock.
// 13      New impl of VersionList and added mutex for it (former RingBuffer)
// 14      Added field for tracking ongoing encrypted writes
const uint_fast16_t g_shared_info_version = 14;


struct VersionList {
    // the VersionList is an array of ReadCount structures.
    // it is placed in the "lock-file" and accessed via memory mapping
    struct ReadCount {
        uint64_t version;
        uint64_t filesize;
        uint64_t current_top;
        uint32_t count_live;
        uint32_t count_frozen;
        uint32_t count_full;
        bool is_active()
        {
            return version != 0;
        }
        void deactivate()
        {
            version = 0;
            count_live = count_frozen = count_full = 0;
        }
        void activate(uint64_t v)
        {
            version = v;
        }
    };

    void reserve(uint32_t size) noexcept
    {
        for (auto i = entries; i < size; ++i)
            data()[i].deactivate();
        if (size > entries) {
            // Fence preventing downward motion of above writes
            std::atomic_signal_fence(std::memory_order_release);
            entries = size;
        }
    }

    VersionList() noexcept
    {
        newest = nil; // empty
        entries = 0;
        reserve(init_readers_size);
    }

    static size_t compute_required_space(uint_fast32_t num_entries) noexcept
    {
        // get space required for given number of entries beyond the initial count.
        // NB: this not the size of the VersionList, it is the size minus whatever was
        // the initial size.
        return sizeof(ReadCount) * (num_entries - init_readers_size);
    }

    unsigned int capacity() const noexcept
    {
        return entries;
    }

    ReadCount& get(uint_fast32_t idx) noexcept
    {
        return data()[idx];
    }

    ReadCount& get_newest() noexcept
    {
        return get(newest);
    }
    // returns nullptr if all entries are in use
    ReadCount* try_allocate_entry(uint64_t top, uint64_t size, uint64_t version)
    {
        auto i = allocating.load();
        if (i == newest.load()) {
            // if newest != allocating we are recovering from a crash and MUST complete the earlier allocation
            // but if not, find lowest free entry by linear search.
            uint32_t k = 0;
            while (k < entries && data()[k].is_active()) {
                ++k;
            }
            if (k == entries)
                return nullptr;     // no free entries
            allocating.exchange(k); // barrier: prevent upward movement of instructions below
            i = k;
        }
        auto& rc = data()[i];
        REALM_ASSERT(rc.count_frozen == 0);
        REALM_ASSERT(rc.count_live == 0);
        REALM_ASSERT(rc.count_full == 0);
        rc.current_top = top;
        rc.filesize = size;
        rc.activate(version);
        newest.store(i); // barrier: prevent downward movement of instructions above
        return &rc;
    }

    uint32_t index_of(const ReadCount& rc) noexcept
    {
        return (uint32_t)(&rc - data());
    }

    void free_entry(ReadCount* rc) noexcept
    {
        rc->current_top = rc->filesize = -1ULL; // easy to recognize in debugger
        rc->deactivate();
    }

    // This method resets the version list to an empty state, then allocates an entry.
    // Precondition: This should *only* be done if the caller has established that she
    // is the only thread/process that has access to the VersionList. It is currently
    // called from init_versioning(), which is called by DB::open() under the
    // condition that it is the session initiator and under guard by the control mutex,
    // thus ensuring the precondition. It is also called from compact() in a similar situation.
    // It is most likely not suited for any other use.
    ReadCount& init_versioning(uint64_t top, uint64_t filesize, uint64_t version) noexcept
    {
        newest = nil;
        allocating = 0;
        auto t_free = entries;
        entries = 0;
        reserve(t_free);
        return *try_allocate_entry(top, filesize, version);
    }

    void purge_versions(uint64_t& oldest_live_v, TopRefMap& top_refs, bool& any_new_unreachables)
    {
        oldest_live_v = std::numeric_limits<uint64_t>::max();
        auto oldest_full_v = std::numeric_limits<uint64_t>::max();
        any_new_unreachables = false;
        // correct case where an earlier crash may have left the entry at 'allocating' partially initialized:
        const auto index_of_newest = newest.load();
        if (auto a = allocating.load(); a != index_of_newest) {
            data()[a].deactivate();
        }
        // determine fully locked versions - after one of those all versions are considered live.
        for (auto* rc = data(); rc < data() + entries; ++rc) {
            if (!rc->is_active())
                continue;
            if (rc->count_full) {
                if (rc->version < oldest_full_v)
                    oldest_full_v = rc->version;
            }
        }
        // collect reachable versions and determine oldest live reachable version
        // (oldest reachable version is the first entry in the top_refs map, so no need to find it explicitly)
        for (auto* rc = data(); rc < data() + entries; ++rc) {
            if (!rc->is_active())
                continue;
            if (rc->count_frozen || rc->count_live || rc->version >= oldest_full_v) {
                // entry is still reachable
                top_refs.emplace(rc->version, VersionInfo{to_ref(rc->current_top), to_ref(rc->filesize)});
            }
            if (rc->count_live || rc->version >= oldest_full_v) {
                if (rc->version < oldest_live_v)
                    oldest_live_v = rc->version;
            }
        }
        // we must have found at least one reachable version
        REALM_ASSERT(top_refs.size());
        // free unreachable entries and determine if we want to trigger backdating
        uint64_t oldest_v = top_refs.begin()->first;
        for (auto* rc = data(); rc < data() + entries; ++rc) {
            if (!rc->is_active())
                continue;
            if (rc->count_frozen == 0 && rc->count_live == 0 && rc->version < oldest_full_v) {
                // entry is becoming unreachable.
                // if it is also younger than a reachable version, then set 'any_new_unreachables' to trigger
                // backdating
                if (rc->version > oldest_v) {
                    any_new_unreachables = true;
                }
                REALM_ASSERT(index_of(*rc) != index_of_newest);
                free_entry(rc);
            }
        }
        REALM_ASSERT(oldest_v != std::numeric_limits<uint64_t>::max());
        REALM_ASSERT(oldest_live_v != std::numeric_limits<uint64_t>::max());
    }

#if REALM_DEBUG
    void dump()
    {
        util::format(std::cout, "VersionList has %1 entries: \n", entries);
        for (auto* rc = data(); rc < data() + entries; ++rc) {
            util::format(std::cout, "[%1]: version %2, live: %3, full: %4, frozen: %5\n", index_of(*rc), rc->version,
                         rc->count_live, rc->count_full, rc->count_frozen);
        }
    }
#endif // REALM_DEBUG

    constexpr static uint32_t nil = (uint32_t)-1;
    const static int init_readers_size = 32;
    uint32_t entries;
    std::atomic<uint32_t> allocating; // atomic for crash safety, not threading
    std::atomic<uint32_t> newest;     // atomic for crash safety, not threading

    // IMPORTANT: The actual data comprising the version list MUST BE PLACED LAST in
    // the VersionList structure, as the data area is extended at run time.
    // Similarly, the VersionList must be the final element of the SharedInfo structure.
    // IMPORTANT II:
    // To ensure proper alignment across all platforms, the SharedInfo structure
    // should NOT have a stricter alignment requirement than the ReadCount structure.
    ReadCount m_data[init_readers_size];

    // Silence UBSan errors about out-of-bounds reads on m_data by casting to a pointer
    ReadCount* data() noexcept
    {
        return m_data;
    }
    const ReadCount* data() const noexcept
    {
        return m_data;
    }
};

// Using lambda rather than function so that shared_ptr shared state doesn't need to hold a function pointer.
constexpr auto TransactionDeleter = [](Transaction* t) {
    t->close();
    delete t;
};

template <typename... Args>
TransactionRef make_transaction_ref(Args&&... args)
{
    return TransactionRef(new Transaction(std::forward<Args>(args)...), TransactionDeleter);
}

} // anonymous namespace

namespace realm {

/// The structure of the contents of the per session `.lock` file. Note that
/// this file is transient in that it is recreated/reinitialized at the
/// beginning of every session. A session is any sequence of temporally
/// overlapping openings of a particular Realm file via DB objects. For
/// example, if there are two DB objects, A and B, and the file is
/// first opened via A, then opened via B, then closed via A, and finally closed
/// via B, then the session streaches from the opening via A to the closing via
/// B.
///
/// IMPORTANT: Remember to bump `g_shared_info_version` if anything is changed
/// in the memory layout of this class, or if the meaning of any of the stored
/// values change.
///
/// Members `init_complete`, `shared_info_version`, `size_of_mutex`, and
/// `size_of_condvar` may only be modified only while holding an exclusive lock
/// on the file, and may be read only while holding a shared (or exclusive) lock
/// on the file. All other members (except for the VersionList which has its own mutex)
/// may be accessed only while holding a lock on `controlmutex`.
///
/// SharedInfo must be 8-byte aligned. On 32-bit Apple platforms, mutexes store their
/// alignment as part of the mutex state. We're copying the SharedInfo (including
/// embedded but alway unlocked mutexes) and it must retain the same alignment
/// throughout.
struct alignas(8) DB::SharedInfo {
    /// Indicates that initialization of the lock file was completed
    /// sucessfully.
    ///
    /// CAUTION: This member must never move or change type, as that would
    /// compromize safety of the the session initiation process.
    std::atomic<uint8_t> init_complete; // Offset 0

    /// The size in bytes of a mutex member of SharedInfo. This allows all
    /// session participants to be in agreement. Obviously, a size match is not
    /// enough to guarantee identical layout internally in the mutex object, but
    /// it is hoped that it will catch some (if not most) of the cases where
    /// there is a layout discrepancy internally in the mutex object.
    uint8_t size_of_mutex; // Offset 1

    /// Like size_of_mutex, but for condition variable members of SharedInfo.
    uint8_t size_of_condvar; // Offset 2

    /// Set during the critical phase of a commit, when the logs, the VersionList
    /// and the database may be out of sync with respect to each other. If a
    /// writer crashes during this phase, there is no safe way of continuing
    /// with further write transactions. When beginning a write transaction,
    /// this must be checked and an exception thrown if set.
    ///
    /// Note that std::atomic<uint8_t> is guaranteed to have standard layout.
    std::atomic<uint8_t> commit_in_critical_phase = {0}; // Offset 3

    /// The target Realm file format version for the current session. This
    /// allows all session participants to be in agreement. It can only differ
    /// from what is returned by Group::get_file_format_version() temporarily,
    /// and only during the Realm file opening process. If it differs, it means
    /// that the file format needs to be upgraded from its current format
    /// (Group::get_file_format_version()), the format specified by this member
    /// of SharedInfo.
    uint8_t file_format_version; // Offset 4

    /// Stores a value of type Replication::HistoryType. Must match across all
    /// session participants.
    int8_t history_type; // Offset 5

    /// The SharedInfo layout version. This allows all session participants to
    /// be in agreement. Must be bumped if the layout of the SharedInfo
    /// structure is changed. Note, however, that only the part that lies beyond
    /// SharedInfoUnchangingLayout can have its layout changed.
    ///
    /// CAUTION: This member must never move or change type, as that would
    /// compromize version agreement checking.
    uint16_t shared_info_version = g_shared_info_version; // Offset 6

    uint16_t durability;           // Offset 8
    uint16_t free_write_slots = 0; // Offset 10

    /// Number of participating shared groups
    uint32_t num_participants = 0; // Offset 12

    /// Latest version number. Guarded by the controlmutex (for lock-free
    /// access, use get_version_of_latest_snapshot() instead)
    uint64_t latest_version_number; // Offset 16

    /// Pid of process initiating the session, but only if that process runs
    /// with encryption enabled, zero otherwise. This was used to prevent
    /// multiprocess encryption until support for that was added.
    uint64_t session_initiator_pid = 0; // Offset 24

    std::atomic<uint64_t> number_of_versions; // Offset 32

    /// True (1) if there is a sync agent present (a session participant acting
    /// as sync client). It is an error to have a session with more than one
    /// sync agent. The purpose of this flag is to prevent that from ever
    /// happening. If the sync agent crashes and leaves the flag set, the
    /// session will need to be restarted (lock file reinitialized) before a new
    /// sync agent can be started.
    uint8_t sync_agent_present = 0; // Offset 40

    /// Set when a participant decides to start the daemon, cleared by the
    /// daemon when it decides to exit. Participants check during open() and
    /// start the daemon if running in async mode.
    uint8_t daemon_started = 0; // Offset 41

    /// Set by the daemon when it is ready to handle commits. Participants must
    /// wait during open() on 'daemon_becomes_ready' for this to become true.
    /// Cleared by the daemon when it decides to exit.
    uint8_t daemon_ready = 0; // Offset 42

    uint8_t filler_1; // Offset 43

    /// Stores a history schema version (as returned by
    /// Replication::get_history_schema_version()). Must match across all
    /// session participants.
    uint16_t history_schema_version; // Offset 44

    uint16_t filler_2; // Offset 46

    InterprocessMutex::SharedPart shared_writemutex; // Offset 48
    InterprocessMutex::SharedPart shared_controlmutex;
    InterprocessMutex::SharedPart shared_versionlist_mutex;
    InterprocessCondVar::SharedPart room_to_write;
    InterprocessCondVar::SharedPart work_to_do;
    InterprocessCondVar::SharedPart daemon_becomes_ready;
    InterprocessCondVar::SharedPart new_commit_available;
    InterprocessCondVar::SharedPart pick_next_writer;
    std::atomic<uint32_t> next_ticket;
    std::atomic<uint32_t> next_served = 0;
    std::atomic<uint64_t> writing_page_offset;
    std::atomic<uint64_t> write_counter;

    // IMPORTANT: The VersionList MUST be the last field in SharedInfo - see above.
    VersionList readers;

    SharedInfo(Durability, Replication::HistoryType, int history_schema_version);
    ~SharedInfo() noexcept {}

    void init_versioning(ref_type top_ref, size_t file_size, uint64_t initial_version)
    {
        // Create our first versioning entry:
        readers.init_versioning(top_ref, file_size, initial_version);
    }
};


DB::SharedInfo::SharedInfo(Durability dura, Replication::HistoryType ht, int hsv)
    : size_of_mutex(sizeof(shared_writemutex))
    , size_of_condvar(sizeof(room_to_write))
    , shared_writemutex()   // Throws
    , shared_controlmutex() // Throws
{
    durability = static_cast<uint16_t>(dura); // durability level is fixed from creation
    REALM_ASSERT(!util::int_cast_has_overflow<decltype(history_type)>(ht + 0));
    REALM_ASSERT(!util::int_cast_has_overflow<decltype(history_schema_version)>(hsv));
    history_type = ht;
    history_schema_version = static_cast<uint16_t>(hsv);
    InterprocessCondVar::init_shared_part(new_commit_available); // Throws
    InterprocessCondVar::init_shared_part(pick_next_writer);     // Throws
    next_ticket = 0;

// IMPORTANT: The offsets, types (, and meanings) of these members must
// never change, not even when the SharedInfo layout version is bumped. The
// eternal constancy of this part of the layout is what ensures that a
// joining session participant can reliably verify that the actual format is
// as expected.
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
    static_assert(offsetof(SharedInfo, init_complete) == 0 && ATOMIC_BOOL_LOCK_FREE == 2 &&
                      std::is_same<decltype(init_complete), std::atomic<uint8_t>>::value &&
                      offsetof(SharedInfo, shared_info_version) == 6 &&
                      std::is_same<decltype(shared_info_version), uint16_t>::value,
                  "Forbidden change in SharedInfo layout");

    // Try to catch some of the memory layout changes that requires bumping of
    // the SharedInfo file format version (shared_info_version).
    static_assert(
        offsetof(SharedInfo, size_of_mutex) == 1 && std::is_same<decltype(size_of_mutex), uint8_t>::value &&
            offsetof(SharedInfo, size_of_condvar) == 2 && std::is_same<decltype(size_of_condvar), uint8_t>::value &&
            offsetof(SharedInfo, commit_in_critical_phase) == 3 &&
            std::is_same<decltype(commit_in_critical_phase), std::atomic<uint8_t>>::value &&
            offsetof(SharedInfo, file_format_version) == 4 &&
            std::is_same<decltype(file_format_version), uint8_t>::value && offsetof(SharedInfo, history_type) == 5 &&
            std::is_same<decltype(history_type), int8_t>::value && offsetof(SharedInfo, durability) == 8 &&
            std::is_same<decltype(durability), uint16_t>::value && offsetof(SharedInfo, free_write_slots) == 10 &&
            std::is_same<decltype(free_write_slots), uint16_t>::value &&
            offsetof(SharedInfo, num_participants) == 12 &&
            std::is_same<decltype(num_participants), uint32_t>::value &&
            offsetof(SharedInfo, latest_version_number) == 16 &&
            std::is_same<decltype(latest_version_number), uint64_t>::value &&
            offsetof(SharedInfo, session_initiator_pid) == 24 &&
            std::is_same<decltype(session_initiator_pid), uint64_t>::value &&
            offsetof(SharedInfo, number_of_versions) == 32 &&
            std::is_same<decltype(number_of_versions), std::atomic<uint64_t>>::value &&
            offsetof(SharedInfo, sync_agent_present) == 40 &&
            std::is_same<decltype(sync_agent_present), uint8_t>::value &&
            offsetof(SharedInfo, daemon_started) == 41 && std::is_same<decltype(daemon_started), uint8_t>::value &&
            offsetof(SharedInfo, daemon_ready) == 42 && std::is_same<decltype(daemon_ready), uint8_t>::value &&
            offsetof(SharedInfo, filler_1) == 43 && std::is_same<decltype(filler_1), uint8_t>::value &&
            offsetof(SharedInfo, history_schema_version) == 44 &&
            std::is_same<decltype(history_schema_version), uint16_t>::value && offsetof(SharedInfo, filler_2) == 46 &&
            std::is_same<decltype(filler_2), uint16_t>::value && offsetof(SharedInfo, shared_writemutex) == 48 &&
            std::is_same<decltype(shared_writemutex), InterprocessMutex::SharedPart>::value,
        "Caught layout change requiring SharedInfo file format bumping");
    static_assert(std::atomic<uint64_t>::is_always_lock_free);
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
}

class DB::VersionManager {
public:
    VersionManager(util::InterprocessMutex& mutex)
        : m_mutex(mutex)
    {
    }
    virtual ~VersionManager() {}

    void cleanup_versions(uint64_t& oldest_live_version, TopRefMap& top_refs, bool& any_new_unreachables)
        REQUIRES(!m_info_mutex)
    {
        std::lock_guard lock(m_mutex);
        util::CheckedLockGuard info_lock(m_info_mutex);
        ensure_reader_mapping();
        m_info->readers.purge_versions(oldest_live_version, top_refs, any_new_unreachables);
    }

    version_type get_newest_version() REQUIRES(!m_local_readers_mutex, !m_info_mutex)
    {
        return get_version_id_of_latest_snapshot().version;
    }

    VersionID get_version_id_of_latest_snapshot() REQUIRES(!m_local_readers_mutex, !m_info_mutex)
    {
        {
            // First check the local cache. This is an unlocked read, so it may
            // race with adding a new version. If this happens we'll either see
            // a stale value (acceptable for a racing write on one thread and
            // a read on another), or a new value which is guaranteed to not
            // be an active index in the local cache.
            util::CheckedLockGuard lock(m_local_readers_mutex);
            util::CheckedLockGuard info_lock(m_info_mutex);
            auto index = m_info->readers.newest.load();
            if (index < m_local_readers.size()) {
                auto& r = m_local_readers[index];
                if (r.is_active()) {
                    return {r.version, index};
                }
            }
        }

        std::lock_guard lock(m_mutex);
        util::CheckedLockGuard info_lock(m_info_mutex);
        auto index = m_info->readers.newest.load();
        ensure_reader_mapping(index);
        return {m_info->readers.get(index).version, index};
    }

    void release_read_lock(const ReadLockInfo& read_lock) REQUIRES(!m_local_readers_mutex, !m_info_mutex)
    {
        {
            util::CheckedLockGuard lock(m_local_readers_mutex);
            REALM_ASSERT(read_lock.m_reader_idx < m_local_readers.size());
            auto& r = m_local_readers[read_lock.m_reader_idx];
            auto& f = field_for_type(r, read_lock.m_type);
            REALM_ASSERT(f > 0);
            if (--f > 0)
                return;
            if (r.count_live == 0 && r.count_full == 0 && r.count_frozen == 0)
                r.version = 0;
        }

        std::lock_guard lock(m_mutex);
        util::CheckedLockGuard info_lock(m_info_mutex);
        // we should not need to call ensure_full_reader_mapping,
        // since releasing a read lock means it has been grabbed
        // earlier - and hence must reside in mapped memory:
        REALM_ASSERT(read_lock.m_reader_idx < m_local_max_entry);
        auto& r = m_info->readers.get(read_lock.m_reader_idx);
        REALM_ASSERT(read_lock.m_version == r.version);
        --field_for_type(r, read_lock.m_type);
    }

    ReadLockInfo grab_read_lock(ReadLockInfo::Type type, VersionID version_id = {})
        REQUIRES(!m_local_readers_mutex, !m_info_mutex)
    {
        ReadLockInfo read_lock;
        if (try_grab_local_read_lock(read_lock, type, version_id))
            return read_lock;

        {
            const bool pick_specific = version_id.version != VersionID().version;
            std::lock_guard lock(m_mutex);
            util::CheckedLockGuard info_lock(m_info_mutex);
            auto newest = m_info->readers.newest.load();
            REALM_ASSERT(newest != VersionList::nil);
            read_lock.m_reader_idx = pick_specific ? version_id.index : newest;
            ensure_reader_mapping((unsigned int)read_lock.m_reader_idx);
            bool picked_newest = read_lock.m_reader_idx == (unsigned)newest;
            auto& r = m_info->readers.get(read_lock.m_reader_idx);
            if (pick_specific && version_id.version != r.version)
                throw BadVersion(version_id.version);
            if (!picked_newest) {
                if (type == ReadLockInfo::Frozen && r.count_frozen == 0 && r.count_live == 0)
                    throw BadVersion(version_id.version);
                if (type != ReadLockInfo::Frozen && r.count_live == 0)
                    throw BadVersion(version_id.version);
            }
            populate_read_lock(read_lock, r, type);
        }

        {
            util::CheckedLockGuard local_lock(m_local_readers_mutex);
            grow_local_cache(read_lock.m_reader_idx + 1);
            auto& r2 = m_local_readers[read_lock.m_reader_idx];
            if (!r2.is_active()) {
                r2.version = read_lock.m_version;
                r2.filesize = read_lock.m_file_size;
                r2.current_top = read_lock.m_top_ref;
                r2.count_full = r2.count_live = r2.count_frozen = 0;
            }
            REALM_ASSERT_EX(field_for_type(r2, type) == 0, type, r2.count_full, r2.count_live, r2.count_frozen);
            field_for_type(r2, type) = 1;
        }

        return read_lock;
    }

    void init_versioning(ref_type top_ref, size_t file_size, uint64_t initial_version) REQUIRES(!m_info_mutex)
    {
        std::lock_guard lock(m_mutex);
        util::CheckedLockGuard info_lock(m_info_mutex);
        m_info->init_versioning(top_ref, file_size, initial_version);
    }

    void add_version(ref_type new_top_ref, size_t new_file_size, uint64_t new_version) REQUIRES(!m_info_mutex)
    {
        std::lock_guard lock(m_mutex);
        util::CheckedLockGuard info_lock(m_info_mutex);
        ensure_reader_mapping();
        if (m_info->readers.try_allocate_entry(new_top_ref, new_file_size, new_version)) {
            return;
        }
        // allocation failed, expand VersionList (and lockfile) and retry
        auto entries = m_info->readers.capacity();
        auto new_entries = entries + 32;
        expand_version_list(new_entries);
        m_local_max_entry = new_entries;
        m_info->readers.reserve(new_entries);
        auto success = m_info->readers.try_allocate_entry(new_top_ref, new_file_size, new_version);
        REALM_ASSERT_EX(success, new_entries, new_version);
    }


private:
    void grow_local_cache(size_t new_size) REQUIRES(m_local_readers_mutex)
    {
        if (new_size > m_local_readers.size())
            m_local_readers.resize(new_size, VersionList::ReadCount{});
    }

    void populate_read_lock(ReadLockInfo& read_lock, VersionList::ReadCount& r, ReadLockInfo::Type type)
    {
        ++field_for_type(r, type);
        read_lock.m_type = type;
        read_lock.m_version = r.version;
        read_lock.m_top_ref = static_cast<ref_type>(r.current_top);
        read_lock.m_file_size = static_cast<size_t>(r.filesize);
    }

    bool try_grab_local_read_lock(ReadLockInfo& read_lock, ReadLockInfo::Type type, VersionID version_id)
        REQUIRES(!m_local_readers_mutex, !m_info_mutex)
    {
        const bool pick_specific = version_id.version != VersionID().version;
        auto index = version_id.index;
        if (!pick_specific) {
            util::CheckedLockGuard lock(m_info_mutex);
            index = m_info->readers.newest.load();
        }
        util::CheckedLockGuard local_lock(m_local_readers_mutex);
        if (index >= m_local_readers.size())
            return false;

        auto& r = m_local_readers[index];
        if (!r.is_active())
            return false;
        if (pick_specific && r.version != version_id.version)
            return false;
        if (field_for_type(r, type) == 0)
            return false;

        read_lock.m_reader_idx = index;
        populate_read_lock(read_lock, r, type);
        return true;
    }

    static uint32_t& field_for_type(VersionList::ReadCount& r, ReadLockInfo::Type type)
    {
        switch (type) {
            case ReadLockInfo::Frozen:
                return r.count_frozen;
            case ReadLockInfo::Live:
                return r.count_live;
            case ReadLockInfo::Full:
                return r.count_full;
            default:
                REALM_UNREACHABLE(); // silence a warning
        }
    }

    void mark_page_for_writing(uint64_t page_offset) REQUIRES(!m_info_mutex)
    {
        util::CheckedLockGuard info_lock(m_info_mutex);
        m_info->writing_page_offset = page_offset + 1;
        m_info->write_counter++;
    }
    void clear_writing_marker() REQUIRES(!m_info_mutex)
    {
        util::CheckedLockGuard info_lock(m_info_mutex);
        m_info->write_counter++;
        m_info->writing_page_offset = 0;
    }
    // returns false if no page is marked.
    // if a page is marked, returns true and optionally the offset of the page marked for writing
    // in all cases returns optionally the write counter
    bool observe_writer(uint64_t* page_offset, uint64_t* write_counter) REQUIRES(!m_info_mutex)
    {
        util::CheckedLockGuard info_lock(m_info_mutex);
        if (write_counter) {
            *write_counter = m_info->write_counter;
        }
        uint64_t marked = m_info->writing_page_offset;
        if (marked && page_offset) {
            *page_offset = marked - 1;
        }
        return marked != 0;
    }

protected:
    util::InterprocessMutex& m_mutex;
    util::CheckedMutex m_local_readers_mutex;
    std::vector<VersionList::ReadCount> m_local_readers GUARDED_BY(m_local_readers_mutex);

    util::CheckedMutex m_info_mutex;
    unsigned int m_local_max_entry GUARDED_BY(m_info_mutex) = 0;
    SharedInfo* m_info GUARDED_BY(m_info_mutex) = nullptr;

    virtual void ensure_reader_mapping(unsigned int required = -1) REQUIRES(m_info_mutex) = 0;
    virtual void expand_version_list(unsigned new_entries) REQUIRES(m_info_mutex) = 0;
    friend class DB::EncryptionMarkerObserver;
};

class DB::FileVersionManager final : public DB::VersionManager {
public:
    FileVersionManager(File& file, util::InterprocessMutex& mutex)
        : VersionManager(mutex)
        , m_file(file)
    {
        size_t size = 0, required_size = sizeof(SharedInfo);
        while (size < required_size) {
            // Map the file without the lock held. This could result in the
            // mapping being too small and having to remap if the file is grown
            // concurrently, but if this is the case we should always see a bigger
            // size the next time.
            auto new_size = static_cast<size_t>(m_file.get_size());
            REALM_ASSERT(new_size > size);
            size = new_size;
            m_reader_map.remap(m_file, File::access_ReadWrite, size, File::map_NoSync);
            m_info = m_reader_map.get_addr();

            std::lock_guard lock(m_mutex);
            m_local_max_entry = m_info->readers.capacity();
            required_size = sizeof(SharedInfo) + m_info->readers.compute_required_space(m_local_max_entry);
            REALM_ASSERT(required_size >= size);
        }
    }

    void expand_version_list(unsigned new_entries) override REQUIRES(m_info_mutex)
    {
        size_t new_info_size = sizeof(SharedInfo) + m_info->readers.compute_required_space(new_entries);
        m_file.prealloc(new_info_size);                                          // Throws
        m_reader_map.remap(m_file, util::File::access_ReadWrite, new_info_size); // Throws
        m_info = m_reader_map.get_addr();
    }

private:
    void ensure_reader_mapping(unsigned int required = -1) override REQUIRES(m_info_mutex)
    {
        using _impl::SimulatedFailure;
        SimulatedFailure::trigger(SimulatedFailure::shared_group__grow_reader_mapping); // Throws

        if (required < m_local_max_entry)
            return;

        auto new_max_entry = m_info->readers.capacity();
        if (new_max_entry > m_local_max_entry) {
            // handle mapping expansion if required
            size_t info_size = sizeof(DB::SharedInfo) + m_info->readers.compute_required_space(new_max_entry);
            m_reader_map.remap(m_file, util::File::access_ReadWrite, info_size); // Throws
            m_local_max_entry = new_max_entry;
            m_info = m_reader_map.get_addr();
        }
    }

    File& m_file;
    File::Map<DB::SharedInfo> m_reader_map;

    friend class DB::EncryptionMarkerObserver;
};

// adapter class for marking/observing encrypted writes
class DB::EncryptionMarkerObserver : public util::WriteMarker, public util::WriteObserver {
public:
    EncryptionMarkerObserver(DB::VersionManager& vm)
        : vm(vm)
    {
    }
    bool no_concurrent_writer_seen() override
    {
        uint64_t tmp_write_count;
        auto page_may_have_been_written = vm.observe_writer(nullptr, &tmp_write_count);
        if (tmp_write_count != last_seen_count) {
            page_may_have_been_written = true;
            last_seen_count = tmp_write_count;
        }
        if (page_may_have_been_written) {
            calls_since_last_writer_observed = 0;
            return false;
        }
        ++calls_since_last_writer_observed;
        constexpr size_t max_calls = 5; // an arbitrary handful, > 1
        return (calls_since_last_writer_observed >= max_calls);
    }
    void mark(uint64_t pos) override
    {
        vm.mark_page_for_writing(pos);
    }
    void unmark() override
    {
        vm.clear_writing_marker();
    }

private:
    DB::VersionManager& vm;
    uint64_t last_seen_count = 0;
    size_t calls_since_last_writer_observed = 0;
};

class DB::InMemoryVersionManager final : public DB::VersionManager {
public:
    InMemoryVersionManager(SharedInfo* info, util::InterprocessMutex& mutex)
        : VersionManager(mutex)
    {
        m_info = info;
        m_local_max_entry = m_info->readers.capacity();
    }
    void expand_version_list(unsigned) override
    {
        REALM_ASSERT(false);
    }

private:
    void ensure_reader_mapping(unsigned int) override {}
};

#if REALM_HAVE_STD_FILESYSTEM
std::string DBOptions::sys_tmp_dir = std::filesystem::temp_directory_path().string();
#else
std::string DBOptions::sys_tmp_dir = getenv("TMPDIR") ? getenv("TMPDIR") : "";
#endif

// NOTES ON CREATION AND DESTRUCTION OF SHARED MUTEXES:
//
// According to the 'process-sharing example' in the POSIX man page
// for pthread_mutexattr_init() other processes may continue to use a
// process-shared mutex after exit of the process that initialized
// it. Also, the example does not contain any call to
// pthread_mutex_destroy(), so apparently a process-shared mutex need
// not be destroyed at all, nor can it be that a process-shared mutex
// is associated with any resources that are local to the initializing
// process, because that would imply a leak.
//
// While it is not explicitly guaranteed in the man page, we shall
// assume that is is valid to initialize a process-shared mutex twice
// without an intervening call to pthread_mutex_destroy(). We need to
// be able to reinitialize a process-shared mutex if the first
// initializing process crashes and leaves the shared memory in an
// undefined state.

void DB::open(const std::string& path, const DBOptions& options)
{
    // Exception safety: Since do_open() is called from constructors, if it
    // throws, it must leave the file closed.
    using util::format;

    REALM_ASSERT(!is_attached());
    REALM_ASSERT(path.size());

    m_db_path = path;

    set_logger(options.logger);
    if (m_replication) {
        m_replication->set_logger(m_logger.get());
    }
    if (m_logger) {
        m_logger->log(util::Logger::Level::detail, "Open file: %1", path);
    }
    SlabAlloc& alloc = m_alloc;
    ref_type top_ref = 0;

    if (options.is_immutable) {
        SlabAlloc::Config cfg;
        cfg.read_only = true;
        cfg.no_create = true;
        cfg.encryption_key = options.encryption_key;
        top_ref = alloc.attach_file(path, cfg);
        SlabAlloc::DetachGuard dg(alloc);
        Group::read_only_version_check(alloc, top_ref, path);
        m_fake_read_lock_if_immutable = ReadLockInfo::make_fake(top_ref, m_alloc.get_baseline());
        dg.release();
        return;
    }
    std::string lockfile_path = get_core_file(path, CoreFileType::Lock);
    std::string coordination_dir = get_core_file(path, CoreFileType::Management);
    std::string lockfile_prefix = coordination_dir + "/access_control";
    m_alloc.set_read_only(false);

    Replication::HistoryType openers_hist_type = Replication::hist_None;
    int openers_hist_schema_version = 0;
    if (Replication* repl = get_replication()) {
        openers_hist_type = repl->get_history_type();
        openers_hist_schema_version = repl->get_history_schema_version();
    }

    int current_file_format_version;
    int target_file_format_version;
    int stored_hist_schema_version = -1; // Signals undetermined

    int retries_left = 10; // number of times to retry before throwing exceptions
    // in case there is something wrong with the .lock file... the retries allows
    // us to pick a new lockfile initializer in case the first one crashes without
    // completing the initialization
    std::default_random_engine random_gen;
    for (;;) {

        // if we're retrying, we first wait a random time
        if (retries_left < 10) {
            if (retries_left == 9) { // we seed it from a true random source if possible
                std::random_device r;
                random_gen.seed(r());
            }
            int max_delay = (10 - retries_left) * 10;
            int msecs = random_gen() % max_delay;
            millisleep(msecs);
        }

        m_file.open(lockfile_path, File::access_ReadWrite, File::create_Auto, 0); // Throws
        File::CloseGuard fcg(m_file);
        m_file.set_fifo_path(coordination_dir, "lock.fifo");

        if (m_file.try_rw_lock_exclusive()) { // Throws
            File::UnlockGuard ulg(m_file);

            // We're alone in the world, and it is Ok to initialize the
            // file. Start by truncating the file to zero to ensure that
            // the following resize will generate a file filled with zeroes.
            //
            // This will in particular set m_init_complete to 0.
            m_file.resize(0);
            m_file.prealloc(sizeof(SharedInfo));

            // We can crash anytime during this process. A crash prior to
            // the first resize could allow another thread which could not
            // get the exclusive lock because we hold it, and hence were
            // waiting for the shared lock instead, to observe and use an
            // old lock file.
            m_file_map.map(m_file, File::access_ReadWrite, sizeof(SharedInfo), File::map_NoSync); // Throws
            File::UnmapGuard fug(m_file_map);
            SharedInfo* info = m_file_map.get_addr();

            new (info) SharedInfo{options.durability, openers_hist_type, openers_hist_schema_version}; // Throws

            // Because init_complete is an std::atomic, it's guaranteed not to be observable by others
            // as being 1 before the entire SharedInfo header has been written.
            info->init_complete = 1;
        }

// We hold the shared lock from here until we close the file!
#if REALM_PLATFORM_APPLE
        // macOS has a bug which can cause a hang waiting to obtain a lock, even
        // if the lock is already open in shared mode, so we work around it by
        // busy waiting. This should occur only briefly during session initialization.
        while (!m_file.try_rw_lock_shared()) {
            sched_yield();
        }
#else
        m_file.rw_lock_shared(); // Throws
#endif
        File::UnlockGuard ulg(m_file);

        // The coordination/management dir is created as a side effect of the lock
        // operation above if needed for lock emulation. But it may also be needed
        // for other purposes, so make sure it exists.
        // in worst case there'll be a race on creating this directory.
        // This should be safe but a waste of resources.
        // Unfortunately it cannot be created at an earlier point, because
        // it may then be deleted during the above lock_shared() operation.
        try_make_dir(coordination_dir);

        // If the file is not completely initialized at this point in time, the
        // preceeding initialization attempt must have failed. We know that an
        // initialization process was in progress, because this thread (or
        // process) failed to get an exclusive lock on the file. Because this
        // thread (or process) currently has a shared lock on the file, we also
        // know that the initialization process can no longer be in progress, so
        // the initialization must either have completed or failed at this time.

        // The file is taken to be completely initialized if it is large enough
        // to contain the `init_complete` field, and `init_complete` is true. If
        // the file was not completely initialized, this thread must give up its
        // shared lock, and retry to become the initializer. Eventually, one of
        // two things must happen; either this thread, or another thread
        // succeeds in completing the initialization, or this thread becomes the
        // initializer, and fails the initialization. In either case, the retry
        // loop will eventually terminate.

        // An empty file is (and was) never a successfully initialized file.
        size_t info_size = sizeof(SharedInfo);
        {
            auto file_size = m_file.get_size();
            if (util::int_less_than(file_size, info_size)) {
                if (file_size == 0)
                    continue; // Retry
                info_size = size_t(file_size);
            }
        }

        // Map the initial section of the SharedInfo file that corresponds to
        // the SharedInfo struct, or less if the file is smaller. We know that
        // we have at least one byte, and that is enough to read the
        // `init_complete` flag.
        m_file_map.map(m_file, File::access_ReadWrite, info_size, File::map_NoSync);
        File::UnmapGuard fug_1(m_file_map);
        SharedInfo* info = m_file_map.get_addr();

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
        static_assert(offsetof(SharedInfo, init_complete) + sizeof SharedInfo::init_complete <= 1,
                      "Unexpected position or size of SharedInfo::init_complete");
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
        if (info->init_complete == 0)
            continue;
        REALM_ASSERT(info->init_complete == 1);

        // At this time, we know that the file was completely initialized, but
        // we still need to verify that is was initialized with the memory
        // layout expected by this session participant. We could find that it is
        // initializaed with a different memory layout if other concurrent
        // session participants use different versions of the core library.
        if (info_size < sizeof(SharedInfo)) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            throw IncompatibleLockFile(path, format("Architecture mismatch: SharedInfo size is %1 but should be %2.",
                                                    info_size, sizeof(SharedInfo)));
        }
        if (info->shared_info_version != g_shared_info_version) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            throw IncompatibleLockFile(path, format("Version mismatch: SharedInfo version is %1 but should be %2.",
                                                    info->shared_info_version, g_shared_info_version));
        }
        // Validate compatible sizes of mutex and condvar types. Sizes of all
        // other fields are architecture independent, so if condvar and mutex
        // sizes match, the entire struct matches. The offsets of
        // `size_of_mutex` and `size_of_condvar` are known to be as expected due
        // to the preceeding check in `shared_info_version`.
        if (info->size_of_mutex != sizeof info->shared_controlmutex) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            throw IncompatibleLockFile(path, format("Architecture mismatch: Mutex size is %1 but should be %2.",
                                                    info->size_of_mutex, sizeof(info->shared_controlmutex)));
        }

        if (info->size_of_condvar != sizeof info->room_to_write) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            throw IncompatibleLockFile(
                path, format("Architecture mismatch: Condition variable size is %1 but should be %2.",
                             info->size_of_condvar, sizeof(info->room_to_write)));
        }
        m_writemutex.set_shared_part(info->shared_writemutex, lockfile_prefix, "write");
        m_controlmutex.set_shared_part(info->shared_controlmutex, lockfile_prefix, "control");
        m_versionlist_mutex.set_shared_part(info->shared_versionlist_mutex, lockfile_prefix, "versions");

        // even though fields match wrt alignment and size, there may still be incompatibilities
        // between implementations, so lets ask one of the mutexes if it thinks it'll work.
        if (!m_controlmutex.is_valid()) {
            throw IncompatibleLockFile(
                path, "Control mutex is invalid. This suggests that incompatible pthread libraries are in use.");
        }

        // OK! lock file appears valid. We can now continue operations under the protection
        // of the controlmutex. The controlmutex protects the following activities:
        // - attachment of the database file
        // - start of the async daemon
        // - stop of the async daemon
        // - restore of a backup, if desired
        // - backup of the realm file in preparation of file format upgrade
        // - DB beginning/ending a session
        // - Waiting for and signalling database changes
        {
            std::lock_guard<InterprocessMutex> lock(m_controlmutex); // Throws
            auto version_manager = std::make_unique<FileVersionManager>(m_file, m_versionlist_mutex);

            // proceed to initialize versioning and other metadata information related to
            // the database. Also create the database if we're beginning a new session
            bool begin_new_session = (info->num_participants == 0);
            SlabAlloc::Config cfg;
            cfg.session_initiator = begin_new_session;
            cfg.is_shared = true;
            cfg.read_only = false;
            cfg.skip_validate = !begin_new_session;
            cfg.disable_sync = options.durability == Durability::MemOnly || options.durability == Durability::Unsafe;
            cfg.clear_file_on_error = options.clear_on_invalid_file;

            // only the session initiator is allowed to create the database, all other
            // must assume that it already exists.
            cfg.no_create = (begin_new_session ? options.no_create : true);

            // if we're opening a MemOnly file that isn't already opened by
            // someone else then it's a file which should have been deleted on
            // close previously, but wasn't (perhaps due to the process crashing)
            cfg.clear_file = (options.durability == Durability::MemOnly && begin_new_session);

            cfg.encryption_key = options.encryption_key;
            m_marker_observer = std::make_unique<EncryptionMarkerObserver>(*version_manager);
            try {
                top_ref = alloc.attach_file(path, cfg, m_marker_observer.get()); // Throws
            }
            catch (const SlabAlloc::Retry&) {
                // On a SlabAlloc::Retry file mappings are already unmapped, no
                // need to do more
                continue;
            }

            // Determine target file format version for session (upgrade
            // required if greater than file format version of attached file).
            current_file_format_version = alloc.get_committed_file_format_version();
            target_file_format_version =
                Group::get_target_file_format_version_for_session(current_file_format_version, openers_hist_type);
            BackupHandler backup(path, options.accepted_versions, options.to_be_deleted);
            if (backup.must_restore_from_backup(current_file_format_version)) {
                // we need to unmap before any file ops that'll change the realm
                // file:
                // (only strictly needed for Windows)
                alloc.detach();
                backup.restore_from_backup();
                // finally, retry with the restored file instead of the original
                // one:
                continue;
            }
            backup.cleanup_backups();

            // From here on, if we fail in any way, we must detach the
            // allocator.
            SlabAlloc::DetachGuard alloc_detach_guard(alloc);
            alloc.note_reader_start(this);
            // must come after the alloc detach guard
            auto handler = [this, &alloc]() noexcept {
                alloc.note_reader_end(this);
            };
            auto reader_end_guard = make_scope_exit(handler);

            // Check validity of top array (to give more meaningful errors
            // early)
            if (top_ref) {
                try {
                    alloc.note_reader_start(this);
                    auto reader_end_guard = make_scope_exit([&]() noexcept {
                        alloc.note_reader_end(this);
                    });
                    Array top{alloc};
                    top.init_from_ref(top_ref);
                    Group::validate_top_array(top, alloc);
                }
                catch (const InvalidDatabase& e) {
                    if (e.get_path().empty()) {
                        throw InvalidDatabase(e.what(), path);
                    }
                    throw;
                }
            }
            if (options.backup_at_file_format_change) {
                backup.backup_realm_if_needed(current_file_format_version, target_file_format_version);
            }
            using gf = _impl::GroupFriend;
            bool file_format_ok;
            // In shared mode (Realm file opened via a DB instance) this
            // version of the core library is able to open Realms using file format
            // versions listed below. Please see Group::get_file_format_version() for
            // information about the individual file format versions.
            if (current_file_format_version == 0) {
                file_format_ok = (top_ref == 0);
            }
            else {
                file_format_ok = backup.is_accepted_file_format(current_file_format_version);
            }

            if (REALM_UNLIKELY(!file_format_ok)) {
                throw UnsupportedFileFormatVersion(current_file_format_version);
            }

            if (begin_new_session) {
                // Determine version (snapshot number) and check history
                // compatibility
                version_type version = 0;
                int stored_hist_type = 0;
                gf::get_version_and_history_info(alloc, top_ref, version, stored_hist_type,
                                                 stored_hist_schema_version);
                bool good_history_type = false;
                switch (openers_hist_type) {
                    case Replication::hist_None:
                        good_history_type = (stored_hist_type == Replication::hist_None);
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format("Realm file at path '%1' has history type '%2', but is being opened "
                                             "with replication disabled.",
                                             path, Replication::history_type_name(stored_hist_type)),
                                path);
                        break;
                    case Replication::hist_OutOfRealm:
                        REALM_ASSERT(false); // No longer in use
                        break;
                    case Replication::hist_InRealm:
                        good_history_type = (stored_hist_type == Replication::hist_InRealm ||
                                             stored_hist_type == Replication::hist_None);
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format("Realm file at path '%1' has history type '%2', but is being opened in "
                                             "local history mode.",
                                             path, Replication::history_type_name(stored_hist_type)),
                                path);
                        break;
                    case Replication::hist_SyncClient:
                        good_history_type = ((stored_hist_type == Replication::hist_SyncClient) || (top_ref == 0));
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format("Realm file at path '%1' has history type '%2', but is being opened in "
                                             "synchronized history mode.",
                                             path, Replication::history_type_name(stored_hist_type)),
                                path);
                        break;
                    case Replication::hist_SyncServer:
                        good_history_type = ((stored_hist_type == Replication::hist_SyncServer) || (top_ref == 0));
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format("Realm file at path '%1' has history type '%2', but is being opened in "
                                             "server history mode.",
                                             path, Replication::history_type_name(stored_hist_type)),
                                path);
                        break;
                }

                REALM_ASSERT(stored_hist_schema_version >= 0);
                if (stored_hist_schema_version > openers_hist_schema_version)
                    throw IncompatibleHistories(
                        util::format("Unexpected future history schema version %1, current schema %2",
                                     stored_hist_schema_version, openers_hist_schema_version),
                        path);
                bool need_hist_schema_upgrade =
                    (stored_hist_schema_version < openers_hist_schema_version && top_ref != 0);
                if (need_hist_schema_upgrade) {
                    Replication* repl = get_replication();
                    if (!repl->is_upgradable_history_schema(stored_hist_schema_version))
                        throw IncompatibleHistories(util::format("Nonupgradable history schema %1, current schema %2",
                                                                 stored_hist_schema_version,
                                                                 openers_hist_schema_version),
                                                    path);
                }

                bool need_file_format_upgrade =
                    current_file_format_version < target_file_format_version && top_ref != 0;
                if (!options.allow_file_format_upgrade && (need_hist_schema_upgrade || need_file_format_upgrade)) {
                    throw FileFormatUpgradeRequired(m_db_path);
                }

                alloc.convert_from_streaming_form(top_ref);
                try {
                    bool file_changed_size = alloc.align_filesize_for_mmap(top_ref, cfg);
                    if (file_changed_size) {
                        // we need to re-establish proper mappings after file size change.
                        // we do this simply by aborting and starting all over:
                        continue;
                    }
                }
                // something went wrong. Retry.
                catch (SlabAlloc::Retry&) {
                    continue;
                }
                if (options.encryption_key) {
#ifdef _WIN32
                    uint64_t pid = GetCurrentProcessId();
#else
                    static_assert(sizeof(pid_t) <= sizeof(uint64_t), "process identifiers too large");
                    uint64_t pid = getpid();
#endif
                    info->session_initiator_pid = pid;
                }

                info->file_format_version = uint_fast8_t(target_file_format_version);

                // Initially there is a single version in the file
                info->number_of_versions = 1;

                info->latest_version_number = version;
                alloc.init_mapping_management(version);

                size_t file_size = 24;
                if (top_ref) {
                    Array top(alloc);
                    top.init_from_ref(top_ref);
                    file_size = Group::get_logical_file_size(top);
                }
                version_manager->init_versioning(top_ref, file_size, version);
            }
            else { // Not the session initiator
                // Durability setting must be consistent across a session. An
                // inconsistency is a logic error, as the user is required to
                // make sure that all possible concurrent session participants
                // use the same durability setting for the same Realm file.
                if (Durability(info->durability) != options.durability)
                    throw RuntimeError(ErrorCodes::IncompatibleSession, "Durability not consistent");

                // History type must be consistent across a session. An
                // inconsistency is a logic error, as the user is required to
                // make sure that all possible concurrent session participants
                // use the same history type for the same Realm file.
                if (info->history_type != openers_hist_type)
                    throw RuntimeError(ErrorCodes::IncompatibleSession, "History type not consistent");

                // History schema version must be consistent across a
                // session. An inconsistency is a logic error, as the user is
                // required to make sure that all possible concurrent session
                // participants use the same history schema version for the same
                // Realm file.
                if (info->history_schema_version != openers_hist_schema_version)
                    throw RuntimeError(ErrorCodes::IncompatibleSession, "History schema version not consistent");

                // We need per session agreement among all participants on the
                // target Realm file format. From a technical perspective, the
                // best way to ensure that, would be to require a bumping of the
                // SharedInfo file format version on any change that could lead
                // to a different result from
                // get_target_file_format_for_session() given the same current
                // Realm file format version and the same history type, as that
                // would prevent the outcome of the Realm opening process from
                // depending on race conditions. However, for practical reasons,
                // we shall instead simply check that there is agreement, and
                // throw the same kind of exception, as would have been thrown
                // with a bumped SharedInfo file format version, if there isn't.
                if (info->file_format_version != target_file_format_version) {
                    throw IncompatibleLockFile(path,
                                               format("Version mismatch: File format version is %1 but should be %2.",
                                                      info->file_format_version, target_file_format_version));
                }

                // Even though this session participant is not the session initiator,
                // it may be the one that has to perform the history schema upgrade.
                // See upgrade_file_format(). However we cannot get the actual value
                // at this point as the allocator is not synchronized with the file.
                // The value will be read in a ReadTransaction later.

                // We need to setup the allocators version information, as it is needed
                // to correctly age and later reclaim memory mappings.
                version_type version = info->latest_version_number;
                alloc.init_mapping_management(version);
            }

            m_new_commit_available.set_shared_part(info->new_commit_available, lockfile_prefix, "new_commit",
                                                   options.temp_dir);
            m_pick_next_writer.set_shared_part(info->pick_next_writer, lockfile_prefix, "pick_writer",
                                               options.temp_dir);

            // make our presence noted:
            ++info->num_participants;
            m_info = info;

            // Keep the mappings and file open:
            m_version_manager = std::move(version_manager);
            alloc_detach_guard.release();
            fug_1.release(); // Do not unmap
            fcg.release();   // Do not close
        }
        ulg.release(); // Do not release shared lock
        break;
    }

    if (m_logger) {
        m_logger->log(util::Logger::Level::debug, "   Number of participants: %1", m_info->num_participants);
        m_logger->log(util::Logger::Level::debug, "   Durability: %1", [&] {
            switch (options.durability) {
                case DBOptions::Durability::Full:
                    return "Full";
                case DBOptions::Durability::MemOnly:
                    return "MemOnly";
                case realm::DBOptions::Durability::Unsafe:
                    return "Unsafe";
            }
            return "";
        }());
        m_logger->log(util::Logger::Level::debug, "   EncryptionKey: %1", options.encryption_key ? "yes" : "no");
        if (m_logger->would_log(util::Logger::Level::debug)) {
            if (top_ref) {
                Array top(alloc);
                top.init_from_ref(top_ref);
                auto file_size = Group::get_logical_file_size(top);
                auto history_size = Group::get_history_size(top);
                auto freee_space_size = Group::get_free_space_size(top);
                m_logger->log(util::Logger::Level::debug, "   File size: %1", file_size);
                m_logger->log(util::Logger::Level::debug, "   User data size: %1",
                              file_size - (freee_space_size + history_size));
                m_logger->log(util::Logger::Level::debug, "   Free space size: %1", freee_space_size);
                m_logger->log(util::Logger::Level::debug, "   History size: %1", history_size);
            }
            else {
                m_logger->log(util::Logger::Level::debug, "   Empty file");
            }
        }
    }

    // Upgrade file format and/or history schema
    try {
        if (stored_hist_schema_version == -1) {
            // current_hist_schema_version has not been read. Read it now
            stored_hist_schema_version = start_read()->get_history_schema_version();
        }
        if (current_file_format_version == 0) {
            // If the current file format is still undecided, no upgrade is
            // necessary, but we still need to make the chosen file format
            // visible to the rest of the core library by updating the value
            // that will be subsequently returned by
            // Group::get_file_format_version(). For this to work, all session
            // participants must adopt the chosen target Realm file format when
            // the stored file format version is zero regardless of the version
            // of the core library used.
            m_file_format_version = target_file_format_version;
        }
        else {
            m_file_format_version = current_file_format_version;
            upgrade_file_format(options.allow_file_format_upgrade, target_file_format_version,
                                stored_hist_schema_version, openers_hist_schema_version); // Throws
        }
    }
    catch (...) {
        close();
        throw;
    }
    m_alloc.set_read_only(true);
}

void DB::open(BinaryData buffer, bool take_ownership)
{
    auto top_ref = m_alloc.attach_buffer(buffer.data(), buffer.size());
    m_fake_read_lock_if_immutable = ReadLockInfo::make_fake(top_ref, buffer.size());
    if (take_ownership)
        m_alloc.own_buffer();
}

void DB::open(Replication& repl, const std::string& file, const DBOptions& options)
{
    // Exception safety: Since open() is called from constructors, if it throws,
    // it must leave the file closed.

    REALM_ASSERT(!is_attached());

    repl.initialize(*this); // Throws

    set_replication(&repl);

    open(file, options); // Throws
}

class DBLogger : public Logger {
public:
    DBLogger(const std::shared_ptr<Logger>& base_logger, unsigned hash) noexcept
        : Logger(LogCategory::storage, *base_logger)
        , m_hash(hash)
        , m_base_logger_ptr(base_logger)
    {
    }

protected:
    void do_log(const LogCategory& category, Level level, const std::string& message) final
    {
        std::ostringstream ostr;
        auto id = std::this_thread::get_id();
        ostr << "DB: " << m_hash << " Thread " << id << ": " << message;
        Logger::do_log(*m_base_logger_ptr, category, level, ostr.str());
    }

private:
    unsigned m_hash;
    std::shared_ptr<Logger> m_base_logger_ptr;
};

void DB::set_logger(const std::shared_ptr<util::Logger>& logger) noexcept
{
    if (logger)
        m_logger = std::make_shared<DBLogger>(logger, m_log_id);
}

void DB::open(Replication& repl, const DBOptions& options)
{
    REALM_ASSERT(!is_attached());
    repl.initialize(*this); // Throws
    set_replication(&repl);

    m_alloc.init_in_memory_buffer();

    set_logger(options.logger);
    m_replication->set_logger(m_logger.get());
    if (m_logger)
        m_logger->detail("Open memory-only realm");

    auto hist_type = repl.get_history_type();
    m_in_memory_info =
        std::make_unique<SharedInfo>(DBOptions::Durability::MemOnly, hist_type, repl.get_history_schema_version());
    SharedInfo* info = m_in_memory_info.get();
    m_writemutex.set_shared_part(info->shared_writemutex, "", "write");
    m_controlmutex.set_shared_part(info->shared_controlmutex, "", "control");
    m_new_commit_available.set_shared_part(info->new_commit_available, "", "new_commit", options.temp_dir);
    m_pick_next_writer.set_shared_part(info->pick_next_writer, "", "pick_writer", options.temp_dir);
    m_versionlist_mutex.set_shared_part(info->shared_versionlist_mutex, "", "versions");

    auto target_file_format_version = uint_fast8_t(Group::get_target_file_format_version_for_session(0, hist_type));
    info->file_format_version = target_file_format_version;
    info->number_of_versions = 1;
    info->latest_version_number = 1;
    info->init_versioning(0, m_alloc.get_baseline(), 1);
    ++info->num_participants;

    m_version_manager = std::make_unique<InMemoryVersionManager>(info, m_versionlist_mutex);

    m_file_format_version = target_file_format_version;

    m_info = info;
    m_alloc.set_read_only(true);
}

void DB::create_new_history(Replication& repl)
{
    Replication* old_repl = get_replication();
    try {
        repl.initialize(*this);
        set_replication(&repl);

        auto tr = start_write();
        tr->clear_history();
        tr->replicate(tr.get(), repl);
        tr->commit();
    }
    catch (...) {
        set_replication(old_repl);
        throw;
    }
}

void DB::create_new_history(std::unique_ptr<Replication> repl)
{
    create_new_history(*repl);
    m_history = std::move(repl);
}

// WARNING / FIXME: compact() should NOT be exposed publicly on Windows because it's not crash safe! It may
// corrupt your database if something fails.
// Tracked by https://github.com/realm/realm-core/issues/4111

// A note about lock ordering.
// The local mutex, m_mutex, guards transaction start/stop and map/unmap of the lock file.
// Except for compact(), open() and close(), it should only be held briefly.
// The controlmutex guards operations which change the file size, session initialization
// and session exit.
// The writemutex guards the integrity of the (write) transaction data.
// The controlmutex and writemutex resides in the .lock file and thus requires
// the mapping of the .lock file to work. A straightforward approach would be to lock
// the m_mutex whenever the other mutexes are taken or released...but that would be too
// bad for performance of transaction start/stop.
//
// The locks are to be taken in this order: writemutex->controlmutex->m_mutex
//
// The .lock file is mapped during DB::create() and unmapped by a call to DB::close().
// Once unmapped, it is never mapped again. Hence any observer with a valid DBRef may
// only see the transition from mapped->unmapped, never the opposite.
//
// Trying to create a transaction if the .lock file is unmapped will result in an assert.
// Unmapping (during close()) while transactions are live, is not considered an error. There
// is a potential race between unmapping during close() and any operation carried out by a live
// transaction. The user must ensure that this race never happens if she uses DB::close().
bool DB::compact(bool bump_version_number, util::Optional<const char*> output_encryption_key)
    NO_THREAD_SAFETY_ANALYSIS // this would work except for a known limitation: "No alias analysis" where clang cannot
                              // tell that tr->db->m_mutex is the same thing as m_mutex
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::string tmp_path = m_db_path + ".tmp_compaction_space";

    // To enter compact, the DB object must already have been attached to a file,
    // since this happens in DB::create().

    // Verify that the lock file is still attached. There is no attempt to guard against
    // a race between close() and compact().
    if (is_attached() == false) {
        throw Exception(ErrorCodes::IllegalOperation, m_db_path + ": compact must be done on an open/attached DB");
    }
    auto info = m_info;
    Durability dura = Durability(info->durability);
    const char* write_key = bool(output_encryption_key) ? *output_encryption_key : get_encryption_key();
    {
        std::unique_lock<InterprocessMutex> lock(m_controlmutex); // Throws
        auto t1 = std::chrono::steady_clock::now();

        // We must be the ONLY DB object attached if we're to do compaction
        if (info->num_participants > 1)
            return false;

        // Holding the controlmutex prevents any other DB from attaching to the file.

        // Using start_read here ensures that we have access to the latest entry
        // in the VersionList. We need to have access to that later to update top_ref and file_size.
        // This is also needed to attach the group (get the proper top pointer, etc)
        TransactionRef tr = start_read();
        auto file_size_before = tr->get_logical_file_size();

        // local lock blocking any transaction from starting (and stopping)
        CheckedLockGuard local_lock(m_mutex);

        // We should be the only transaction active - otherwise back out
        if (m_transaction_count != 1)
            return false;

        // group::write() will throw if the file already exists.
        // To prevent this, we have to remove the file (should it exist)
        // before calling group::write().
        File::try_remove(tmp_path);

        // Compact by writing a new file holding only live data, then renaming the new file
        // so it becomes the database file, replacing the old one in the process.
        try {
            File file;
            file.open(tmp_path, File::access_ReadWrite, File::create_Must, 0);
            int incr = bump_version_number ? 1 : 0;
            Group::DefaultTableWriter writer;
            tr->write(file, write_key, info->latest_version_number + incr, writer); // Throws
            // Data needs to be flushed to the disk before renaming.
            bool disable_sync = get_disable_sync_to_disk();
            if (!disable_sync && dura != Durability::Unsafe)
                file.sync(); // Throws
        }
        catch (...) {
            // If writing the compact version failed in any way, delete the partially written file to clean up disk
            // space. This is so that we don't fail with 100% disk space used when compacting on a mostly full disk.
            if (File::exists(tmp_path)) {
                File::remove(tmp_path);
            }
            throw;
        }
        // if we've written a file with a bumped version number, we need to update the lock file to match.
        if (bump_version_number) {
            ++info->latest_version_number;
        }
        // We need to release any shared mapping *before* releasing the control mutex.
        // When someone attaches to the new database file, they *must* *not* see and
        // reuse any existing memory mapping of the stale file.
        tr->close_read_with_lock();
        m_alloc.detach();

        util::File::move(tmp_path, m_db_path);

        SlabAlloc::Config cfg;
        cfg.session_initiator = true;
        cfg.is_shared = true;
        cfg.read_only = false;
        cfg.skip_validate = false;
        cfg.no_create = true;
        cfg.clear_file = false;
        cfg.encryption_key = write_key;
        ref_type top_ref;
        top_ref = m_alloc.attach_file(m_db_path, cfg, m_marker_observer.get());
        m_alloc.convert_from_streaming_form(top_ref);
        m_alloc.init_mapping_management(info->latest_version_number);
        info->number_of_versions = 1;
        size_t logical_file_size = sizeof(SlabAlloc::Header);
        if (top_ref) {
            Array top(m_alloc);
            top.init_from_ref(top_ref);
            logical_file_size = Group::get_logical_file_size(top);
        }
        m_version_manager->init_versioning(top_ref, logical_file_size, info->latest_version_number);
        if (m_logger) {
            auto t2 = std::chrono::steady_clock::now();
            m_logger->log(util::Logger::Level::info, "DB compacted from: %1 to %2 in %3 us", file_size_before,
                          logical_file_size, std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
        }
    }
    return true;
}

void DB::write_copy(StringData path, const char* output_encryption_key)
{
    auto tr = start_read();
    if (auto hist = tr->get_history()) {
        if (!hist->no_pending_local_changes(tr->get_version())) {
            throw Exception(ErrorCodes::IllegalOperation,
                            "All client changes must be integrated in server before writing copy");
        }
    }

    class NoClientFileIdWriter : public Group::DefaultTableWriter {
    public:
        NoClientFileIdWriter()
            : Group::DefaultTableWriter(true)
        {
        }
        HistoryInfo write_history(_impl::OutputStream& out) override
        {
            auto hist = Group::DefaultTableWriter::write_history(out);
            hist.sync_file_id = 0;
            return hist;
        }
    } writer;

    File file;
    file.open(path, File::access_ReadWrite, File::create_Must, 0);
    file.resize(0);

    auto t1 = std::chrono::steady_clock::now();
    tr->write(file, output_encryption_key, m_info->latest_version_number, writer);
    if (m_logger) {
        auto t2 = std::chrono::steady_clock::now();
        m_logger->log(util::Logger::Level::info, "DB written to '%1' in %2 us", path,
                      std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
    }
}

uint_fast64_t DB::get_number_of_versions()
{
    if (m_fake_read_lock_if_immutable)
        return 1;
    return m_info->number_of_versions;
}

size_t DB::get_allocated_size() const
{
    return m_alloc.get_allocated_size();
}

void DB::release_all_read_locks() noexcept
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    CheckedLockGuard local_lock(m_mutex); // mx on m_local_locks_held
    for (auto& read_lock : m_local_locks_held) {
        --m_transaction_count;
        m_version_manager->release_read_lock(read_lock);
    }
    m_local_locks_held.clear();
    REALM_ASSERT(m_transaction_count == 0);
}

class DB::AsyncCommitHelper {
public:
    AsyncCommitHelper(DB* db)
        : m_db(db)
    {
    }
    ~AsyncCommitHelper()
    {
        {
            std::unique_lock lg(m_mutex);
            if (!m_running) {
                return;
            }
            m_running = false;
            m_cv_worker.notify_one();
        }
        m_thread.join();
    }

    void begin_write(util::UniqueFunction<void()> fn)
    {
        std::unique_lock lg(m_mutex);
        start_thread();
        m_pending_writes.emplace_back(std::move(fn));
        m_cv_worker.notify_one();
    }

    void blocking_begin_write()
    {
        std::unique_lock lg(m_mutex);

        // If we support unlocking InterprocessMutex from a different thread
        // than it was locked on, we can sometimes just begin the write on
        // the current thread. This requires that no one is currently waiting
        // for the worker thread to acquire the write lock, as we'll deadlock
        // if we try to async commit while the worker is waiting for the lock.
        bool can_lock_on_caller =
            !InterprocessMutex::is_thread_confined && (!m_owns_write_mutex && m_pending_writes.empty() &&
                                                       m_write_lock_claim_ticket == m_write_lock_claim_fulfilled);

        // If we support cross-thread unlocking and m_running is false,
        // can_lock_on_caller should always be true or we forgot to launch the thread
        REALM_ASSERT(can_lock_on_caller || m_running || InterprocessMutex::is_thread_confined);

        // If possible, just begin the write on the current thread
        if (can_lock_on_caller) {
            m_waiting_for_write_mutex = true;
            lg.unlock();
            m_db->do_begin_write();
            lg.lock();
            m_waiting_for_write_mutex = false;
            m_has_write_mutex = true;
            m_owns_write_mutex = false;
            return;
        }

        // Otherwise we have to ask the worker thread to acquire it and wait
        // for that
        start_thread();
        size_t ticket = ++m_write_lock_claim_ticket;
        m_cv_worker.notify_one();
        m_cv_callers.wait(lg, [this, ticket] {
            return ticket == m_write_lock_claim_fulfilled;
        });
    }

    void end_write()
    {
        std::unique_lock lg(m_mutex);
        REALM_ASSERT(m_has_write_mutex);
        REALM_ASSERT(m_owns_write_mutex || !InterprocessMutex::is_thread_confined);

        // If we acquired the write lock on the worker thread, also release it
        // there even if our mutex supports unlocking cross-thread as it simplifies things.
        if (m_owns_write_mutex) {
            m_pending_mx_release = true;
            m_cv_worker.notify_one();
        }
        else {
            m_db->do_end_write();
            m_has_write_mutex = false;
        }
    }

    bool blocking_end_write()
    {
        std::unique_lock lg(m_mutex);
        if (!m_has_write_mutex) {
            return false;
        }
        REALM_ASSERT(m_owns_write_mutex || !InterprocessMutex::is_thread_confined);

        // If we acquired the write lock on the worker thread, also release it
        // there even if our mutex supports unlocking cross-thread as it simplifies things.
        if (m_owns_write_mutex) {
            m_pending_mx_release = true;
            m_cv_worker.notify_one();
            m_cv_callers.wait(lg, [this] {
                return !m_pending_mx_release;
            });
        }
        else {
            m_db->do_end_write();
            m_has_write_mutex = false;

            // The worker thread may have ignored a request for the write mutex
            // while we were acquiring it, so we need to wake up the thread
            if (has_pending_write_requests()) {
                lg.unlock();
                m_cv_worker.notify_one();
            }
        }
        return true;
    }


    void sync_to_disk(util::UniqueFunction<void()> fn)
    {
        REALM_ASSERT(fn);
        std::unique_lock lg(m_mutex);
        REALM_ASSERT(!m_pending_sync);
        start_thread();
        m_pending_sync = std::move(fn);
        m_cv_worker.notify_one();
    }

private:
    DB* m_db;
    std::thread m_thread;
    std::mutex m_mutex;
    std::condition_variable m_cv_worker;
    std::condition_variable m_cv_callers;
    std::deque<util::UniqueFunction<void()>> m_pending_writes;
    util::UniqueFunction<void()> m_pending_sync;
    size_t m_write_lock_claim_ticket = 0;
    size_t m_write_lock_claim_fulfilled = 0;
    bool m_pending_mx_release = false;
    bool m_running = false;
    bool m_has_write_mutex = false;
    bool m_owns_write_mutex = false;
    bool m_waiting_for_write_mutex = false;

    void main();

    void start_thread()
    {
        if (m_running) {
            return;
        }
        m_running = true;
        m_thread = std::thread([this]() {
            main();
        });
    }

    bool has_pending_write_requests()
    {
        return m_write_lock_claim_fulfilled < m_write_lock_claim_ticket || !m_pending_writes.empty();
    }
};

DB::~DB() noexcept
{
    close();
}

// Note: close() and close_internal() may be called from the DB::~DB().
// in that case, they will not throw. Throwing can only happen if called
// directly.
void DB::close(bool allow_open_read_transactions)
{
    // make helper thread(s) terminate
    m_commit_helper.reset();

    if (m_fake_read_lock_if_immutable) {
        if (!is_attached())
            return;
        {
            CheckedLockGuard local_lock(m_mutex);
            if (!allow_open_read_transactions && m_transaction_count)
                throw WrongTransactionState("Closing with open read transactions");
        }
        if (m_alloc.is_attached())
            m_alloc.detach();
        m_fake_read_lock_if_immutable.reset();
    }
    else {
        close_internal(std::unique_lock<InterprocessMutex>(m_controlmutex, std::defer_lock),
                       allow_open_read_transactions);
    }
}

void DB::close_internal(std::unique_lock<InterprocessMutex> lock, bool allow_open_read_transactions)
{
    if (!is_attached())
        return;

    {
        CheckedLockGuard local_lock(m_mutex);
        if (m_write_transaction_open)
            throw WrongTransactionState("Closing with open write transactions");
        if (!allow_open_read_transactions && m_transaction_count)
            throw WrongTransactionState("Closing with open read transactions");
    }
    SharedInfo* info = m_info;
    {
        if (!lock.owns_lock())
            lock.lock();

        if (m_alloc.is_attached())
            m_alloc.detach();

        if (m_is_sync_agent) {
            REALM_ASSERT(info->sync_agent_present);
            info->sync_agent_present = 0; // Set to false
        }
        release_all_read_locks();
        --info->num_participants;
        bool end_of_session = info->num_participants == 0;
        // std::cerr << "closing" << std::endl;
        if (end_of_session) {

            // If the db file is just backing for a transient data structure,
            // we can delete it when done.
            if (Durability(info->durability) == Durability::MemOnly && !m_in_memory_info) {
                try {
                    util::File::remove(m_db_path.c_str());
                }
                catch (...) {
                } // ignored on purpose.
            }
        }
        lock.unlock();
    }
    {
        CheckedLockGuard local_lock(m_mutex);

        m_new_commit_available.close();
        m_pick_next_writer.close();

        if (m_in_memory_info) {
            m_in_memory_info.reset();
        }
        else {
            // On Windows it is important that we unmap before unlocking, else a SetEndOfFile() call from another
            // thread may interleave which is not permitted on Windows. It is permitted on *nix.
            m_file_map.unmap();
            m_version_manager.reset();
            m_file.rw_unlock();
            // info->~SharedInfo(); // DO NOT Call destructor
            m_file.close();
        }
        m_info = nullptr;
        if (m_logger)
            m_logger->log(util::Logger::Level::detail, "DB closed");
    }
}

bool DB::other_writers_waiting_for_lock() const
{
    SharedInfo* info = m_info;

    uint32_t next_ticket = info->next_ticket.load(std::memory_order_relaxed);
    uint32_t next_served = info->next_served.load(std::memory_order_relaxed);
    // When holding the write lock, next_ticket = next_served + 1, hence, if the diference between 'next_ticket' and
    // 'next_served' is greater than 1, there is at least one thread waiting to acquire the write lock.
    return next_ticket > next_served + 1;
}

void DB::AsyncCommitHelper::main()
{
    std::unique_lock lg(m_mutex);
    while (m_running) {
#if 0 // Enable for testing purposes
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
#endif
        if (m_has_write_mutex) {
            if (auto cb = std::move(m_pending_sync)) {
                // Only one of sync_to_disk(), end_write(), or blocking_end_write()
                // should be called, so we should never have both a pending sync
                // and pending release.
                REALM_ASSERT(!m_pending_mx_release);
                lg.unlock();
                cb();
                cb = nullptr; // Release things captured by the callback before reacquiring the lock
                lg.lock();
                m_pending_mx_release = true;
            }
            if (m_pending_mx_release) {
                REALM_ASSERT(!InterprocessMutex::is_thread_confined || m_owns_write_mutex);
                m_db->do_end_write();
                m_pending_mx_release = false;
                m_has_write_mutex = false;
                m_owns_write_mutex = false;

                lg.unlock();
                m_cv_callers.notify_all();
                lg.lock();
                continue;
            }
        }
        else {
            REALM_ASSERT(!m_pending_sync && !m_pending_mx_release);

            // Acquire the write lock if anyone has requested it, but only if
            // another thread is not already waiting for it. If there's another
            // thread requesting and they get it while we're waiting, we'll
            // deadlock if they ask us to perform the sync.
            if (!m_waiting_for_write_mutex && has_pending_write_requests()) {
                lg.unlock();
                m_db->do_begin_write();
                lg.lock();

                REALM_ASSERT(!m_has_write_mutex);
                m_has_write_mutex = true;
                m_owns_write_mutex = true;

                // Synchronous transaction requests get priority over async
                if (m_write_lock_claim_fulfilled < m_write_lock_claim_ticket) {
                    ++m_write_lock_claim_fulfilled;
                    m_cv_callers.notify_all();
                    continue;
                }

                REALM_ASSERT(!m_pending_writes.empty());
                auto callback = std::move(m_pending_writes.front());
                m_pending_writes.pop_front();
                lg.unlock();
                callback();
                // Release things captured by the callback before reacquiring the lock
                callback = nullptr;
                lg.lock();
                continue;
            }
        }
        m_cv_worker.wait(lg);
    }
    if (m_has_write_mutex && m_owns_write_mutex) {
        m_db->do_end_write();
    }
}

void DB::async_begin_write(util::UniqueFunction<void()> fn)
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->begin_write(std::move(fn));
}

void DB::async_end_write()
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->end_write();
}

void DB::async_sync_to_disk(util::UniqueFunction<void()> fn)
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->sync_to_disk(std::move(fn));
}

bool DB::has_changed(TransactionRef& tr)
{
    if (m_fake_read_lock_if_immutable)
        return false; // immutable doesn't change
    bool changed = tr->m_read_lock.m_version != get_version_of_latest_snapshot();
    return changed;
}

bool DB::wait_for_change(TransactionRef& tr)
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    while (tr->m_read_lock.m_version == m_info->latest_version_number && m_wait_for_change_enabled) {
        m_new_commit_available.wait(m_controlmutex, 0);
    }
    return tr->m_read_lock.m_version != m_info->latest_version_number;
}


void DB::wait_for_change_release()
{
    if (m_fake_read_lock_if_immutable)
        return;
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    m_wait_for_change_enabled = false;
    m_new_commit_available.notify_all();
}


void DB::enable_wait_for_change()
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    m_wait_for_change_enabled = true;
}

bool DB::needs_file_format_upgrade(const std::string& file, const std::vector<char>& encryption_key)
{
    SlabAlloc alloc;
    SlabAlloc::Config cfg;
    cfg.session_initiator = false;
    cfg.read_only = true;
    cfg.no_create = true;
    if (!encryption_key.empty()) {
        cfg.encryption_key = encryption_key.data();
    }
    try {
        alloc.attach_file(file, cfg);
        if (auto current_file_format_version = alloc.get_committed_file_format_version()) {
            auto target_file_format_version = Group::g_current_file_format_version;
            return current_file_format_version < target_file_format_version;
        }
    }
    catch (const FileAccessError& err) {
        if (err.code() != ErrorCodes::FileNotFound) {
            throw;
        }
    }
    return false;
}

void DB::upgrade_file_format(bool allow_file_format_upgrade, int target_file_format_version,
                             int current_hist_schema_version, int target_hist_schema_version)
{
    // In a multithreaded scenario multiple threads may initially see a need to
    // upgrade (maybe_upgrade == true) even though one onw thread is supposed to
    // perform the upgrade, but that is ok, because the condition is rechecked
    // in a fully reliable way inside a transaction.

    // First a non-threadsafe but fast check
    int current_file_format_version = m_file_format_version;
    REALM_ASSERT(current_file_format_version <= target_file_format_version);
    REALM_ASSERT(current_hist_schema_version <= target_hist_schema_version);
    bool maybe_upgrade_file_format = (current_file_format_version < target_file_format_version);
    bool maybe_upgrade_hist_schema = (current_hist_schema_version < target_hist_schema_version);
    bool maybe_upgrade = maybe_upgrade_file_format || maybe_upgrade_hist_schema;
    if (maybe_upgrade) {

#ifdef REALM_DEBUG
// This sleep() only exists in order to increase the quality of the
// TEST(Upgrade_Database_2_3_Writes_New_File_Format_new) unit test.
// The unit test creates multiple threads that all call
// upgrade_file_format() simultaneously. This sleep() then acts like
// a simple thread barrier that makes sure the threads meet here, to
// increase the likelyhood of detecting any potential race problems.
// See the unit test for details.
//
// NOTE: This sleep has been disabled because no problems have been found with
// this code in a long while, and it was dramatically slowing down a unit test
// in realm-sync.

// millisleep(200);
#endif

        // WriteTransaction wt(*this);
        auto wt = start_write();
        bool dirty = false;

        // We need to upgrade history first. We may need to access it during migration
        // when processing the !OID columns
        int current_hist_schema_version_2 = wt->get_history_schema_version();
        // The history must either still be using its initial schema or have
        // been upgraded already to the chosen target schema version via a
        // concurrent DB object.
        REALM_ASSERT(current_hist_schema_version_2 == current_hist_schema_version ||
                     current_hist_schema_version_2 == target_hist_schema_version);
        bool need_hist_schema_upgrade = (current_hist_schema_version_2 < target_hist_schema_version);
        if (need_hist_schema_upgrade) {
            if (!allow_file_format_upgrade)
                throw FileFormatUpgradeRequired(this->m_db_path);

            Replication* repl = get_replication();
            repl->upgrade_history_schema(current_hist_schema_version_2); // Throws
            wt->set_history_schema_version(target_hist_schema_version);  // Throws
            dirty = true;
        }

        // File format upgrade
        int current_file_format_version_2 = m_alloc.get_committed_file_format_version();
        // The file must either still be using its initial file_format or have
        // been upgraded already to the chosen target file format via a
        // concurrent DB object.
        REALM_ASSERT(current_file_format_version_2 == current_file_format_version ||
                     current_file_format_version_2 == target_file_format_version);
        bool need_file_format_upgrade = (current_file_format_version_2 < target_file_format_version);
        if (need_file_format_upgrade) {
            if (!allow_file_format_upgrade)
                throw FileFormatUpgradeRequired(this->m_db_path);
            wt->upgrade_file_format(target_file_format_version); // Throws
            // Note: The file format version stored in the Realm file will be
            // updated to the new file format version as part of the following
            // commit operation. This happens in GroupWriter::commit().
            if (m_upgrade_callback)
                m_upgrade_callback(current_file_format_version_2, target_file_format_version); // Throws
            dirty = true;
        }
        wt->set_file_format_version(target_file_format_version);
        m_file_format_version = target_file_format_version;

        if (dirty)
            wt->commit(); // Throws
    }
}

void DB::release_read_lock(ReadLockInfo& read_lock) noexcept
{
    // ignore if opened with immutable file (then we have no lockfile)
    if (m_fake_read_lock_if_immutable)
        return;
    CheckedLockGuard lock(m_mutex); // mx on m_local_locks_held
    do_release_read_lock(read_lock);
}

// this is called with m_mutex locked
void DB::do_release_read_lock(ReadLockInfo& read_lock) noexcept
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    bool found_match = false;
    // simple linear search and move-last-over if a match is found.
    // common case should have only a modest number of transactions in play..
    for (size_t j = 0; j < m_local_locks_held.size(); ++j) {
        if (m_local_locks_held[j].m_version == read_lock.m_version) {
            m_local_locks_held[j] = m_local_locks_held.back();
            m_local_locks_held.pop_back();
            found_match = true;
            break;
        }
    }
    if (!found_match) {
        REALM_ASSERT(!is_attached());
        // it's OK, someone called close() and all locks where released
        return;
    }
    --m_transaction_count;
    m_version_manager->release_read_lock(read_lock);
}


DB::ReadLockInfo DB::grab_read_lock(ReadLockInfo::Type type, VersionID version_id)
{
    CheckedLockGuard lock(m_mutex); // mx on m_local_locks_held
    REALM_ASSERT_RELEASE(is_attached());
    auto read_lock = m_version_manager->grab_read_lock(type, version_id);

    m_local_locks_held.emplace_back(read_lock);
    ++m_transaction_count;
    REALM_ASSERT(read_lock.m_file_size > read_lock.m_top_ref);
    return read_lock;
}

void DB::leak_read_lock(ReadLockInfo& read_lock) noexcept
{
    CheckedLockGuard lock(m_mutex); // mx on m_local_locks_held
    // simple linear search and move-last-over if a match is found.
    // common case should have only a modest number of transactions in play..
    for (size_t j = 0; j < m_local_locks_held.size(); ++j) {
        if (m_local_locks_held[j].m_version == read_lock.m_version) {
            m_local_locks_held[j] = m_local_locks_held.back();
            m_local_locks_held.pop_back();
            --m_transaction_count;
            return;
        }
    }
}

bool DB::do_try_begin_write()
{
    // In the non-blocking case, we will only succeed if there is no contention for
    // the write mutex. For this case we are trivially fair and can ignore the
    // fairness machinery.
    bool got_the_lock = m_writemutex.try_lock();
    if (got_the_lock) {
        finish_begin_write();
    }
    return got_the_lock;
}

void DB::do_begin_write()
{
    if (m_logger) {
        m_logger->log(util::LogCategory::transaction, util::Logger::Level::trace, "acquire writemutex");
    }

    SharedInfo* info = m_info;

    // Get write lock - the write lock is held until do_end_write().
    //
    // We use a ticketing scheme to ensure fairness wrt performing write transactions.
    // (But cannot do that on Windows until we have interprocess condition variables there)
    uint32_t my_ticket = info->next_ticket.fetch_add(1, std::memory_order_relaxed);
    m_writemutex.lock(); // Throws

    // allow for comparison even after wrap around of ticket numbering:
    int32_t diff = int32_t(my_ticket - info->next_served.load(std::memory_order_relaxed));
    bool should_yield = diff > 0; // ticket is in the future
    // a) the above comparison is only guaranteed to be correct, if the distance
    //    between my_ticket and info->next_served is less than 2^30. This will
    //    be the case since the distance will be bounded by the number of threads
    //    and each thread cannot ever hold more than one ticket.
    // b) we could use 64 bit counters instead, but it is unclear if all platforms
    //    have support for interprocess atomics for 64 bit values.

    timespec time_limit; // only compute the time limit if we're going to use it:
    if (should_yield) {
        // This clock is not monotonic, so time can move backwards. This can lead
        // to a wrong time limit, but the only effect of a wrong time limit is that
        // we momentarily lose fairness, so we accept it.
        timeval tv;
        gettimeofday(&tv, nullptr);
        time_limit.tv_sec = tv.tv_sec;
        time_limit.tv_nsec = tv.tv_usec * 1000;
        time_limit.tv_nsec += 500000000;        // 500 msec wait
        if (time_limit.tv_nsec >= 1000000000) { // overflow
            time_limit.tv_nsec -= 1000000000;
            time_limit.tv_sec += 1;
        }
    }

    while (should_yield) {

        m_pick_next_writer.wait(m_writemutex, &time_limit);
        timeval tv;
        gettimeofday(&tv, nullptr);
        if (time_limit.tv_sec < tv.tv_sec ||
            (time_limit.tv_sec == tv.tv_sec && time_limit.tv_nsec < tv.tv_usec * 1000)) {
            // Timeout!
            break;
        }
        diff = int32_t(my_ticket - info->next_served);
        should_yield = diff > 0; // ticket is in the future, so yield to someone else
    }

    // we may get here because a) it's our turn, b) we timed out
    // we don't distinguish, satisfied that event b) should be rare.
    // In case b), we have to *make* it our turn. Failure to do so could leave us
    // with 'next_served' permanently trailing 'next_ticket'.
    //
    // In doing so, we may bypass other waiters, hence the condition for yielding
    // should take this situation into account by comparing with '>' instead of '!='
    info->next_served = my_ticket;
    finish_begin_write();
    if (m_logger) {
        m_logger->log(util::LogCategory::transaction, util::Logger::Level::trace, "writemutex acquired");
    }
}

void DB::finish_begin_write()
{
    if (m_info->commit_in_critical_phase) {
        m_writemutex.unlock();
        throw RuntimeError(ErrorCodes::BrokenInvariant, "Crash of other process detected, session restart required");
    }


    {
        CheckedLockGuard local_lock(m_mutex);
        m_write_transaction_open = true;
    }
    m_alloc.set_read_only(false);
}

void DB::do_end_write() noexcept
{
    m_info->next_served.fetch_add(1, std::memory_order_relaxed);

    CheckedLockGuard local_lock(m_mutex);
    REALM_ASSERT(m_write_transaction_open);
    m_alloc.set_read_only(true);
    m_write_transaction_open = false;
    m_pick_next_writer.notify_all();
    m_writemutex.unlock();
    if (m_logger) {
        m_logger->log(util::LogCategory::transaction, util::Logger::Level::trace, "writemutex released");
    }
}


Replication::version_type DB::do_commit(Transaction& transaction, bool commit_to_disk)
{
    version_type current_version;
    {
        current_version = m_version_manager->get_newest_version();
    }
    version_type new_version = current_version + 1;

    if (!transaction.m_tables_to_clear.empty()) {
        for (auto table_key : transaction.m_tables_to_clear) {
            transaction.get_table_unchecked(table_key)->clear();
        }
        transaction.m_tables_to_clear.clear();
    }
    if (Replication* repl = get_replication()) {
        // If Replication::prepare_commit() fails, then the entire transaction
        // fails. The application then has the option of terminating the
        // transaction with a call to Transaction::Rollback(), which in turn
        // must call Replication::abort_transact().
        new_version = repl->prepare_commit(current_version);        // Throws
        low_level_commit(new_version, transaction, commit_to_disk); // Throws
        repl->finalize_commit();
    }
    else {
        low_level_commit(new_version, transaction); // Throws
    }

    {
        std::lock_guard lock(m_commit_listener_mutex);
        for (auto listener : m_commit_listeners) {
            listener->on_commit(new_version);
        }
    }

    return new_version;
}

VersionID DB::get_version_id_of_latest_snapshot()
{
    if (m_fake_read_lock_if_immutable)
        return {m_fake_read_lock_if_immutable->m_version, 0};
    return m_version_manager->get_version_id_of_latest_snapshot();
}


DB::version_type DB::get_version_of_latest_snapshot()
{
    return get_version_id_of_latest_snapshot().version;
}


void DB::low_level_commit(uint_fast64_t new_version, Transaction& transaction, bool commit_to_disk)
{
    SharedInfo* info = m_info;

    // Version of oldest snapshot currently (or recently) bound in a transaction
    // of the current session.
    uint64_t oldest_version = 0, oldest_live_version = 0;
    TopRefMap top_refs;
    bool any_new_unreachables;
    {
        CheckedLockGuard lock(m_mutex);
        m_version_manager->cleanup_versions(oldest_live_version, top_refs, any_new_unreachables);
        oldest_version = top_refs.begin()->first;
        // Allow for trimming of the history. Some types of histories do not
        // need store changesets prior to the oldest *live* bound snapshot.
        if (auto hist = transaction.get_history()) {
            hist->set_oldest_bound_version(oldest_live_version); // Throws
        }
        // Cleanup any stale mappings
        m_alloc.purge_old_mappings(oldest_version, new_version);
    }
    // save number of live versions for later:
    // (top_refs is std::moved into GroupWriter so we'll loose it in the call to set_versions below)
    auto live_versions = top_refs.size();
    // Do the actual commit
    REALM_ASSERT(oldest_version <= new_version);

    GroupWriter out(transaction, Durability(info->durability), m_marker_observer.get()); // Throws
    out.set_versions(new_version, top_refs, any_new_unreachables);
    out.prepare_evacuation();
    auto t1 = std::chrono::steady_clock::now();
    auto commit_size = m_alloc.get_commit_size();
    if (m_logger) {
        m_logger->log(util::LogCategory::transaction, util::Logger::Level::debug, "Initiate commit version: %1",
                      new_version);
    }
    if (auto limit = out.get_evacuation_limit()) {
        // Get a work limit based on the size of the transaction we're about to commit
        // Add 4k to ensure progress on small commits
        size_t work_limit = commit_size / 2 + out.get_free_list_size() + 0x1000;
        transaction.cow_outliers(out.get_evacuation_progress(), limit, work_limit);
    }

    ref_type new_top_ref;
    // Recursively write all changed arrays to end of file
    {
        // protect against race with any other DB trying to attach to the file
        std::lock_guard<InterprocessMutex> lock(m_controlmutex); // Throws
        new_top_ref = out.write_group();                         // Throws
    }
    {
        // protect access to shared variables and m_reader_mapping from here
        CheckedLockGuard lock_guard(m_mutex);
        m_free_space = out.get_free_space_size();
        m_locked_space = out.get_locked_space_size();
        m_used_space = out.get_logical_size() - m_free_space;
        m_evac_stage.store(EvacStage(out.get_evacuation_stage()));
        out.sync_according_to_durability();
        if (Durability(info->durability) == Durability::Full || Durability(info->durability) == Durability::Unsafe) {
            if (commit_to_disk) {
                GroupCommitter cm(transaction, Durability(info->durability), m_marker_observer.get());
                cm.commit(new_top_ref);
            }
        }
        size_t new_file_size = out.get_logical_size();
        // We must reset the allocators free space tracking before communicating the new
        // version through the ring buffer. If not, a reader may start updating the allocators
        // mappings while the allocator is in dirty state.
        reset_free_space_tracking();
        // Add the new version. If this fails in any way, the VersionList may be corrupted.
        // This can lead to readers seing invalid data which is likely to cause them
        // to crash. Other writers *must* be prevented from writing any further updates
        // to the database. The flag "commit_in_critical_phase" is used to prevent such updates.
        info->commit_in_critical_phase = 1;
        {
            m_version_manager->add_version(new_top_ref, new_file_size, new_version);

            // REALM_ASSERT(m_alloc.matches_section_boundary(new_file_size));
            REALM_ASSERT(new_top_ref < new_file_size);
        }
        // At this point, the VersionList has been succesfully updated, and the next writer
        // can safely proceed once the writemutex has been lifted.
        info->commit_in_critical_phase = 0;
    }
    {
        // protect against concurrent updates to the .lock file.
        // must release m_mutex before this point to obey lock order
        std::lock_guard<InterprocessMutex> lock(m_controlmutex);

        info->number_of_versions = live_versions + 1;
        info->latest_version_number = new_version;

        m_new_commit_available.notify_all();
    }
    auto t2 = std::chrono::steady_clock::now();
    if (m_logger) {
        std::string to_disk_str = commit_to_disk ? util::format(" ref %1", new_top_ref) : " (no commit to disk)";
        m_logger->log(util::LogCategory::transaction, util::Logger::Level::debug, "Commit of size %1 done in %2 us%3",
                      commit_size, std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count(),
                      to_disk_str);
    }
}

#ifdef REALM_DEBUG
void DB::reserve(size_t size)
{
    REALM_ASSERT(is_attached());
    m_alloc.reserve_disk_space(size); // Throws
}
#endif

bool DB::call_with_lock(const std::string& realm_path, CallbackWithLock&& callback)
{
    auto lockfile_path = get_core_file(realm_path, CoreFileType::Lock);

    File lockfile;
    lockfile.open(lockfile_path, File::access_ReadWrite, File::create_Auto, 0); // Throws
    File::CloseGuard fcg(lockfile);
    lockfile.set_fifo_path(realm_path + ".management", "lock.fifo");
    if (lockfile.try_rw_lock_exclusive()) { // Throws
        callback(realm_path);
        return true;
    }
    return false;
}

std::string DB::get_core_file(const std::string& base_path, CoreFileType type)
{
    switch (type) {
        case CoreFileType::Lock:
            return base_path + ".lock";
        case CoreFileType::Storage:
            return base_path;
        case CoreFileType::Management:
            return base_path + ".management";
        case CoreFileType::Note:
            return base_path + ".note";
        case CoreFileType::Log:
            return base_path + ".log";
    }
    REALM_UNREACHABLE();
}

void DB::delete_files(const std::string& base_path, bool* did_delete, bool delete_lockfile)
{
    if (File::try_remove(get_core_file(base_path, CoreFileType::Storage)) && did_delete) {
        *did_delete = true;
    }

    File::try_remove(get_core_file(base_path, CoreFileType::Note));
    File::try_remove(get_core_file(base_path, CoreFileType::Log));
    util::try_remove_dir_recursive(get_core_file(base_path, CoreFileType::Management));

    if (delete_lockfile) {
        File::try_remove(get_core_file(base_path, CoreFileType::Lock));
    }
}

TransactionRef DB::start_read(VersionID version_id)
{
    if (!is_attached())
        throw StaleAccessor("Stale transaction");
    TransactionRef tr;
    if (m_fake_read_lock_if_immutable) {
        tr = make_transaction_ref(shared_from_this(), &m_alloc, *m_fake_read_lock_if_immutable, DB::transact_Reading);
    }
    else {
        ReadLockInfo read_lock = grab_read_lock(ReadLockInfo::Live, version_id);
        ReadLockGuard g(*this, read_lock);
        read_lock.check();
        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Reading);
        g.release();
    }
    tr->set_file_format_version(get_file_format_version());
    return tr;
}

TransactionRef DB::start_frozen(VersionID version_id)
{
    if (!is_attached())
        throw StaleAccessor("Stale transaction");
    TransactionRef tr;
    if (m_fake_read_lock_if_immutable) {
        tr = make_transaction_ref(shared_from_this(), &m_alloc, *m_fake_read_lock_if_immutable, DB::transact_Frozen);
    }
    else {
        ReadLockInfo read_lock = grab_read_lock(ReadLockInfo::Frozen, version_id);
        ReadLockGuard g(*this, read_lock);
        read_lock.check();
        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Frozen);
        g.release();
    }
    tr->set_file_format_version(get_file_format_version());
    return tr;
}

TransactionRef DB::start_write(bool nonblocking)
{
    if (m_fake_read_lock_if_immutable) {
        REALM_ASSERT(false && "Can't write an immutable DB");
    }
    if (nonblocking) {
        bool success = do_try_begin_write();
        if (!success) {
            return TransactionRef();
        }
    }
    else {
        do_begin_write();
    }
    {
        CheckedUniqueLock local_lock(m_mutex);
        if (!is_attached()) {
            local_lock.unlock();
            end_write_on_correct_thread();
            throw StaleAccessor("Stale transaction");
        }
        m_write_transaction_open = true;
    }
    TransactionRef tr;
    try {
        ReadLockInfo read_lock = grab_read_lock(ReadLockInfo::Live, VersionID());
        ReadLockGuard g(*this, read_lock);
        read_lock.check();

        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Writing);
        tr->set_file_format_version(get_file_format_version());
        version_type current_version = read_lock.m_version;
        m_alloc.init_mapping_management(current_version);
        if (Replication* repl = get_replication()) {
            bool history_updated = false;
            repl->initiate_transact(*tr, current_version, history_updated); // Throws
        }
        g.release();
    }
    catch (...) {
        end_write_on_correct_thread();
        throw;
    }

    return tr;
}

void DB::async_request_write_mutex(TransactionRef& tr, util::UniqueFunction<void()>&& when_acquired)
{
    {
        util::CheckedLockGuard lck(tr->m_async_mutex);
        REALM_ASSERT(tr->m_async_stage == Transaction::AsyncState::Idle);
        tr->m_async_stage = Transaction::AsyncState::Requesting;
        tr->m_request_time_point = std::chrono::steady_clock::now();
        if (tr->db->m_logger) {
            tr->db->m_logger->log(util::LogCategory::transaction, util::Logger::Level::trace,
                                  "Tr %1: Async request write lock", tr->m_log_id);
        }
    }
    std::weak_ptr<Transaction> weak_tr = tr;
    async_begin_write([weak_tr, cb = std::move(when_acquired)]() {
        if (auto tr = weak_tr.lock()) {
            util::CheckedLockGuard lck(tr->m_async_mutex);
            // If a synchronous transaction happened while we were pending
            // we may be in HasCommits
            if (tr->m_async_stage == Transaction::AsyncState::Requesting) {
                tr->m_async_stage = Transaction::AsyncState::HasLock;
            }
            if (tr->db->m_logger) {
                auto t2 = std::chrono::steady_clock::now();
                tr->db->m_logger->log(
                    util::LogCategory::transaction, util::Logger::Level::trace, "Tr %1, Got write lock in %2 us",
                    tr->m_log_id,
                    std::chrono::duration_cast<std::chrono::microseconds>(t2 - tr->m_request_time_point).count());
            }
            if (tr->m_waiting_for_write_lock) {
                tr->m_waiting_for_write_lock = false;
                tr->m_async_cv.notify_one();
            }
            else if (cb) {
                cb();
            }
            tr.reset(); // Release pointer while lock is held
        }
    });
}

inline DB::DB(Private, const DBOptions& options)
    : m_upgrade_callback(std::move(options.upgrade_callback))
    , m_log_id(util::gen_log_id(this))
{
    if (options.enable_async_writes) {
        m_commit_helper = std::make_unique<AsyncCommitHelper>(this);
    }
}

DBRef DB::create(const std::string& file, const DBOptions& options) NO_THREAD_SAFETY_ANALYSIS
{
    DBRef retval = std::make_shared<DB>(Private(), options);
    retval->open(file, options);
    return retval;
}

DBRef DB::create(Replication& repl, const std::string& file, const DBOptions& options) NO_THREAD_SAFETY_ANALYSIS
{
    DBRef retval = std::make_shared<DB>(Private(), options);
    retval->open(repl, file, options);
    return retval;
}

DBRef DB::create(std::unique_ptr<Replication> repl, const std::string& file,
                 const DBOptions& options) NO_THREAD_SAFETY_ANALYSIS
{
    REALM_ASSERT(repl);
    DBRef retval = std::make_shared<DB>(Private(), options);
    retval->m_history = std::move(repl);
    retval->open(*retval->m_history, file, options);
    return retval;
}

DBRef DB::create(std::unique_ptr<Replication> repl, const DBOptions& options) NO_THREAD_SAFETY_ANALYSIS
{
    REALM_ASSERT(repl);
    DBRef retval = std::make_shared<DB>(Private(), options);
    retval->m_history = std::move(repl);
    retval->open(*retval->m_history, options);
    return retval;
}

DBRef DB::create_in_memory(std::unique_ptr<Replication> repl, const std::string& in_memory_path,
                           const DBOptions& options) NO_THREAD_SAFETY_ANALYSIS
{
    DBRef db = create(std::move(repl), options);
    db->m_db_path = in_memory_path;
    return db;
}

DBRef DB::create(BinaryData buffer, bool take_ownership) NO_THREAD_SAFETY_ANALYSIS
{
    DBOptions options;
    options.is_immutable = true;
    DBRef retval = std::make_shared<DB>(Private(), options);
    retval->open(buffer, take_ownership);
    return retval;
}

bool DB::try_claim_sync_agent()
{
    REALM_ASSERT(is_attached());
    std::lock_guard lock(m_controlmutex);
    if (m_info->sync_agent_present)
        return false;
    m_info->sync_agent_present = 1; // Set to true
    m_is_sync_agent = true;
    return true;
}

void DB::claim_sync_agent()
{
    if (!try_claim_sync_agent())
        throw MultipleSyncAgents{};
}

void DB::release_sync_agent()
{
    REALM_ASSERT(is_attached());
    std::lock_guard lock(m_controlmutex);
    if (!m_is_sync_agent)
        return;
    REALM_ASSERT(m_info->sync_agent_present);
    m_info->sync_agent_present = 0;
    m_is_sync_agent = false;
}

void DB::do_begin_possibly_async_write()
{
    if (m_commit_helper) {
        m_commit_helper->blocking_begin_write();
    }
    else {
        do_begin_write();
    }
}

void DB::end_write_on_correct_thread() noexcept
{
    //    m_local_write_mutex.unlock();
    if (!m_commit_helper || !m_commit_helper->blocking_end_write()) {
        do_end_write();
    }
}

void DB::add_commit_listener(CommitListener* listener)
{
    std::lock_guard lock(m_commit_listener_mutex);
    m_commit_listeners.push_back(listener);
}

void DB::remove_commit_listener(CommitListener* listener)
{
    std::lock_guard lock(m_commit_listener_mutex);
    m_commit_listeners.erase(std::remove(m_commit_listeners.begin(), m_commit_listeners.end(), listener),
                             m_commit_listeners.end());
}

DisableReplication::DisableReplication(Transaction& t)
    : m_tr(t)
    , m_owner(t.get_db())
    , m_repl(m_owner->get_replication())
    , m_version(t.get_version())
{
    m_owner->set_replication(nullptr);
    t.m_history = nullptr;
}

DisableReplication::~DisableReplication()
{
    m_owner->set_replication(m_repl);
    if (m_version != m_tr.get_version())
        m_tr.initialize_replication();
}

} // namespace realm
