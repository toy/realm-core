/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#ifndef REALM_ARRAY_COMPRESS_HPP
#define REALM_ARRAY_COMPRESS_HPP

#include <realm/array.hpp>

namespace realm {
//
// This class represents the basic interface that every compressed array must implement.
//
class ArrayEncode : public Array {
public:
    explicit ArrayEncode(Array& array);
    virtual ~ArrayEncode() = default;
    virtual void init_array_encode(MemRef) = 0;
    virtual bool encode() = 0;
    virtual bool decode() = 0;
    virtual bool is_encoded() const = 0;

    char* get_data() const
    {
        return m_data;
    }
    virtual size_t size() const = 0;
    virtual int64_t get(size_t) const = 0;

    //
    // Factory method to construct the proper encoded array
    //
    static ArrayEncode* create_encoded_array(NodeHeader::Encoding encoding, Array& array);

protected:
    Array& m_array;
};


} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP