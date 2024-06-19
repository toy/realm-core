#include "test.hpp"
#include "util/random.hpp"

#include <realm/sync/network/network.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/network/default_socket.hpp>

#include <queue>

using namespace realm;
using namespace realm::sync;
using namespace realm::sync::websocket;

struct WebSocketEvent {
    enum Type {
        ReadError,
        WriteError,
        HandshakeIgnored,
        HandshakeError,
        ProtocolError,
        HandshakeComplete,
        BinaryMessage,
        CloseFrame
    } type;
    std::string payload;
    WebSocketError close_code = WebSocketError::websocket_ok;
};

class WebSocketEventQueue {
public:
    ~WebSocketEventQueue()
    {
        REALM_ASSERT_RELEASE(waiters.empty());
        REALM_ASSERT_RELEASE(events.empty());
    }

    template <typename... Args>
    void add_event(WebSocketEvent::Type type, Args... args)
    {
        WebSocketEvent event{type, args...};
        std::lock_guard lk(mutex);
        if (!waiters.empty()) {
            waiters.front().emplace_value(std::move(event));
            waiters.pop();
        }
        else {
            events.push(std::move(event));
        }
    }

    util::Future<WebSocketEvent> next_event()
    {
        std::unique_lock lk(mutex);
        if (events.empty()) {
            auto pf = util::make_promise_future<WebSocketEvent>();
            waiters.push(std::move(pf.promise));
            return std::move(pf.future);
        }
        else {
            auto ret = std::move(events.front());
            events.pop();
            return ret;
        }
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<util::Promise<WebSocketEvent>> waiters;
    std::queue<WebSocketEvent> events;
};

class WebSocketEventQueueObserver : public WebSocketObserver {
public:
    static auto make_observer_and_queue()
    {
        struct ObserverAndQueue {
            std::unique_ptr<WebSocketEventQueueObserver> observer;
            std::shared_ptr<WebSocketEventQueue> queue = std::make_shared<WebSocketEventQueue>();
        };

        ObserverAndQueue ret;
        ret.observer = std::make_unique<WebSocketEventQueueObserver>(ret.queue);
        return ret;
    }

    explicit WebSocketEventQueueObserver(std::shared_ptr<WebSocketEventQueue> queue)
        : m_queue(std::move(queue))
    {
    }

    void websocket_connected_handler(const std::string& protocol) override
    {
        m_queue->add_event(WebSocketEvent::HandshakeComplete, protocol);
    }

    void websocket_error_handler() override
    {
        m_queue->add_event(WebSocketEvent::ReadError);
    }

    void websocket_binary_message_received(util::Span<const char> data) override
    {
        m_queue->add_event(WebSocketEvent::BinaryMessage, std::string{data.data(), data.size()});
    }

    void websocket_closed_handler(bool, WebSocketError error, std::string_view msg) override
    {
        m_queue->add_event(WebSocketEvent::CloseFrame, std::string{msg}, error);
    }

private:
    std::shared_ptr<WebSocketEventQueue> m_queue;
};

static std::unique_ptr<WebSocketInterface>
do_connect(DefaultSocketProvider& provider, std::unique_ptr<WebSocketObserver> observer, WebSocketEndpoint ep)
{
    auto pf = util::make_promise_future<std::unique_ptr<WebSocketInterface>>();
    provider.post([&](Status status) {
        REALM_ASSERT(status.is_ok());
        pf.promise.emplace_value(provider.connect(std::move(observer), std::move(ep)));
    });
    return std::move(pf.future.get());
}

template <typename Service, typename Func>
static void do_synchronous_post(Service& service, Func&& func)
{
    auto pf = util::make_promise_future();
    service.post([&](Status status) {
        REALM_ASSERT(status.is_ok());
        func();
        pf.promise.emplace_value();
    });
    pf.future.get();
}

class TestWebSocketServer {
public:
    TestWebSocketServer(test_util::unit_test::TestContext& test_context)
        : m_test_context(test_context)
        , m_logger(std::make_shared<util::PrefixLogger>("TestWebSocketServer ", m_test_context.logger))
        , m_acceptor(m_service)
        , m_server_thread([this] {
            m_service.run_until_stopped();
        })
    {
        do_synchronous_post(m_service, [this]() mutable {
            m_acceptor.open(m_endpoint.protocol());
            m_acceptor.set_option(network::SocketBase::reuse_address(true));
            m_acceptor.bind(m_endpoint);
            m_endpoint = m_acceptor.local_endpoint();
            m_acceptor.listen();
            m_logger->debug("Listening on port %1", m_endpoint.port());
        });
    }

    ~TestWebSocketServer()
    {
        m_acceptor.cancel();
        m_service.drain();
        m_server_thread.join();
    }

    WebSocketEndpoint endpoint() const
    {
        WebSocketEndpoint ep;
        ep.port = m_endpoint.port();
        ep.path = "/";
        ep.is_ssl = false;
        ep.address = "localhost";
        ep.protocols = {"RealmTestWebSocket#1"};
        return ep;
    }

    void post(util::UniqueFunction<void()>&& fn)
    {
        m_service.post([fn = std::move(fn)](Status status) {
            REALM_ASSERT(status.is_ok());
            fn();
        });
    }

    struct Conn : websocket::Config, util::AtomicRefCountBase {
        Conn(network::Service& service, test_util::unit_test::TestContext& test_context)
            : random{test_util::produce_nondeterministic_random_seed()}
            , logger(test_context.logger)
            , service(service)
            , socket(service)
            , http_server(*this, logger)
            , websocket(*this)
        {
        }

        ~Conn()
        {
            do_synchronous_post(service, [this] {
                shutdown_websocket();
            });
        }

        util::Future<void> send_binary_message(util::Span<char const> data)
        {
            auto pf = util::make_promise_future<void>();
            service.post([self = util::bind_ptr(this), promise = std::move(pf.promise), data](Status status) mutable {
                REALM_ASSERT(status.is_ok());
                self->websocket.async_write_binary(
                    data.data(), data.size(),
                    [self, promise = std::move(promise)](std::error_code ec, size_t) mutable {
                        if (ec) {
                            promise.set_error(status_from_network_error_code(ec));
                        }
                        else {
                            promise.emplace_value();
                        }
                    });
            });
            return std::move(pf.future);
        }

        util::Future<void> send_close_frame(WebSocketError error, std::string_view msg)
        {
            auto pf = util::make_promise_future<void>();
            std::vector<char> msg_data(2 + msg.size());
            uint16_t error_short = htons(static_cast<uint16_t>(error));
            msg_data[0] = error_short & 0xff;
            msg_data[1] = (error_short >> 8) & 0xff;
            std::copy(msg.begin(), msg.end(), msg_data.begin() + 2);

            service.post([self = util::bind_ptr(this), msg = std::move(msg_data),
                          promise = std::move(pf.promise)](Status status) mutable {
                REALM_ASSERT(status.is_ok());
                self->websocket.async_write_close(
                    msg.data(), msg.size(), [self, promise = std::move(promise)](std::error_code ec, size_t) mutable {
                        if (ec) {
                            promise.set_error(status_from_network_error_code(ec));
                        }
                        else {
                            promise.emplace_value();
                        }
                    });
            });
            return std::move(pf.future);
        }

        util::Future<HTTPRequest> initiate_server_handshake()
        {
            auto pf = util::make_promise_future<HTTPRequest>();
            service.post([self = util::bind_ptr(this), promise = std::move(pf.promise)](Status status) mutable {
                REALM_ASSERT(status.is_ok());
                self->http_server.async_receive_request(
                    [self, promise = std::move(promise)](HTTPRequest req, std::error_code ec) mutable {
                        if (ec) {
                            promise.set_error(status_from_network_error_code(ec));
                        }
                        else {
                            promise.emplace_value(std::move(req));
                        }
                    });
            });
            return std::move(pf.future);
        }

        util::Future<void> complete_server_handshake(HTTPRequest&& req)
        {
            auto pf = util::make_promise_future<void>();
            auto protocol_it = req.headers.find("Sec-WebSocket-Protocol");
            REALM_ASSERT(protocol_it != req.headers.end());
            auto protocols = protocol_it->second;

            auto first_comma = protocols.find(',');
            std::string protocol;
            if (first_comma == std::string::npos) {
                protocol = protocols;
            }
            else {
                protocol = protocols.substr(0, first_comma);
            }
            std::error_code ec;
            auto maybe_resp = websocket::make_http_response(req, protocol, ec);
            REALM_ASSERT(maybe_resp);
            REALM_ASSERT(!ec);
            http_server.async_send_response(
                *maybe_resp,
                [self = util::bind_ptr(this), promise = std::move(pf.promise)](std::error_code ec) mutable {
                    if (ec) {
                        promise.set_error(status_from_network_error_code(ec));
                        return;
                    }

                    self->websocket.initiate_server_websocket_after_handshake();
                    promise.emplace_value();
                });
            return std::move(pf.future);
        }

        void do_server_handshake()
        {
            initiate_server_handshake()
                .then([self = util::bind_ptr(this)](HTTPRequest&& req) {
                    return self->complete_server_handshake(std::move(req));
                })
                .get_async([self = util::bind_ptr(this)](Status status) {
                    if (status.is_ok()) {
                        self->events.add_event(WebSocketEvent::HandshakeComplete);
                    }
                    else {
                        self->events.add_event(WebSocketEvent::ReadError);
                    }
                });
        }

        util::Future<WebSocketEvent> next_event()
        {
            return events.next_event();
        }

    protected:
        friend struct HTTPServer<Conn>;
        friend struct HTTPParser<Conn>;
        friend class websocket::Socket;
        friend class TestWebSocketServer;

        void shutdown_websocket()
        {
            websocket.stop();
            socket.close();
        }

        // Implement the websocket::Config interface
        const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept override
        {
            return logger;
        }

        std::mt19937_64& websocket_get_random() noexcept override
        {
            return random;
        }

        void async_write(const char* data, size_t size, WriteCompletionHandler handler) override
        {

            socket.async_write(data, size, std::move(handler));
        }

        void async_read(char* buffer, size_t size, ReadCompletionHandler handler) override
        {
            socket.async_read(buffer, size, read_buffer, std::move(handler));
        }

        void async_read_until(char* buffer, size_t size, char delim, ReadCompletionHandler handler) override
        {
            socket.async_read_until(buffer, size, delim, read_buffer, std::move(handler));
        }

        void websocket_handshake_completion_handler(const HTTPHeaders&) override
        {
            events.add_event(WebSocketEvent::HandshakeComplete);
        }

        void websocket_read_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::ReadError);
            shutdown_websocket();
        }

        void websocket_write_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::WriteError);
            shutdown_websocket();
        }

        void websocket_handshake_error_handler(std::error_code, const HTTPHeaders*, std::string_view) override
        {
            events.add_event(WebSocketEvent::HandshakeError);
            shutdown_websocket();
        }

        void websocket_protocol_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::ProtocolError);
            shutdown_websocket();
        }

        bool websocket_text_message_received(const char*, size_t) override
        {
            return !closed;
        }

        bool websocket_binary_message_received(const char* data, size_t size) override
        {
            events.add_event(WebSocketEvent::BinaryMessage, std::string(data, size));
            return !closed;
        }

        bool websocket_close_message_received(websocket::WebSocketError code, std::string_view message) override
        {
            events.add_event(WebSocketEvent::CloseFrame, std::string{message}, code);
            return !closed;
        }

        bool websocket_ping_message_received(const char*, size_t) override
        {
            return !closed;
        }

        bool websocket_pong_message_received(const char*, size_t) override
        {
            return !closed;
        }


        std::mt19937_64 random;
        const std::shared_ptr<util::Logger> logger;
        network::Service& service;

        network::ReadAheadBuffer read_buffer;
        network::Socket socket;
        HTTPServer<Conn> http_server;
        websocket::Socket websocket;

        WebSocketEventQueue events;
        bool closed = false;
    };

    util::Future<util::bind_ptr<Conn>> accept_connection()
    {
        auto pf = util::make_promise_future<util::bind_ptr<Conn>>();
        m_service.post([this, promise = std::move(pf.promise)](Status status) mutable {
            if (status == ErrorCodes::OperationAborted) {
                promise.set_error(status);
                return;
            }
            auto conn = util::make_bind<Conn>(m_service, m_test_context);
            m_acceptor.async_accept(
                conn->socket, [conn = std::move(conn), promise = std::move(promise)](std::error_code ec) mutable {
                    if (ec) {
                        promise.set_error(status_from_network_error_code(ec));
                        return;
                    }

                    promise.emplace_value(std::move(conn));
                });
        });
        return std::move(pf.future);
    }

private:
    test_util::unit_test::TestContext& m_test_context;
    const std::shared_ptr<util::Logger> m_logger;
    network::Service m_service;
    network::Acceptor m_acceptor;
    network::Endpoint m_endpoint;
    std::thread m_server_thread;
};

static auto make_fut_callback()
{
    auto pf = util::make_promise_future<void>();
    return std::make_pair(std::move(pf.future), util::UniqueFunction<void(Status)>(
                                                    [promise = std::move(pf.promise)](Status status) mutable {
                                                        if (status.is_ok()) {
                                                            promise.emplace_value();
                                                        }
                                                        else {
                                                            promise.set_error(status);
                                                        }
                                                    }));
}

TEST(DefaultWebSocket_WriteErrors)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    std::string message_to_send = "hello, world!\n";
    auto [before_connect_fut, before_connect_cb] = make_fut_callback();
    client_provider.post([&message_to_send, &client, cb = std::move(before_connect_cb)](Status status) mutable {
        REALM_ASSERT(status.is_ok());
        client->async_write_binary(message_to_send, std::move(cb));
    });
    auto status = before_connect_fut.get_no_throw();
    CHECK(status == ErrorCodes::InvalidArgument);

    auto server_conn = std::move(server_conn_fut.get());
    server_conn->do_server_handshake();

    CHECK(client_events->next_event().get().type == WebSocketEvent::HandshakeComplete);
    CHECK(server_conn->next_event().get().type == WebSocketEvent::HandshakeComplete);

    auto [after_connect_fut, after_connect_cb] = make_fut_callback();
    client_provider.post([&message_to_send, &client, cb = std::move(after_connect_cb)](Status status) mutable {
        REALM_ASSERT(status.is_ok());
        client->async_write_binary(message_to_send, std::move(cb));
    });
    status = after_connect_fut.get_no_throw();
    CHECK(status.is_ok());
    auto bin_msg_event = server_conn->next_event().get();
    CHECK(bin_msg_event.type == WebSocketEvent::BinaryMessage);
    CHECK(bin_msg_event.payload == message_to_send);

    std::string close_msg = "moved";
    server_conn->send_close_frame(WebSocketError::websocket_moved_permanently, close_msg).get();

    auto close_event = client_events->next_event().get();
    CHECK(close_event.type == WebSocketEvent::CloseFrame);
    CHECK(close_event.payload == close_msg);
    CHECK(close_event.close_code == WebSocketError::websocket_moved_permanently);

    auto [after_close_fut, after_close_cb] = make_fut_callback();
    client_provider.post([&message_to_send, &client, cb = std::move(after_close_cb)](Status status) mutable {
        REALM_ASSERT(status.is_ok());
        client->async_write_binary(message_to_send, std::move(cb));
    });
    status = after_close_fut.get_no_throw();
    CHECK(status == ErrorCodes::ConnectionClosed);

    auto server_read_error = server_conn->next_event().get();
    CHECK(server_read_error.type == WebSocketEvent::ReadError);

    do_synchronous_post(client_provider, [&] {
        client.reset();
    });
}

TEST(DefaultWebSocket_ClientClosedBeforeHandshake)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    auto server_conn = std::move(server_conn_fut.get());
    auto handshake_start_fut = server_conn->initiate_server_handshake();
    handshake_start_fut.get();

    do_synchronous_post(client_provider, [&] {
        client.reset();
    });
    auto handshake_complete_fut = util::make_promise_future();
    server.post([req = handshake_start_fut.get(), server_conn,
                 promise = std::move(handshake_complete_fut.promise)]() mutable {
        promise.set_from(server_conn->complete_server_handshake(std::move(req)));
    });

    handshake_complete_fut.future.get();
    auto read_error_event = server_conn->next_event().get();
    CHECK(read_error_event.type == WebSocketEvent::ReadError);
}
