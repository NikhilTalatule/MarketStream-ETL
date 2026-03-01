#pragma once

// ============================================================================
// TickServer.hpp — Boost.Beast synchronous WebSocket server
// ============================================================================
//
// WHAT IS A WEBSOCKET?
// HTTP is request-response: client asks, server answers, connection closes.
// WebSocket is PERSISTENT: client connects ONCE, then both sides can send
// messages to each other at any time — like a phone call, not letters.
//
// This is exactly what live market data feeds use:
//   Exchange → WebSocket → Your system
//   One connection. Messages arrive continuously. No polling.
//
// WHY SYNCHRONOUS (not async)?
// Boost.Beast supports two styles:
//   Sync:  ws.write(data)   — blocks until sent, simple, one thread
//   Async: ws.async_write() — non-blocking, callbacks, requires io_context loop
//
// Sync is correct here because the server has ONE job: send ticks.
// It runs on its own dedicated thread. No other work competes for that thread.
// Async would add io_context, strand, coroutine complexity with zero benefit
// for a single-purpose server thread.
//
// ARCHITECTURE OF THIS SERVER:
//   main thread: start() → launches server_thread_ → returns future
//   server_thread_: bind port → accept ONE connection → send tick loop
//   Each tick: generate Trade → serialize to JSON → ws.write()
//
// WHY std::promise<void> FOR READINESS?
// The server needs to be listening BEFORE the client tries to connect.
// std::promise<void> + std::future<void> = a one-shot signal:
//   Server sets promise (ready) → future.get() in main thread unblocks.
// Alternative: sleep(100ms). That's fragile — on a slow machine, 100ms
// might not be enough. promise/future is exact, not time-based.
// ============================================================================

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <thread>
#include <atomic>
#include <future>
#include <random>
#include <chrono>
#include <iostream>
#include <unordered_map>

#include "../feed/TickMessage.hpp"

// Namespace aliases — Boost's names are long. These shorten them.
// 'namespace X = Y' means: in this file, X and Y are interchangeable.
// NO performance cost — pure compile-time aliasing.
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace MarketStream
{

    class TickServer
    {
    public:
        // ========================================================================
        // Constructor
        // ========================================================================
        // port = 9002 (default). Why 9002?
        // Port 80 = HTTP (requires root on Linux). Port 443 = HTTPS (same).
        // Ports 1024–65535 are "user ports" — no root needed.
        // 9002 is a common convention for local WebSocket servers.
        // ========================================================================
        explicit TickServer(uint16_t port = 9002)
            : port_(port), running_(false), ticks_sent_(0)
        {
        }

        ~TickServer() { stop(); }

        // Delete copy/move — server owns a thread, can't be copied.
        TickServer(const TickServer &) = delete;
        TickServer &operator=(const TickServer &) = delete;

        // ========================================================================
        // start() — Launches server on a background thread
        // ========================================================================
        // Returns std::future<void>.
        // Caller does: server.start().get() — blocks until server is ready.
        //
        // WHY shared_ptr<promise>?
        // The promise lives in this function's stack frame.
        // The lambda captures it to use it INSIDE the server thread.
        // If we captured by reference, the stack frame would be gone before
        // the thread uses it — dangling reference = crash.
        // shared_ptr ensures the promise lives until the thread is done with it.
        // ========================================================================
        std::future<void> start()
        {
            auto promise = std::make_shared<std::promise<void>>();
            auto future = promise->get_future();

            running_.store(true, std::memory_order_release);

            server_thread_ = std::thread(
                [this, promise]()
                {
                    run(promise);
                });

            return future; // Caller .get()s this to wait for readiness
        }

        // ========================================================================
        // stop() — Graceful shutdown
        // ========================================================================
        // Sets running_ = false. The send loop in run() checks running_ every tick.
        // On the next loop iteration, the loop exits and ws.close() is called.
        // join() waits for the thread to exit cleanly.
        //
        // WHY memory_order_release?
        // We're writing a flag that another thread (the server thread) reads.
        // memory_order_release = "all our writes are visible to any thread that
        // subsequently does an acquire-load of this variable."
        // This ensures the server thread sees running_=false, not stale=true.
        // ========================================================================
        void stop()
        {
            running_.store(false, std::memory_order_release);
            if (server_thread_.joinable())
                server_thread_.join();
        }

        size_t ticks_sent() const
        {
            return ticks_sent_.load(std::memory_order_relaxed);
        }

    private:
        // ========================================================================
        // run() — The actual server logic, runs on server_thread_
        // ========================================================================
        void run(std::shared_ptr<std::promise<void>> ready_promise)
        {
            try
            {
                // ── STEP 1: Bind and listen ────────────────────────────────────
                // io_context = Boost.Asio's event loop engine.
                // For sync I/O, we create one but never call ioc.run() —
                // sync operations drive themselves, no event loop needed.
                net::io_context ioc;

                // tcp::acceptor opens the server socket.
                // tcp::endpoint(tcp::v4(), port_) = "listen on all IPv4 interfaces, port 9002"
                // This is equivalent to bind()+listen() in raw socket programming.
                tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), port_));

                // Signal readiness BEFORE accept() — server is now listening.
                // accept() would block (waiting for a client). If we signaled
                // AFTER accept(), the client would try to connect before we're listening.
                ready_promise->set_value();
                std::cout << "[SERVER] Listening on ws://localhost:" << port_ << "\n";

                // ── STEP 2: Accept one client ─────────────────────────────────
                // accept() BLOCKS here until a client connects.
                // This is a sync call — the thread sleeps (0% CPU) until connection.
                tcp::socket socket(ioc);
                acceptor.accept(socket);

                // Wrap the raw TCP socket in a WebSocket stream.
                // beast::websocket::stream<tcp::socket> adds the WebSocket protocol
                // on top of the TCP socket. The underlying TCP socket moves INTO
                // the WebSocket stream — we no longer use 'socket' directly.
                // std::move = transfer ownership, no copy.
                websocket::stream<tcp::socket> ws(std::move(socket));

                // Complete the WebSocket handshake.
                // The client sent an HTTP Upgrade request. ws.accept() reads it,
                // validates it, and sends the HTTP 101 Switching Protocols response.
                // After this, both sides speak WebSocket protocol, not HTTP.
                ws.accept();
                std::cout << "[SERVER] Client connected. Streaming ticks at ~5K/sec...\n";

                // ── STEP 3: Synthetic tick generator ─────────────────────────
                // Same random walk as DataGenerator — prices drift realistically.
                std::mt19937_64 rng(42);
                std::normal_distribution<double> price_delta(0.0, 0.5);
                std::uniform_int_distribution<int> vol_dist(10, 5000);
                std::uniform_int_distribution<int> side_dist(0, 1);
                std::uniform_int_distribution<int> type_dist(0, 9);
                std::uniform_int_distribution<int> sym_dist(0, 4);

                std::vector<std::string> symbols{
                    "RELIANCE", "TCS", "INFY", "HDFC", "WIPRO"};

                std::unordered_map<std::string, double> prices{
                    {"RELIANCE", 2456.75}, {"TCS", 3567.50}, {"INFY", 1423.25}, {"HDFC", 1678.90}, {"WIPRO", 432.60}};

                long long timestamp = 1698208500000000000LL;
                uint64_t trade_id = 5'000'000ULL;

                // ── STEP 4: Send loop ─────────────────────────────────────────
                // running_ is checked every iteration.
                // stop() sets running_=false → loop exits after current tick.
                while (running_.load(std::memory_order_acquire))
                {
                    const auto &sym = symbols[sym_dist(rng)];
                    auto &price = prices[sym];
                    price += price_delta(rng);
                    if (price < 50.0)
                        price = 50.0;

                    TickMessage msg;
                    msg.trade_id = trade_id++;
                    msg.order_id = trade_id + 1'000'000ULL;
                    msg.timestamp = (timestamp += 10'000LL); // 10µs gap per tick
                    msg.symbol = sym;
                    msg.price = price;
                    msg.volume = static_cast<uint32_t>(vol_dist(rng));
                    msg.side = (side_dist(rng) == 0) ? 'B' : 'S';
                    int tr = type_dist(rng);
                    msg.type = (tr < 3) ? 'M' : (tr < 9) ? 'L'
                                                         : 'I';
                    msg.is_pro = false;

                    // ws.write() sends one WebSocket TEXT frame.
                    // net::buffer() wraps the string as a byte buffer (no copy).
                    // This blocks until the OS confirms the data is in the TCP send buffer.
                    // On error (client disconnected): ec is set, we break.
                    boost::system::error_code ec;
                    ws.write(net::buffer(msg.to_json()), ec);
                    if (ec)
                        break;

                    ticks_sent_.fetch_add(1, std::memory_order_relaxed);

                    // 200 microseconds per tick = ~5,000 ticks/second.
                    // sleep_for yields the CPU — 0% usage while sleeping.
                    // A real exchange feed would not sleep — it sends as fast as data arrives.
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                }

                // ── STEP 5: Graceful close ─────────────────────────────────────
                // ws.close() sends the WebSocket CLOSE frame.
                // Client receives it, sends CLOSE back, TCP connection tears down cleanly.
                // Without this: client gets a TCP RST — abrupt disconnect.
                boost::system::error_code ec;
                ws.close(websocket::close_code::normal, ec);
                std::cout << "[SERVER] Feed stopped. Sent " << ticks_sent_ << " ticks.\n";
            }
            catch (const std::exception &e)
            {
                std::cerr << "[SERVER ERROR] " << e.what() << "\n";
                // If the promise was never set (exception before set_value),
                // set_exception so future.get() throws instead of blocking forever.
                try
                {
                    ready_promise->set_exception(std::current_exception());
                }
                catch (...)
                {
                } // set_value already called — ignore double-set
            }
        }

        uint16_t port_;
        std::atomic<bool> running_;
        std::atomic<size_t> ticks_sent_;
        std::thread server_thread_;
    };

} // namespace MarketStream