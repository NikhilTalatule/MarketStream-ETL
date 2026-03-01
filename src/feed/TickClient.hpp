#pragma once

// ============================================================================
// TickClient.hpp — Boost.Beast WebSocket client → SPSCQueue bridge
// ============================================================================
//
// ROLE IN THE PIPELINE:
//   TickServer → [WebSocket] → TickClient → [SPSCQueue<Trade,4096>] → Consumer
//
// The client does THREE things:
//   1. Connect to the WebSocket server (TCP + HTTP Upgrade handshake)
//   2. Read incoming JSON text frames in a loop
//   3. Parse JSON → TickMessage → Trade → push to SPSCQueue
//
// WHY IS THE CLIENT THE SPSC PRODUCER?
// SPSCQueue = Single Producer, Single Consumer.
//   Producer = TickClient (receives from network, pushes to queue)
//   Consumer = Consumer thread (pops from queue, processes Trade)
// This is the classic "fan-in" pattern: network I/O decoupled from CPU work.
// The client doesn't care how long processing takes.
// The consumer doesn't care about network timing.
// The SPSCQueue absorbs the timing mismatch between them.
//
// WHY A SEPARATE THREAD FOR THE CLIENT?
// ws.read() is BLOCKING — it waits until a message arrives.
// If we ran it on the main thread, main would be stuck waiting.
// On its own thread: client blocks on ws.read(), main does other work.
// When a message arrives: client thread wakes up, parses, pushes to queue.
// ============================================================================

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <thread>
#include <atomic>
#include <iostream>

#include "../feed/TickMessage.hpp"
#include "../threading/SPSCQueue.hpp"
#include "../model/Trade.hpp"

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace MarketStream
{

    class TickClient
    {
    public:
        // SPSCQueue with 4096 slots of Trade objects.
        // 4096 is power-of-2 (required) and fits in L2 cache:
        //   4096 × sizeof(Trade) ≈ 4096 × 88 bytes ≈ 352 KB
        // This is a deliberate choice: large enough to buffer bursts,
        // small enough that head/tail index operations stay cache-warm.
        using TradeQueue = SPSCQueue<Trade, 4096>;

        // ========================================================================
        // Constructor
        // ========================================================================
        // queue — reference to the SPSCQueue that connects us to the consumer.
        //         WHY REFERENCE AND NOT POINTER?
        //         Reference = guaranteed non-null. The queue MUST exist.
        //         Pointer = nullable, requires null-check everywhere. Unnecessary here.
        //
        // host, port — where the TickServer is listening.
        //   Passed as strings because tcp::resolver::resolve() takes strings.
        //   "localhost" resolves to 127.0.0.1 (loopback) — no actual network hop.
        // ========================================================================
        explicit TickClient(TradeQueue &queue,
                            std::string host = "localhost",
                            std::string port = "9002")
            : queue_(queue),
              host_(std::move(host)),
              port_(std::move(port)),
              running_(false),
              ticks_received_(0),
              parse_errors_(0)
        {
        }

        ~TickClient() { stop(); }

        TickClient(const TickClient &) = delete;
        TickClient &operator=(const TickClient &) = delete;

        // ========================================================================
        // start() — Launch client on a background thread
        // ========================================================================
        void start()
        {
            running_.store(true, std::memory_order_release);
            client_thread_ = std::thread([this]()
                                         { run(); });
        }

        // ========================================================================
        // stop() — Signal shutdown and wait for thread to exit
        // ========================================================================
        // running_ = false → the ws.read() loop exits after current read completes.
        // join() waits for clean exit.
        //
        // KNOWN LIMITATION: if ws.read() is blocking waiting for a message,
        // setting running_=false won't interrupt it immediately — the thread wakes
        // only when the next message arrives OR the server closes the connection.
        // In production: use async WebSocket + cancellation token. For this demo:
        // the server sends ticks every 200µs, so the client exits within 200µs of stop().
        // ========================================================================
        void stop()
        {
            running_.store(false, std::memory_order_release);
            if (client_thread_.joinable())
                client_thread_.join();
        }

        size_t ticks_received() const { return ticks_received_.load(std::memory_order_relaxed); }
        size_t parse_errors() const { return parse_errors_.load(std::memory_order_relaxed); }

    private:
        // ========================================================================
        // run() — Client logic on client_thread_
        // ========================================================================
        void run()
        {
            try
            {
                // ── STEP 1: Resolve and Connect ───────────────────────────────
                net::io_context ioc;

                // tcp::resolver converts "localhost" → IP address.
                // On Windows: calls getaddrinfo() internally.
                // "9002" → port number 9002.
                // This is the same as getaddrinfo() + connect() in C socket programming.
                tcp::resolver resolver(ioc);
                auto results = resolver.resolve(host_, port_);

                // Create WebSocket stream. next_layer() = the underlying TCP socket.
                // We connect the TCP socket first, then do the WebSocket handshake.
                websocket::stream<tcp::socket> ws(ioc);

                // net::connect() tries each resolved address until one succeeds.
                // For "localhost": usually only one address (127.0.0.1).
                // Blocks until TCP connection is established (3-way handshake done).
                net::connect(ws.next_layer(), results);

                // ── STEP 2: WebSocket Handshake ───────────────────────────────
                // ws.handshake(host, target):
                //   Sends HTTP GET request with "Upgrade: websocket" header.
                //   Server responds with "101 Switching Protocols".
                //   After this: the TCP connection now carries WebSocket frames.
                //
                // target = "/" (the URL path). Most tick servers use "/" or "/feed".
                ws.handshake(host_, "/");
                std::cout << "[CLIENT] Connected to ws://" << host_ << ":" << port_ << "\n";

                // ── STEP 3: Receive loop ──────────────────────────────────────
                // beast::flat_buffer = Boost.Beast's dynamic byte buffer.
                // ws.read() fills this buffer with the next complete WebSocket message.
                // We reuse the SAME buffer every iteration:
                //   read → consume(size) → read → consume(size) → ...
                // consume() marks bytes as processed (advances the internal pointer).
                // Reusing = zero allocations in the hot loop.
                beast::flat_buffer buffer;

                while (running_.load(std::memory_order_acquire))
                {
                    boost::system::error_code ec;

                    // ws.read() BLOCKS until:
                    //   (a) a complete WebSocket message arrives (normal case)
                    //   (b) server sends CLOSE frame (ec = websocket::error::closed)
                    //   (c) TCP error (network failure, server crash, etc.)
                    ws.read(buffer, ec);

                    // Server sent CLOSE frame — graceful shutdown.
                    if (ec == websocket::error::closed)
                    {
                        std::cout << "[CLIENT] Server closed connection gracefully.\n";
                        break;
                    }

                    // Network error — log and exit.
                    if (ec)
                    {
                        if (running_.load()) // Only log if we didn't initiate shutdown
                            std::cerr << "[CLIENT ERROR] Read failed: " << ec.message() << "\n";
                        break;
                    }

                    // ── STEP 4: Parse JSON → Trade → push to SPSCQueue ────────
                    try
                    {
                        // beast::buffers_to_string() copies the buffer content to a string.
                        // WHY COPY? nlohmann::json::parse() needs a std::string or char*.
                        // In production with FlatBuffers: zero-copy read directly from buffer.
                        auto text = beast::buffers_to_string(buffer.data());

                        // CRITICAL: consume() MUST be called after every read.
                        // Without it: next ws.read() appends to existing data → corrupted messages.
                        buffer.consume(buffer.size());

                        // Parse JSON text → TickMessage struct
                        auto msg = TickMessage::from_json(text);

                        // Convert TickMessage → Trade (our internal domain model)
                        Trade trade = msg.to_trade();

                        // Push to SPSCQueue. If full: yield and retry.
                        // WHY YIELD AND NOT SPIN?
                        // Queue full = consumer is slower than producer (backpressure).
                        // yield() = "I'm waiting, let the consumer run."
                        // The consumer pops, queue has space, we push on next attempt.
                        while (!queue_.try_push(std::move(trade)) &&
                               running_.load(std::memory_order_relaxed))
                        {
                            std::this_thread::yield();
                        }

                        ticks_received_.fetch_add(1, std::memory_order_relaxed);
                    }
                    catch (const std::exception &e)
                    {
                        // JSON parse failure — log, don't crash.
                        // In production: send to dead-letter queue for human review.
                        parse_errors_.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "[CLIENT] Parse error: " << e.what() << "\n";
                        buffer.consume(buffer.size()); // Clear buffer regardless
                    }
                }

                // ── STEP 5: Close gracefully ──────────────────────────────────
                boost::system::error_code ec;
                ws.close(websocket::close_code::normal, ec);
            }
            catch (const std::exception &e)
            {
                std::cerr << "[CLIENT ERROR] " << e.what() << "\n";
            }

            running_.store(false, std::memory_order_release);
            std::cout << "[CLIENT] Received " << ticks_received_ << " ticks, "
                      << parse_errors_ << " parse errors.\n";
        }

        TradeQueue &queue_;
        std::string host_;
        std::string port_;
        std::atomic<bool> running_;
        std::atomic<size_t> ticks_received_;
        std::atomic<size_t> parse_errors_;
        std::thread client_thread_;
    };

} // namespace MarketStream