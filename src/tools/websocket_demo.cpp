// ============================================================================
// websocket_demo.cpp — Wires TickServer → WebSocket → TickClient → SPSCQueue → Consumer
// ============================================================================
//
// WHAT THIS DEMO PROVES:
// Your pipeline can now receive LIVE STREAMING data over a network protocol,
// not just batch-process a static CSV file. This is the architectural jump
// from "batch ETL" to "real-time streaming ETL" — the foundation of every
// live trading system.
//
// THREE THREADS:
//   Thread 1 (server_thread inside TickServer):
//     Generates synthetic trades → serializes to JSON → sends over WebSocket
//     Rate: ~5,000 ticks/second (one tick every 200µs)
//
//   Thread 2 (client_thread inside TickClient):
//     Receives JSON frames from WebSocket
//     Parses JSON → TickMessage → Trade
//     Pushes Trade into SPSCQueue<Trade, 4096>
//
//   Thread 3 (consumer thread in main):
//     Pops Trade from SPSCQueue
//     Validates: price > 0, volume > 0 (lightweight hot-path check)
//     Counts by symbol (in-memory stats)
//
// THREAD INTERACTION:
//
//   [Server Thread]          [Client Thread]        [Consumer Thread]
//        │                         │                       │
//   generate tick            ws.read() blocks        try_pop() spins
//   ws.write(json)  ──TCP──▶ Parse JSON              Process Trade
//   sleep(200µs)             try_push(trade)  ──Q──▶ Update stats
//        │                   (SPSCQueue)              │
//       ...                       │                  ...
//
// BACKPRESSURE:
// If consumer is slow → SPSCQueue fills up → client's try_push() yields.
// Client stays blocked until consumer catches up. No data loss.
// This is natural backpressure propagation through the pipeline.
// ============================================================================

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <string>
#include <optional>

#include "../feed/TickServer.hpp"
#include "../feed/TickClient.hpp"
#include "../threading/SPSCQueue.hpp"
#include "../model/Trade.hpp"

using namespace MarketStream;
using TradeQueue = SPSCQueue<Trade, 4096>;

// ============================================================================
// Consumer — Runs on main thread, pops from SPSCQueue
// ============================================================================
// WHY ON MAIN THREAD?
// SPSCQueue = exactly ONE producer, ONE consumer.
// Producer = client thread.
// Consumer = the thread calling try_pop(). Here that's main.
// Using any other thread as consumer would break the SPSC contract.
//
// WHAT THE CONSUMER DOES:
// For this demo: lightweight validation + per-symbol counting.
// In a real system: this is where you'd run your trading signal logic —
// technical indicators on the live stream, position management, risk checks.
// ============================================================================
struct ConsumerStats
{
    size_t total_consumed = 0;
    size_t valid          = 0;
    size_t rejected       = 0;
    std::unordered_map<std::string, size_t> per_symbol;
};

static ConsumerStats consume_loop(
    TradeQueue&            queue,
    std::atomic<bool>&     keep_running,
    std::chrono::seconds   duration)
{
    ConsumerStats stats;

    auto deadline = std::chrono::steady_clock::now() + duration;

    while (std::chrono::steady_clock::now() < deadline || !queue.empty())
    {
        // try_pop() is non-blocking: returns Trade immediately or nullopt.
        // WHY NOT BLOCK? If we blocked on an empty queue, we'd burn CPU
        // spinning. Instead: yield() gives the CPU to the client thread,
        // which might push a new item. Next iteration: we try again.
        auto item = queue.try_pop();

        if (!item)
        {
            // Queue is empty — client might be mid-receive. Yield = polite wait.
            std::this_thread::yield();
            continue;
        }

        ++stats.total_consumed;

        // Lightweight hot-path validation.
        // NOT the full TradeValidator (regex is expensive in a hot loop).
        // In production: validate on ingestion (client side), trust on consume side.
        // Here: just a sanity check.
        const Trade& t = *item;
        if (t.price > 0.0 && t.volume > 0)
        {
            ++stats.valid;
            stats.per_symbol[t.symbol]++;
        }
        else
        {
            ++stats.rejected;
        }
    }

    keep_running.store(false, std::memory_order_release);
    return stats;
}

// ============================================================================
// main()
// ============================================================================
int main()
{
    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | Phase 14: WebSocket Feed\n";
    std::cout << "===================================================\n\n";

    constexpr auto RUN_DURATION = std::chrono::seconds(5);

    // ── Shared queue: client pushes, main pops ─────────────────────────────
    // Declared here in main — both TickClient and consume_loop reference it.
    // Lifetime: lives for the duration of main. Both threads finish before
    // main returns (server.stop() + client.stop() join their threads first).
    TradeQueue queue;

    // ── Start server ───────────────────────────────────────────────────────
    // start() returns immediately. Server thread starts binding in background.
    // .get() blocks until acceptor is bound and listening.
    // After .get(): safe to start the client.
    TickServer server(9002);
    std::cout << "[MAIN] Starting server...\n";
    server.start().get();   // .get() = wait for "server is ready" signal
    std::cout << "[MAIN] Server ready.\n\n";

    // ── Start client ───────────────────────────────────────────────────────
    // Client connects, does WebSocket handshake, starts receive loop.
    // start() is fire-and-forget — client runs on its own thread.
    TickClient client(queue, "localhost", "9002");
    client.start();

    // Small sleep to let client print its "Connected" message before
    // consumer output. Not required for correctness — just readability.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // ── Run consumer on main thread ────────────────────────────────────────
    std::cout << "[MAIN] Running for " << RUN_DURATION.count() << " seconds...\n\n";

    std::atomic<bool> keep_running{true};
    auto start_time = std::chrono::high_resolution_clock::now();

    ConsumerStats stats = consume_loop(queue, keep_running, RUN_DURATION);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_s = std::chrono::duration<double>(end_time - start_time).count();

    // ── Stop server and client ─────────────────────────────────────────────
    // stop() sets running_=false and joins the thread.
    // Order matters: stop server first → server sends CLOSE frame →
    // client receives it → client exits cleanly → client.stop() joins immediately.
    server.stop();
    client.stop();

    // ── Print results ──────────────────────────────────────────────────────
    double throughput = static_cast<double>(stats.total_consumed) / elapsed_s;

    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════╗\n";
    std::cout << "║     Phase 14 — WebSocket Feed Performance Report     ║\n";
    std::cout << "╠══════════════════════════════════════════════════════╣\n";
    std::cout << "║  Duration              : " << std::fixed << std::setprecision(2)
              << std::setw(8) << elapsed_s << " seconds                ║\n";
    std::cout << "║  Ticks sent (server)   : " << std::setw(8) << server.ticks_sent()
              << "                        ║\n";
    std::cout << "║  Ticks received (client): " << std::setw(7) << client.ticks_received()
              << "                        ║\n";
    std::cout << "║  Ticks consumed (queue) : " << std::setw(7) << stats.total_consumed
              << "                        ║\n";
    std::cout << "║  Valid trades          : " << std::setw(8) << stats.valid
              << "                        ║\n";
    std::cout << "║  Rejected              : " << std::setw(8) << stats.rejected
              << "                        ║\n";
    std::cout << "║  Parse errors          : " << std::setw(8) << client.parse_errors()
              << "                        ║\n";
    std::cout << "║  Consumer throughput   : " << std::setw(8)
              << static_cast<size_t>(throughput) << " trades/sec             ║\n";
    std::cout << "╠══════════════════════════════════════════════════════╣\n";
    std::cout << "║  Per-symbol breakdown:                               ║\n";

    for (const auto& [sym, count] : stats.per_symbol)
    {
        std::cout << "║    " << std::left << std::setw(12) << sym
                  << " : " << std::right << std::setw(6) << count << " trades"
                  << "                       ║\n";
    }

    std::cout << "╚══════════════════════════════════════════════════════╝\n\n";

    std::cout << "[SUCCESS] Phase 14 complete. Real-time WebSocket feed operational.\n";
    std::cout << "===================================================\n";

    return 0;
}