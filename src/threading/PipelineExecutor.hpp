#pragma once

// ============================================================================
// C++20 CONCURRENCY — THE CONCEPTS YOU NEED
//
// THREAD: An independent execution path. Your CPU has multiple cores.
//   Without threads: one core works, others idle.
//   With threads: multiple cores work simultaneously.
//
// std::async: The cleanest way to run a function in a background thread.
//   auto future = std::async(std::launch::async, my_function, arg1, arg2);
//   This immediately starts my_function on a new thread.
//   Your main thread continues running other code.
//   future.get() = "wait here until the background thread finishes".
//
// std::future: A handle to a value that will exist in the future.
//   It's a promise: "I will give you a result, just not yet."
//   future.get() blocks until the result is ready.
//   If the background thread threw an exception, future.get() re-throws it.
//
// WHY NOT std::thread DIRECTLY?
//   std::thread is lower level. You must manually .join() it to avoid crashes.
//   Exception handling is manual — if the thread throws, you never see it.
//   std::async handles all of this automatically. It's the correct tool
//   for task-based parallelism (run this task, get a result back).
//
// DATA RACE: When two threads read AND write the SAME memory simultaneously.
//   This is undefined behavior in C++ — your program can produce wrong answers
//   silently or crash randomly.
//   Our design AVOIDS data races completely:
//     - Each thread has its OWN database connection (separate pqxx::connection)
//     - Each thread reads from its own COPY of the data (passed by value)
//     - No shared mutable state between threads = zero risk of data races
//
// THREAD SAFETY RULE:
//   Reading shared data from multiple threads = SAFE (no modifications)
//   Writing shared data from multiple threads = UNSAFE (use mutex or atomic)
//   Each thread writes to its OWN resources = SAFE (what we do here)
// ============================================================================

#include <future> // std::async, std::future — C++ thread management
#include <vector>
#include <iostream>
#include <stdexcept>
#include "../model/Trade.hpp"
#include "../indicators/TechnicalIndicators.hpp"
#include "../database/DatabaseLoader.hpp"
#include "../benchmark/Benchmarker.hpp"

namespace MarketStream
{

    // ============================================================================
    // PipelineExecutor — Runs DB Load and Indicator Save in PARALLEL
    // ============================================================================
    class PipelineExecutor
    {
    public:
        // ========================================================================
        // run_parallel_load()
        // ========================================================================
        // Fires TWO tasks simultaneously on separate threads:
        //   Thread A: bulk_load(trades)       → writes to 'trades' table
        //   Thread B: save_indicators(indics) → writes to 'technical_indicators'
        //
        // Main thread waits for BOTH to finish, then checks for errors.
        //
        // PARAMETERS passed by const reference:
        //   Why const ref for trades and indicators?
        //   Both threads READ these vectors — they never modify them.
        //   Passing by const ref = zero copies, zero risk of accidental mutation.
        //
        //   Why conn_str passed by VALUE to the lambdas?
        //   Each lambda (thread) captures conn_str. If we captured by reference,
        //   both threads would share the same string memory. If one thread's
        //   DatabaseLoader modified it (it doesn't, but defensively), we'd have
        //   a race condition. Capturing by value = each thread has its own copy.
        //   For a short string like a connection string, this is negligible cost.
        // ========================================================================
        static void run_parallel_load(
            const std::string &conn_str,
            const std::vector<Trade> &trades,
            const std::vector<IndicatorResult> &indicators,
            std::vector<BenchmarkResult> &bench_results)
        {
            std::cout << "[PARALLEL] Launching Load Trades + Save Indicators concurrently...\n";

            // Capture start time BEFORE launching both threads.
            // This measures the true wall-clock time of the parallel operation.
            auto wall_start = std::chrono::high_resolution_clock::now();

            // ------------------------------------------------------------------
            // THREAD A: Load trades into PostgreSQL
            // ------------------------------------------------------------------
            // std::launch::async = "start immediately on a new thread".
            // The alternative std::launch::deferred = "run lazily when .get() is called"
            // (that would defeat the purpose — we want TRUE parallelism).
            //
            // [conn_str, &trades] = capture list:
            //   conn_str by VALUE (each thread owns its string copy)
            //   trades by CONST REFERENCE (read-only, safe to share)
            // ------------------------------------------------------------------
            auto future_trades = std::async(
                std::launch::async,
                [conn_str, &trades]() -> long long // Returns duration in nanoseconds
                {
                    auto t_start = std::chrono::high_resolution_clock::now();

                    // Each thread constructs its OWN DatabaseLoader.
                    // This creates its OWN pqxx::connection internally.
                    // Two separate TCP connections to PostgreSQL = no sharing = no race.
                    DatabaseLoader loader(conn_str);
                    loader.bulk_load(trades);

                    auto t_end = std::chrono::high_resolution_clock::now();
                    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                               t_end - t_start)
                        .count();
                });

            // ------------------------------------------------------------------
            // THREAD B: Save indicators into PostgreSQL
            // ------------------------------------------------------------------
            // This line executes IMMEDIATELY after launching Thread A.
            // Thread A is already running on another CPU core while we launch B.
            // Main thread then continues to the .get() calls below.
            // ------------------------------------------------------------------
            auto future_indicators = std::async(
                std::launch::async,
                [conn_str, &indicators]() -> long long
                {
                    auto t_start = std::chrono::high_resolution_clock::now();

                    DatabaseLoader loader(conn_str);
                    loader.save_indicators(indicators);

                    auto t_end = std::chrono::high_resolution_clock::now();
                    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                               t_end - t_start)
                        .count();
                });

            // ------------------------------------------------------------------
            // SYNCHRONIZATION POINT — Wait for both threads to finish
            // ------------------------------------------------------------------
            // .get() BLOCKS the main thread until the background thread completes.
            // If the background thread threw an exception (e.g., DB connection failed),
            // .get() RE-THROWS that exception here in the main thread.
            // This is why std::async is superior to std::thread — exception propagation
            // is automatic. With raw std::thread, exceptions in background threads
            // are silently swallowed and the program calls std::terminate().
            //
            // ORDER MATTERS: We call future_trades.get() first, then future_indicators.get().
            // This does NOT mean trades finishes first — both are already running.
            // It means: "wait for trades to finish (if not done yet), get its result,
            // then wait for indicators (if not done yet), get its result."
            // Both operations overlap in real time.
            // ------------------------------------------------------------------
            long long trades_ns = future_trades.get();         // Block until trades done
            long long indicators_ns = future_indicators.get(); // Block until indicators done

            auto wall_end = std::chrono::high_resolution_clock::now();
            long long wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    wall_end - wall_start)
                                    .count();

            // Record individual thread durations for reporting
            bench_results.push_back({"  Trades (thread)", trades_ns, trades.size()});
            bench_results.push_back({"  Indics (thread)", indicators_ns, indicators.size()});

            // Wall clock = actual elapsed time with parallelism
            // This should be approximately max(trades_ns, indicators_ns), not their sum.
            bench_results.push_back({"PARALLEL DB Total", wall_ns, trades.size() + indicators.size()});

            double speedup = static_cast<double>(trades_ns + indicators_ns) / static_cast<double>(wall_ns);

            std::cout << "[PARALLEL] Both threads complete.\n";
            std::cout << "[PARALLEL] Sequential would have taken: "
                      << (trades_ns + indicators_ns) / 1'000'000 << "ms\n";
            std::cout << "[PARALLEL] Actual wall time:            "
                      << wall_ns / 1'000'000 << "ms\n";
            std::cout << "[PARALLEL] Speedup factor:              "
                      << std::fixed << std::setprecision(2) << speedup << "x\n";
        }
    };

} // namespace MarketStream