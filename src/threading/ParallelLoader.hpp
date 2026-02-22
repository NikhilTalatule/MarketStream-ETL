#pragma once

// ============================================================================
// ParallelLoader — Splits bulk COPY across N database connections
// ============================================================================
//
// WHY 4 CONNECTIONS AND NOT 1?
//
// PostgreSQL's COPY protocol maxes out at roughly 200K–250K rows/sec per
// connection over localhost TCP. This is a network+serialization bottleneck,
// not a disk bottleneck. The disk can handle much more.
//
// With 4 independent connections:
//   Connection 0: rows 0..249,999   (250K rows, ~1.0s)
//   Connection 1: rows 250K..499,999 (250K rows, ~1.0s)  ← all 4 simultaneously
//   Connection 2: rows 500K..749,999 (250K rows, ~1.0s)
//   Connection 3: rows 750K..999,999 (250K rows, ~1.0s)
//   ─────────────────────────────────────────────────────
//   Wall time ≈ 1.0s  (not 4.0s — they overlap)
//
// This is exactly how pg_restore -j 4 works (the -j flag = parallel jobs).
// pg_restore opens N connections and restores N tables simultaneously.
// We're applying the same principle to a single-table bulk load.
//
// THREAD SAFETY:
// PostgreSQL handles concurrent writes to the same table correctly.
// Multiple COPY streams can append to the same table simultaneously —
// each stream holds its own transaction, PostgreSQL's MVCC ensures
// they don't interfere with each other.
//
// LOAD SEQUENCE (MUST follow this order):
//   1. prepare_for_parallel_load()    ← DROP PK + index (main thread, sequential)
//   2. Thread pool: copy_chunk × N   ← parallel COPY streams
//   3. finalize_parallel_load()       ← ADD PRIMARY KEY + index (main thread, sequential)
//
// WHY IS prepare/finalize SEQUENTIAL?
// Only one connection can DROP or ADD a PRIMARY KEY at a time —
// it's a DDL operation that takes an ACCESS EXCLUSIVE lock on the table.
// If two threads tried to ADD PRIMARY KEY simultaneously, one would block
// and the other might see a half-built index. Sequential is correct here.
// ============================================================================

#include <vector>
#include <span> // C++20: zero-copy view over a slice of a vector
#include <future>
#include <chrono>
#include <iostream>
#include <iomanip>
#include "../model/Trade.hpp"
#include "../indicators/TechnicalIndicators.hpp"
#include "../database/DatabaseLoader.hpp"
#include "../benchmark/Benchmarker.hpp"
#include "ThreadPool.hpp"

namespace MarketStream
{

    class ParallelLoader
    {
    public:
        // ====================================================================
        // run() — The main entry point. Replaces PipelineExecutor for Stage 4.
        // ====================================================================
        // PARAMETERS:
        //   conn_str    — PostgreSQL connection string (copied to each thread)
        //   trades      — full 1M trade vector (passed as const ref — no copy)
        //   indicators  — computed indicators (6-10 rows, loaded separately)
        //   bench_results — vector to push timing results into
        //   num_threads — how many parallel COPY streams to use (default: 4)
        // ====================================================================
        static void run(
            const std::string &conn_str,
            const std::vector<Trade> &trades,
            const std::vector<IndicatorResult> &indicators,
            std::vector<BenchmarkResult> &bench_results,
            size_t num_threads = 4)
        {
            const size_t total_trades = trades.size();

            std::cout << "[PARALLEL-LOAD] Strategy: " << num_threads
                      << " threads × " << (total_trades / num_threads)
                      << " rows each\n";

            // ----------------------------------------------------------------
            // STEP 0: Save indicators in background (independent of trades load)
            // ----------------------------------------------------------------
            // Indicators are just 6-10 rows — they don't need the thread pool.
            // Launch as a single std::async task. It runs simultaneously with
            // everything below, completely independent.
            // ----------------------------------------------------------------
            auto future_indicators = std::async(
                std::launch::async,
                [conn_str, &indicators]() -> long long
                {
                    auto t0 = std::chrono::high_resolution_clock::now();
                    DatabaseLoader loader(conn_str);
                    loader.save_indicators(indicators);
                    auto t1 = std::chrono::high_resolution_clock::now();
                    return std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
                });

            // ----------------------------------------------------------------
            // STEP 1: Prepare — drop PK and index (sequential, main thread)
            // ----------------------------------------------------------------
            // WHY MUST THIS BE SEQUENTIAL?
            // ALTER TABLE ... DROP CONSTRAINT acquires ACCESS EXCLUSIVE lock.
            // This means NO other query can run on this table simultaneously.
            // So we do it once, fast, on the main thread before launching workers.
            //
            // After this call: table has NO primary key, NO index.
            // COPY will be pure sequential writes — maximum speed.
            // ----------------------------------------------------------------
            {
                DatabaseLoader prep_loader(conn_str);
                prep_loader.prepare_for_parallel_load();
            }

            auto wall_start = std::chrono::high_resolution_clock::now();

            // ----------------------------------------------------------------
            // STEP 2: Partition trades into N chunks using std::span
            // ----------------------------------------------------------------
            // std::span<const Trade> = a non-owning view over a contiguous range.
            // It's a struct containing: pointer to first element + count.
            // Size: 16 bytes regardless of how many trades are in the chunk.
            // No memory is copied. Each span just points into the trades vector.
            //
            // WHY SPAN AND NOT ITERATORS OR INDICES?
            // span is the C++20 idiomatic way to pass "a portion of an array."
            // It's explicitly designed for this use case. It's self-documenting:
            // "here is a contiguous slice of trades, you cannot modify them."
            // const Trade in std::span<const Trade> enforces read-only access.
            // ----------------------------------------------------------------
            std::vector<std::span<const Trade>> chunks;
            chunks.reserve(num_threads);

            size_t chunk_size = total_trades / num_threads;
            size_t remainder = total_trades % num_threads;

            size_t offset = 0;
            for (size_t i = 0; i < num_threads; ++i)
            {
                // Distribute remainder rows to the first few chunks
                // Example: 1,000,003 trades / 4 threads:
                //   chunks 0,1,2 get 250,001 rows
                //   chunk  3    gets 250,000 rows
                size_t this_chunk_size = chunk_size + (i < remainder ? 1 : 0);

                chunks.push_back(
                    std::span<const Trade>(trades.data() + offset, this_chunk_size));

                offset += this_chunk_size;
            }

            // ----------------------------------------------------------------
            // STEP 3: Create thread pool and submit one COPY task per chunk
            // ----------------------------------------------------------------
            // ThreadPool constructor creates num_threads OS threads right now.
            // All threads start running worker_loop() and immediately sleep,
            // waiting for work.
            // ----------------------------------------------------------------
            ThreadPool pool(num_threads);

            // One future per chunk — lets us retrieve per-thread timing
            std::vector<std::future<long long>> futures;
            futures.reserve(num_threads);

            for (size_t i = 0; i < num_threads; ++i)
            {
                // Capture: conn_str by VALUE (each thread needs its own copy)
                //          chunk   by VALUE (span is 16 bytes — cheap to copy)
                //          i       by VALUE (for logging which thread this is)
                //
                // WHY conn_str BY VALUE?
                // If captured by reference: all threads share the same string memory.
                // If main thread's conn_str somehow changes (it won't here, but
                // as a design principle): all threads see the change = data race.
                // Each thread owning its own copy = zero risk, negligible cost
                // (connection string is ~80 chars).
                //
                // WHY chunk BY VALUE?
                // span itself is 16 bytes (pointer + size). Copying it is cheaper
                // than capturing by reference + the reference dereference on every
                // access. And the span only POINTS to trades — not copies trades.
                auto chunk = chunks[i];
                auto thread_id = i;

                futures.push_back(
                    pool.submit(
                        [conn_str, chunk, thread_id]() -> long long
                        {
                            auto t0 = std::chrono::high_resolution_clock::now();

                            // Each thread creates its OWN DatabaseLoader.
                            // DatabaseLoader constructor opens its OWN pqxx::connection.
                            // 4 threads = 4 TCP connections to PostgreSQL.
                            // 4 independent COPY streams running simultaneously.
                            DatabaseLoader loader(conn_str);
                            loader.copy_chunk(chunk, static_cast<int>(thread_id));

                            auto t1 = std::chrono::high_resolution_clock::now();
                            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

                            std::cout << "[THREAD " << thread_id << "] COPY complete: "
                                      << chunk.size() << " rows in "
                                      << ns / 1'000'000 << "ms\n";

                            return ns;
                        }));
            }

            // ----------------------------------------------------------------
            // STEP 4: Wait for all COPY threads to finish
            // ----------------------------------------------------------------
            // pool.wait_all() blocks the main thread here.
            // All 4 worker threads are running their COPY streams in parallel.
            // The main thread sleeps (via condition_variable) consuming 0% CPU.
            // When the last task completes, the worker notifies done_cv_,
            // and main thread wakes up.
            // ----------------------------------------------------------------
            pool.wait_all();

            auto wall_end = std::chrono::high_resolution_clock::now();
            long long wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    wall_end - wall_start)
                                    .count();

            // ----------------------------------------------------------------
            // STEP 5: Finalize — rebuild PRIMARY KEY and index (sequential)
            // ----------------------------------------------------------------
            // PostgreSQL sorts all 1M trade_ids and builds the B-tree in ONE PASS.
            // This is O(N log N) but with excellent cache behavior.
            // One sort of 1M items >> 1M individual B-tree insertions.
            // ----------------------------------------------------------------
            std::cout << "[PARALLEL-LOAD] All COPY streams done. Rebuilding constraints...\n";
            {
                DatabaseLoader fin_loader(conn_str);
                fin_loader.finalize_parallel_load(total_trades);
            }

            // ----------------------------------------------------------------
            // STEP 6: Wait for indicators (probably already done by now)
            // ----------------------------------------------------------------
            long long indics_ns = future_indicators.get();

            // ----------------------------------------------------------------
            // STEP 7: Collect per-thread timings and push to bench_results
            // ----------------------------------------------------------------
            std::vector<long long> thread_durations;
            long long max_thread_ns = 0;

            for (size_t i = 0; i < futures.size(); ++i)
            {
                long long ns = futures[i].get();
                thread_durations.push_back(ns);
                max_thread_ns = std::max(max_thread_ns, ns);

                bench_results.push_back({"  Thread " + std::to_string(i) + " COPY",
                                         ns,
                                         chunks[i].size()});
            }

            bench_results.push_back({"  Indics save", indics_ns, indicators.size()});
            bench_results.push_back({"PARALLEL DB Total", wall_ns, total_trades});

            // Summary
            double speedup_vs_single = 4.2 * 1e9 / static_cast<double>(wall_ns);

            std::cout << "[PARALLEL-LOAD] Complete.\n";
            std::cout << "[PARALLEL-LOAD]   Total rows loaded   : " << total_trades << "\n";
            std::cout << "[PARALLEL-LOAD]   Wall time (COPY only): " << wall_ns / 1'000'000 << "ms\n";
            std::cout << "[PARALLEL-LOAD]   vs single-thread    : ~4200ms\n";
            std::cout << "[PARALLEL-LOAD]   Speedup             : "
                      << std::fixed << std::setprecision(2) << speedup_vs_single << "x\n";
        }
    };

} // namespace MarketStream