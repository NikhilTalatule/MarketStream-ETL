#pragma once

// ============================================================================
// WHAT IS BENCHMARKING AND WHY DOES IT MATTER?
//
// Benchmarking = measuring exactly how long code takes to run.
//
// In HFT, performance claims must be backed by numbers.
// "Our parser is fast" means nothing.
// "Our parser processes 2.1 million trades/second at 476ns per trade"
// is something you put on a resume and defend in a system design interview.
//
// This benchmarker uses std::chrono — C++'s high-resolution clock.
// On modern hardware, std::chrono::high_resolution_clock has nanosecond
// precision. One nanosecond = one billionth of a second.
//
// WHY NOT USE clock() OR time()?
// clock() measures CPU ticks — not wall clock time.
// time() only has second-level precision — useless for microsecond work.
// std::chrono::high_resolution_clock measures actual elapsed real time
// with nanosecond granularity. This is the correct tool.
// ============================================================================

#include <chrono>   // std::chrono — the C++ time library
#include <string>   // std::string for labels
#include <iostream> // std::cout for reporting
#include <vector>   // storing multiple results
#include <iomanip>  // std::setw, std::fixed, std::setprecision for formatting

namespace MarketStream
{

    // ============================================================================
    // BenchmarkResult — Stores the result of one timed measurement
    // ============================================================================
    struct BenchmarkResult
    {
        std::string label;     // What we measured ("Parse", "Validate", etc.)
        long long duration_ns; // How long it took in nanoseconds
        size_t item_count;     // How many items were processed

        // Computed properties — calculated on demand, not stored
        // WHY NOT STORE THESE?
        // They are derived from duration_ns and item_count.
        // Storing derived data creates risk of inconsistency.
        // Compute them fresh every time they're needed.

        double duration_ms() const
        {
            // Convert nanoseconds → milliseconds
            // 1 millisecond = 1,000,000 nanoseconds
            return static_cast<double>(duration_ns) / 1'000'000.0;
        }

        double ns_per_item() const
        {
            if (item_count == 0)
                return 0.0;
            return static_cast<double>(duration_ns) / static_cast<double>(item_count);
        }

        double items_per_second() const
        {
            if (duration_ns == 0)
                return 0.0;
            // items_per_second = item_count / duration_in_seconds
            // duration_in_seconds = duration_ns / 1,000,000,000
            // So: items_per_second = item_count * 1,000,000,000 / duration_ns
            return static_cast<double>(item_count) * 1'000'000'000.0 / static_cast<double>(duration_ns);
        }
    };

    // ============================================================================
    // Benchmarker — A RAII-based scoped timer
    // ============================================================================
    // HOW TO USE IT:
    //
    //   std::vector<BenchmarkResult> results;
    //
    //   {   // Open a scope
    //       Benchmarker bm("Parse Stage", 10, results);
    //       // ... code you want to time ...
    //   }   // Scope closes → Benchmarker destructor fires → time is recorded
    //
    // WHY RAII FOR TIMING?
    // The constructor records the START time.
    // The destructor records the END time and calculates duration.
    // The scope { } guarantees the destructor runs at the right moment —
    // even if an exception is thrown. You cannot forget to stop the timer.
    // This is the same principle as std::lock_guard for mutexes.
    // ============================================================================
    class Benchmarker
    {
    public:
        // Type aliases — these long type names are standard in chrono
        // 'using' creates a shorter name for them in this class scope
        using Clock = std::chrono::high_resolution_clock;
        using TimePoint = std::chrono::time_point<Clock>;

        // Constructor: records the start time
        // Parameters:
        //   label      — name of what we're measuring
        //   item_count — how many items will be processed (for throughput calc)
        //   results    — vector to push our result into when done
        //                Passed by reference so we can modify the caller's vector.
        Benchmarker(std::string label, size_t item_count,
                    std::vector<BenchmarkResult> &results)
            : label_(std::move(label)), item_count_(item_count), results_(results), start_(Clock::now()) // Record start time at construction
        {
        }

        // Destructor: fires when the scope closes
        // Calculates elapsed time and pushes result into the results vector.
        ~Benchmarker()
        {
            auto end = Clock::now();

            // duration_cast converts the raw chrono duration into nanoseconds.
            // .count() extracts the raw integer value from the duration object.
            auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   end - start_)
                                   .count();

            results_.push_back({label_, duration_ns, item_count_});
        }

    private:
        std::string label_;
        size_t item_count_;
        std::vector<BenchmarkResult> &results_; // Reference — no copy
        TimePoint start_;
    };

    // ============================================================================
    // print_benchmark_report() — Prints a formatted performance table
    // ============================================================================
    inline void print_benchmark_report(const std::vector<BenchmarkResult> &results)
    {
        std::cout << "\n";
        std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
        std::cout << "║           MarketStream ETL — Performance Report              ║\n";
        std::cout << "╠══════════════════╦══════════════╦═════════════╦═════════════╣\n";
        std::cout << "║ Stage            ║ Duration(ms) ║  ns/trade   ║ trades/sec  ║\n";
        std::cout << "╠══════════════════╬══════════════╬═════════════╬═════════════╣\n";

        long long total_ns = 0;

        for (const auto &r : results)
        {
            total_ns += r.duration_ns;

            // std::setw(n)  = set column width to n characters
            // std::fixed    = don't use scientific notation (no 1.5e+06)
            // std::setprecision(n) = show n decimal places
            // std::left/right = text alignment within the column
            std::cout << "║ "
                      << std::left << std::setw(16) << r.label
                      << " ║ "
                      << std::right << std::fixed << std::setprecision(3)
                      << std::setw(12) << r.duration_ms()
                      << " ║ "
                      << std::right << std::fixed << std::setprecision(1)
                      << std::setw(11) << r.ns_per_item()
                      << " ║ "
                      << std::right << std::fixed << std::setprecision(0)
                      << std::setw(11) << r.items_per_second()
                      << " ║\n";
        }

        std::cout << "╠══════════════════╬══════════════╬═════════════╬═════════════╣\n";

        // Total row
        double total_ms = static_cast<double>(total_ns) / 1'000'000.0;
        std::cout << "║ "
                  << std::left << std::setw(16) << "TOTAL PIPELINE"
                  << " ║ "
                  << std::right << std::fixed << std::setprecision(3)
                  << std::setw(12) << total_ms
                  << " ║             ║             ║\n";

        std::cout << "╚══════════════════╩══════════════╩═════════════╩═════════════╝\n";
        std::cout << "\n";
    }

} // namespace MarketStream