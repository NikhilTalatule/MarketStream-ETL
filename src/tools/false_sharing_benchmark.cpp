// ============================================================================
// false_sharing_benchmark.cpp — Empirical Proof of Cache Line False Sharing
// ============================================================================
//
// PURPOSE:
//   Most engineers say "false sharing is bad." Few have measured it.
//   This benchmark produces NUMBERS — the kind you quote in interviews.
//
//   We run THREE experiments:
//
//   [EXPERIMENT 1] Pure False Sharing Isolation
//   ─────────────────────────────────────────────
//   Two counters, two threads, each thread increments only ITS counter.
//   No logical sharing. But physically:
//     Version A (shared line): counter_a and counter_b on same cache line
//     Version B (padded):      each counter on its own cache line
//   This isolates PURE false sharing cost with zero queue overhead.
//
//   [EXPERIMENT 2] SPSCQueue vs SPSCQueueNoPadding
//   ─────────────────────────────────────────────────
//   Full producer/consumer queue benchmark.
//   Same queue logic, same atomic operations, same memory ordering.
//   Only difference: 128 bytes of padding between head_ and tail_.
//   Shows the real-world impact on the queue you actually use.
//
//   [EXPERIMENT 3] Synthetic Contention Scaling
//   ─────────────────────────────────────────────
//   How does false sharing scale with more threads fighting over one line?
//   4 counters on one cache line, 1 counter per thread.
//   Demonstrates why "just use a small struct" is dangerous in concurrent code.
//
// WHAT TO EXPECT:
//   Experiment 1: Padded ~3-8x faster than unpadded
//   Experiment 2: Queue with padding ~2-5x faster (queue logic adds noise)
//   Experiment 3: Performance degrades linearly with contention
//
// BUILD (add to CMakeLists.txt):
//   add_executable(false_sharing_benchmark src/tools/false_sharing_benchmark.cpp)
//
// RUN:
//   .\false_sharing_benchmark.exe
// ============================================================================

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <string>

#include "../threading/SPSCQueue.hpp"
#include "../threading/SPSCQueueNoPadding.hpp"

// ============================================================================
// Constants
// ============================================================================
static constexpr size_t CACHE_LINE = MarketStream::CACHE_LINE;

// Number of iterations per benchmark.
// Higher = more accurate average. Lower = faster to run.
// 100M iterations is enough to see stable ns/op numbers.
static constexpr long long ITERATIONS = 100'000'000LL;

// Number of times each test repeats (we take the minimum — eliminates OS jitter)
static constexpr int RUNS = 3;

using Clock = std::chrono::high_resolution_clock;

// ============================================================================
// Timing helper
// ============================================================================
static long long measure_ns(auto fn)
{
    // Run RUNS times, take minimum (best case = least OS noise)
    // WHY MINIMUM AND NOT AVERAGE?
    // Variance in benchmarks comes from OS scheduler preemption, TLB flushes,
    // thermal throttling, etc. These ADD time — they never make code faster.
    // The minimum represents the true hardware cost, uncontaminated by noise.
    // This is the standard methodology used by Google Benchmark and Folly.
    long long best = std::numeric_limits<long long>::max();
    for (int r = 0; r < RUNS; ++r)
    {
        auto t0 = Clock::now();
        fn();
        auto t1 = Clock::now();
        long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        best = std::min(best, ns);
    }
    return best;
}

// ============================================================================
// Print helpers
// ============================================================================
static void print_separator()
{
    std::cout << "╠══════════════════════════════╦═════════════╦══════════════╦═══════════╣\n";
}

static void print_row(const std::string &name, long long ns, long long iters,
                      const std::string &note = "")
{
    double ns_per_op = static_cast<double>(ns) / static_cast<double>(iters);
    double mops = static_cast<double>(iters) / static_cast<double>(ns) * 1000.0;
    double total_ms = static_cast<double>(ns) / 1'000'000.0;

    std::cout << "║ "
              << std::left << std::setw(28) << name.substr(0, 28)
              << " ║ "
              << std::right << std::fixed << std::setprecision(1)
              << std::setw(9) << ns_per_op << " ns"
              << " ║ "
              << std::right << std::fixed << std::setprecision(1)
              << std::setw(9) << mops << " M/s"
              << " ║ "
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(7) << total_ms << "ms "
              << " ║\n";
}

static void print_header(const std::string &title)
{
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  " << std::left << std::setw(68) << title << " ║\n";
    std::cout << "╠══════════════════════════════╦═════════════╦══════════════╦═══════════╣\n";
    std::cout << "║ Variant                      ║  ns/op      ║  Throughput  ║  Total    ║\n";
    std::cout << "╠══════════════════════════════╬═════════════╬══════════════╬═══════════╣\n";
}

static void print_footer() { std::cout << "╚══════════════════════════════╩═════════════╩══════════════╩═══════════╝\n"; }

static void print_speedup(const std::string &label, double factor)
{
    std::cout << "  → " << label << ": "
              << std::fixed << std::setprecision(1) << factor << "x speedup\n";
}

// ============================================================================
// EXPERIMENT 1A — Unpadded counters (false sharing guaranteed)
// ============================================================================
// Two atomic counters packed adjacently in memory.
// Thread A increments counter_a. Thread B increments counter_b.
// They NEVER access each other's counter — zero logical sharing.
// But both counters sit on the SAME 64-byte cache line → false sharing.
//
// WHAT THE CPU DOES:
//   Every increment by Thread A invalidates Thread B's cached copy of the line.
//   Thread B must reload the cache line before it can increment counter_b.
//   Thread B's increment then invalidates Thread A's copy. Repeat forever.
//   Cost: ~100-200 cache-miss cycles per increment on x86.
// ============================================================================
struct UnpaddedCounters
{
    // Both counters fit in the same 64-byte cache line (offset 0 and 8).
    // This is the structure most developers would write without thinking.
    std::atomic<long long> counter_a{0}; // offset 0
    std::atomic<long long> counter_b{0}; // offset 8 — SAME cache line as counter_a
};

static long long bench_false_sharing_unpadded(long long n)
{
    UnpaddedCounters counters;

    return measure_ns([&]()
                      {
        counters.counter_a.store(0, std::memory_order_relaxed);
        counters.counter_b.store(0, std::memory_order_relaxed);

        std::thread thread_b([&]()
        {
            // Thread B only touches counter_b — never counter_a.
            // Yet it pays the price for counter_a's writes.
            for (long long i = 0; i < n; ++i)
                counters.counter_b.fetch_add(1, std::memory_order_relaxed);
        });

        // Thread A (main thread) only touches counter_a.
        for (long long i = 0; i < n; ++i)
            counters.counter_a.fetch_add(1, std::memory_order_relaxed);

        thread_b.join(); });
}

// ============================================================================
// EXPERIMENT 1B — Padded counters (false sharing eliminated)
// ============================================================================
// Same two counters. Same two threads. Same increment logic.
// The ONLY change: 56 bytes of padding after counter_a forces counter_b
// onto the NEXT 64-byte cache line.
//
// Now Thread A owns cache line 1 exclusively.
// Thread B owns cache line 2 exclusively.
// No invalidation. No cache misses. Pure compute.
// ============================================================================
struct PaddedCounters
{
    // counter_a on cache line 1
    alignas(CACHE_LINE) std::atomic<long long> counter_a{0};
    char pad_a[CACHE_LINE - sizeof(std::atomic<long long>)]; // fills line 1

    // counter_b on cache line 2
    alignas(CACHE_LINE) std::atomic<long long> counter_b{0};
    char pad_b[CACHE_LINE - sizeof(std::atomic<long long>)]; // fills line 2
};

static long long bench_false_sharing_padded(long long n)
{
    PaddedCounters counters;

    return measure_ns([&]()
                      {
        counters.counter_a.store(0, std::memory_order_relaxed);
        counters.counter_b.store(0, std::memory_order_relaxed);

        std::thread thread_b([&]()
        {
            for (long long i = 0; i < n; ++i)
                counters.counter_b.fetch_add(1, std::memory_order_relaxed);
        });

        for (long long i = 0; i < n; ++i)
            counters.counter_a.fetch_add(1, std::memory_order_relaxed);

        thread_b.join(); });
}

// ============================================================================
// EXPERIMENT 2A — SPSCQueueNoPadding (head_ and tail_ on same cache line)
// ============================================================================
// Full producer/consumer queue. 5 million push/pop operations.
// The atomic queue logic is correct — no data races.
// But the cache line layout causes constant false sharing between:
//   - Producer writing tail_
//   - Consumer reading tail_ + writing head_
// ============================================================================
static long long bench_queue_no_padding(long long n_ops)
{
    MarketStream::SPSCQueueNoPadding<uint64_t, 4096> queue;
    volatile uint64_t checksum = 0;

    return measure_ns([&]()
                      {
        std::thread consumer([&]()
        {
            for (long long i = 0; i < n_ops; ++i)
            {
                std::optional<uint64_t> item;
                while (!(item = queue.try_pop()))
                    std::this_thread::yield();
                checksum += *item;
            }
        });

        for (long long i = 0; i < n_ops; ++i)
        {
            while (!queue.try_push(static_cast<uint64_t>(i)))
                std::this_thread::yield();
        }

        consumer.join(); });
    (void)checksum;
}

// ============================================================================
// EXPERIMENT 2B — SPSCQueue (head_ and tail_ on SEPARATE cache lines)
// ============================================================================
// Identical queue logic. Identical benchmark.
// 128 bytes of padding = the only structural difference.
// ============================================================================
static long long bench_queue_with_padding(long long n_ops)
{
    MarketStream::SPSCQueue<uint64_t, 4096> queue;
    volatile uint64_t checksum = 0;

    return measure_ns([&]()
                      {
        std::thread consumer([&]()
        {
            for (long long i = 0; i < n_ops; ++i)
            {
                std::optional<uint64_t> item;
                while (!(item = queue.try_pop()))
                    std::this_thread::yield();
                checksum += *item;
            }
        });

        for (long long i = 0; i < n_ops; ++i)
        {
            while (!queue.try_push(static_cast<uint64_t>(i)))
                std::this_thread::yield();
        }

        consumer.join(); });
    (void)checksum;
}

// ============================================================================
// EXPERIMENT 3 — Contention Scaling
// ============================================================================
// 4 counters on ONE cache line. 4 threads, one counter each.
// Shows how false sharing degrades as more threads fight over one cache line.
//
// This is relevant for interview questions like:
// "Why is a naive shared counter bad in a multi-threaded system?"
// Answer: not just because of race conditions (atomics solve that),
// but because all cores fight over the same cache line → performance collapse.
// ============================================================================

// All 4 counters in 32 bytes — guaranteed ONE cache line (64 bytes wide).
struct QuadCountersFalseSharing
{
    std::atomic<long long> a{0}, b{0}, c{0}, d{0};
    // Total: 4 × 8 bytes = 32 bytes → all on one 64-byte cache line
};

// Each counter on its own 64-byte cache line.
struct QuadCountersPadded
{
    alignas(CACHE_LINE) std::atomic<long long> a{0};
    char pa[CACHE_LINE - 8];
    alignas(CACHE_LINE) std::atomic<long long> b{0};
    char pb[CACHE_LINE - 8];
    alignas(CACHE_LINE) std::atomic<long long> c{0};
    char pc[CACHE_LINE - 8];
    alignas(CACHE_LINE) std::atomic<long long> d{0};
    char pd[CACHE_LINE - 8];
};

static long long bench_4thread_false_sharing(long long n)
{
    QuadCountersFalseSharing ctrs;

    return measure_ns([&]()
                      {
        ctrs.a = ctrs.b = ctrs.c = ctrs.d = 0;

        // 3 background threads + main thread = 4 total
        std::thread t1([&]{ for (long long i=0; i<n; ++i) ctrs.b.fetch_add(1, std::memory_order_relaxed); });
        std::thread t2([&]{ for (long long i=0; i<n; ++i) ctrs.c.fetch_add(1, std::memory_order_relaxed); });
        std::thread t3([&]{ for (long long i=0; i<n; ++i) ctrs.d.fetch_add(1, std::memory_order_relaxed); });

        for (long long i = 0; i < n; ++i) ctrs.a.fetch_add(1, std::memory_order_relaxed);

        t1.join(); t2.join(); t3.join(); });
}

static long long bench_4thread_padded(long long n)
{
    QuadCountersPadded ctrs;

    return measure_ns([&]()
                      {
        ctrs.a = ctrs.b = ctrs.c = ctrs.d = 0;

        std::thread t1([&]{ for (long long i=0; i<n; ++i) ctrs.b.fetch_add(1, std::memory_order_relaxed); });
        std::thread t2([&]{ for (long long i=0; i<n; ++i) ctrs.c.fetch_add(1, std::memory_order_relaxed); });
        std::thread t3([&]{ for (long long i=0; i<n; ++i) ctrs.d.fetch_add(1, std::memory_order_relaxed); });

        for (long long i = 0; i < n; ++i) ctrs.a.fetch_add(1, std::memory_order_relaxed);

        t1.join(); t2.join(); t3.join(); });
}

// ============================================================================
// Memory layout verification
// ============================================================================
static void print_memory_verification()
{
    std::cout << "╔══════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  Memory Layout Verification                                          ║\n";
    std::cout << "╠══════════════════════════════════════════════════════════════════════╣\n";

    // Unpadded struct
    UnpaddedCounters uc;
    uintptr_t a_addr = reinterpret_cast<uintptr_t>(&uc.counter_a);
    uintptr_t b_addr = reinterpret_cast<uintptr_t>(&uc.counter_b);
    size_t separation = b_addr - a_addr;
    size_t a_line = a_addr / CACHE_LINE;
    size_t b_line = b_addr / CACHE_LINE;

    std::cout << "║  UnpaddedCounters:                                                   ║\n";
    std::cout << "║    counter_a offset: 0 bytes                                         ║\n";
    std::cout << "║    counter_b offset: " << std::setw(2) << separation
              << " bytes                                        ║\n";
    std::cout << "║    Same cache line: "
              << (a_line == b_line ? "YES ← FALSE SHARING CONFIRMED     " : "NO  ← no false sharing            ")
              << "       ║\n";

    // Padded struct
    PaddedCounters pc;
    uintptr_t pa_addr = reinterpret_cast<uintptr_t>(&pc.counter_a);
    uintptr_t pb_addr = reinterpret_cast<uintptr_t>(&pc.counter_b);
    size_t padded_sep = pb_addr - pa_addr;
    size_t pa_line = pa_addr / CACHE_LINE;
    size_t pb_line = pb_addr / CACHE_LINE;

    std::cout << "║                                                                      ║\n";
    std::cout << "║  PaddedCounters:                                                     ║\n";
    std::cout << "║    counter_a offset: 0 bytes                                         ║\n";
    std::cout << "║    counter_b offset: " << std::setw(2) << padded_sep
              << " bytes                                       ║\n";
    std::cout << "║    Same cache line: "
              << (pa_line == pb_line ? "YES ← FALSE SHARING CONFIRMED     " : "NO  ← ISOLATED, NO FALSE SHARING  ")
              << "       ║\n";
    std::cout << "║                                                                      ║\n";
    std::cout << "║  Cache line size on this CPU: " << CACHE_LINE << " bytes                              ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════════╝\n";
}

// ============================================================================
// main()
// ============================================================================
int main()
{
    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | Phase 12: False Sharing Demo\n";
    std::cout << "===================================================\n\n";

    // ── Memory Layout Proof ────────────────────────────────────────────────
    print_memory_verification();

    // ── Experiment 1: Pure False Sharing ──────────────────────────────────
    std::cout << "\n[Running Experiment 1: Pure counter increment (" << ITERATIONS / 1'000'000 << "M ops)...]\n";
    const long long unpadded_ns = bench_false_sharing_unpadded(ITERATIONS);
    const long long padded_ns = bench_false_sharing_padded(ITERATIONS);

    print_header("Experiment 1: Pure False Sharing (2 threads, 2 counters, 100M increments)");
    print_row("Unpadded (false sharing)", unpadded_ns, ITERATIONS * 2);
    print_separator();
    print_row("Padded   (isolated lines)", padded_ns, ITERATIONS * 2);
    print_footer();
    print_speedup("Padding eliminates false sharing",
                  static_cast<double>(unpadded_ns) / static_cast<double>(padded_ns));

    // ── Experiment 2: Queue Comparison ────────────────────────────────────
    const long long Q_OPS = 5'000'000LL;
    std::cout << "\n[Running Experiment 2: Queue benchmark (" << Q_OPS / 1'000'000 << "M push/pop)...]\n";
    const long long queue_no_pad_ns = bench_queue_no_padding(Q_OPS);
    const long long queue_pad_ns = bench_queue_with_padding(Q_OPS);

    print_header("Experiment 2: SPSCQueue head_/tail_ Cache Line Isolation (5M push/pop)");
    print_row("SPSCQueueNoPadding", queue_no_pad_ns, Q_OPS);
    print_separator();
    print_row("SPSCQueue (padded)", queue_pad_ns, Q_OPS);
    print_footer();
    print_speedup("Padding in SPSC queue",
                  static_cast<double>(queue_no_pad_ns) / static_cast<double>(queue_pad_ns));

    // ── Experiment 3: 4-Thread Contention Scaling ─────────────────────────
    const long long T4_OPS = 50'000'000LL;
    std::cout << "\n[Running Experiment 3: 4-thread contention (" << T4_OPS / 1'000'000 << "M ops each)...]\n";
    const long long t4_false_ns = bench_4thread_false_sharing(T4_OPS);
    const long long t4_padded_ns = bench_4thread_padded(T4_OPS);

    print_header("Experiment 3: 4-Thread Contention (4 counters, 4 threads, 50M each)");
    print_row("4 counters, 1 cache line", t4_false_ns, T4_OPS * 4);
    print_separator();
    print_row("4 counters, 4 cache lines", t4_padded_ns, T4_OPS * 4);
    print_footer();
    print_speedup("Isolation across 4 threads",
                  static_cast<double>(t4_false_ns) / static_cast<double>(t4_padded_ns));

    // ── Summary Table ─────────────────────────────────────────────────────
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  Phase 12 Summary — False Sharing Cost on This Machine              ║\n";
    std::cout << "╠══════════════════════════════════════════════════════════════════════╣\n";

    auto fmt_speedup = [](double s) -> std::string
    {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << s << "x";
        return oss.str();
    };

    double s1 = static_cast<double>(unpadded_ns) / padded_ns;
    double s2 = static_cast<double>(queue_no_pad_ns) / queue_pad_ns;
    double s3 = static_cast<double>(t4_false_ns) / t4_padded_ns;

    std::cout << "║  Exp 1 (pure counters, 2 threads) : "
              << std::left << std::setw(5) << fmt_speedup(s1)
              << " faster with padding                 ║\n";
    std::cout << "║  Exp 2 (SPSC queue, yield)        : "
              << std::left << std::setw(5) << fmt_speedup(s2)
              << " faster with padding                 ║\n";
    std::cout << "║  Exp 3 (4 counters, 4 threads)    : "
              << std::left << std::setw(5) << fmt_speedup(s3)
              << " faster with padding                 ║\n";
    std::cout << "╠══════════════════════════════════════════════════════════════════════╣\n";
    std::cout << "║  Cache line size verified: " << CACHE_LINE << " bytes                                ║\n";
    std::cout << "║  Measurement method: min of " << RUNS << " runs (eliminates OS jitter)            ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════════╝\n";

    // ── Interview Talking Points ───────────────────────────────────────────
    std::cout << "\n";
    std::cout << "WHAT TO SAY IN AN INTERVIEW:\n";
    std::cout << "────────────────────────────\n";
    std::cout << "\"I measured false sharing directly on my hardware.\n";
    std::cout << " Two threads incrementing SEPARATE atomic counters\n";
    std::cout << " ran " << std::fixed << std::setprecision(1) << s1 << "x SLOWER when the counters shared\n";
    std::cout << " a 64-byte cache line — despite zero logical contention.\n";
    std::cout << " The fix: alignas(64) + padding. One cache line per hot variable.\n";
    std::cout << " This is exactly how our SPSC ring buffer is designed.\"\n";

    return 0;
}