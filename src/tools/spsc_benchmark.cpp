// ============================================================================
// spsc_benchmark.cpp — Lock-Free SPSC vs Mutex Queue Latency Comparison
// ============================================================================
//
// PURPOSE:
//   This benchmark answers the question every HFT interviewer asks:
//   "How would you pass data between threads with minimum latency?"
//
//   We run THREE head-to-head comparisons:
//     1. MutexQueue   — std::queue + std::mutex (the naive approach)
//     2. SPSCQueue    — lock-free ring buffer (our implementation)
//     3. Busy-wait SPSC — SPSC with spin-wait instead of sleep (absolute floor)
//
//   Each test: 10 million round trips, producer pushes, consumer pops.
//   We measure:
//     - Total wall time (throughput test)
//     - Per-operation latency in nanoseconds
//     - Throughput in millions of operations per second
//
// HOW TO BUILD (add to CMakeLists.txt):
//   add_executable(spsc_benchmark src/tools/spsc_benchmark.cpp)
//   # No extra libraries — pure C++20 standard library
//
// HOW TO RUN:
//   .\spsc_benchmark.exe
// ============================================================================

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <mutex>
#include <queue>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <condition_variable>
#include "../SPSCQueue.hpp"

// ============================================================================
// MutexQueue — The baseline: std::queue + std::mutex
// ============================================================================
// This is what most developers write first. It is correct, but slow because:
//   - Every push: lock (kernel), push, unlock (kernel) = ~1000ns overhead
//   - Every pop:  lock (kernel), pop,  unlock (kernel) = ~1000ns overhead
//   - If the queue is empty on pop: the thread must WAIT
//     → we use condition_variable to sleep until notified (OS involvement)
//
// This is the CORRECT implementation of a mutex queue — not a strawman.
// We're comparing against the best naive approach, not a broken one.
// ============================================================================
template <typename T>
class MutexQueue
{
public:
    void push(T item)
    {
        // std::lock_guard = RAII mutex lock.
        // Constructor acquires mutex. Destructor releases it.
        // Scope ends at } below → automatic release (no forget-to-unlock bugs).
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(item));

        // Notify waiting consumer that an item is available.
        // Without notify_one(): consumer sleeps forever even after we push.
        // cv_.notify_one() is a kernel call — adds latency.
        cv_.notify_one();
    }

    T pop()
    {
        // unique_lock needed (not lock_guard) because cv_.wait() must
        // temporarily RELEASE the lock while sleeping.
        // lock_guard can't do this — it holds the lock until destruction.
        std::unique_lock<std::mutex> lock(mutex_);

        // cv_.wait(lock, predicate):
        //   1. Check predicate (!queue_.empty())
        //   2. If false: atomically release lock + sleep
        //   3. Wake on notify_one()
        //   4. Re-acquire lock + re-check predicate (spurious wakeup protection)
        //   5. Return when predicate is true and lock is held
        cv_.wait(lock, [this]
                 { return !queue_.empty(); });

        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

// ============================================================================
// Timing utilities
// ============================================================================
using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
using Nanos = std::chrono::nanoseconds;

static long long elapsed_ns(TimePoint start, TimePoint end)
{
    return std::chrono::duration_cast<Nanos>(end - start).count();
}

// ============================================================================
// Print a formatted result row
// ============================================================================
static void print_result(
    const std::string &name,
    long long total_ns,
    long long operations)
{
    double ns_per_op = static_cast<double>(total_ns) / static_cast<double>(operations);
    double mops_per_sec = static_cast<double>(operations) / static_cast<double>(total_ns) * 1000.0;
    double total_ms = static_cast<double>(total_ns) / 1'000'000.0;

    std::cout << "║ "
              << std::left << std::setw(22) << name
              << " ║ "
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(12) << ns_per_op
              << " ║ "
              << std::right << std::fixed << std::setprecision(2)
              << std::setw(12) << mops_per_sec
              << " ║ "
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(10) << total_ms
              << " ║\n";
}

// ============================================================================
// BENCHMARK 1: MutexQueue
// ============================================================================
// Two threads share a MutexQueue.
// Producer pushes N uint64_t values. Consumer pops them.
// We measure total wall time after both threads complete.
//
// WHY uint64_t AND NOT Trade?
// We want to measure QUEUE OVERHEAD, not serialization overhead.
// A simple integer shows us the raw cost of the synchronization mechanism.
// ============================================================================
static long long bench_mutex_queue(long long n_ops)
{
    MutexQueue<uint64_t> queue;
    std::atomic<bool> consumer_done{false};

    auto t_start = Clock::now();

    // Consumer thread: pop N items, sum them (prevent compiler optimization)
    // WHY SUM THEM?
    // If we don't USE the popped values, the compiler might eliminate the pop
    // entirely ("dead code elimination"). The sum forces real work.
    volatile uint64_t checksum = 0;

    std::thread consumer([&]()
                         {
        for (long long i = 0; i < n_ops; ++i)
        {
            checksum += queue.pop();
        }
        consumer_done.store(true, std::memory_order_release); });

    // Producer thread: push N items (in main thread)
    for (long long i = 0; i < n_ops; ++i)
    {
        queue.push(static_cast<uint64_t>(i));
    }

    consumer.join();

    auto t_end = Clock::now();
    (void)checksum; // Suppress unused warning — we care it exists, not its value
    return elapsed_ns(t_start, t_end);
}

// ============================================================================
// BENCHMARK 2: SPSCQueue with sleep-yield on full/empty
// ============================================================================
// Producer and consumer run on separate threads.
// On full (producer) or empty (consumer): std::this_thread::yield().
//
// yield() = "give up my CPU timeslice, let other threads run."
// It's NOT a kernel sleep (no condition_variable), but it IS an OS call.
// Cheaper than mutex (~100-500ns vs ~1000ns) but not free.
//
// WHY YIELD AND NOT SPIN?
// Spinning burns 100% CPU on the waiting thread — wastes a core doing nothing.
// yield() is the polite version: "I'm waiting, schedule someone else."
// For our benchmark, yield() represents the practical SPSC usage pattern.
// ============================================================================
static long long bench_spsc_yield(long long n_ops)
{
    // 4096 slots — large enough that producer rarely blocks
    // (4096 items × sizeof(uint64_t) = 32KB — fits in L1/L2 cache)
    MarketStream::SPSCQueue<uint64_t, 4096> queue;

    auto t_start = Clock::now();

    volatile uint64_t checksum = 0;

    std::thread consumer([&]()
                         {
        for (long long i = 0; i < n_ops; ++i)
        {
            // Spin-yield until an item is available
            // try_pop() returns std::optional<uint64_t>
            // If nullopt (empty): yield and retry
            std::optional<uint64_t> item;
            while (!(item = queue.try_pop()))
            {
                // std::this_thread::yield():
                //   Tells the OS scheduler "I'm waiting, run something else."
                //   Better than a busy spin: doesn't burn CPU on the waiting thread.
                //   Worse than condition_variable for long waits (no sleep, just reschedule).
                //   For HFT with near-continuous data: yield is the right choice.
                std::this_thread::yield();
            }
            checksum += *item;
        } });

    for (long long i = 0; i < n_ops; ++i)
    {
        while (!queue.try_push(static_cast<uint64_t>(i)))
        {
            std::this_thread::yield(); // Queue full — rare, yield and retry
        }
    }

    consumer.join();

    auto t_end = Clock::now();
    (void)checksum;
    return elapsed_ns(t_start, t_end);
}

// ============================================================================
// BENCHMARK 3: SPSCQueue with busy-spin (absolute latency floor)
// ============================================================================
// Producer and consumer NEVER sleep or yield — they spin continuously.
// This represents the ABSOLUTE MINIMUM latency achievable on this hardware.
// The CPU core is 100% occupied at all times. This is:
//
//   PROS: Lowest possible latency (~5-20ns per operation on modern x86)
//   CONS: Burns an ENTIRE CPU CORE doing nothing but checking the queue.
//         Unusable in production unless you have a dedicated core.
//         This is what "core pinning" means in HFT: one core, one task, no sharing.
//
// Citadel and HRT pin a dedicated core per market data feed channel.
// The OS is told: "never schedule anything else on core 3 — that's our feed core."
// That core spins forever, checking the network buffer, with ~10ns latency.
//
// This benchmark tells us: "how fast COULD this queue be with unlimited resources?"
// ============================================================================
static long long bench_spsc_spin(long long n_ops)
{
    MarketStream::SPSCQueue<uint64_t, 4096> queue;

    auto t_start = Clock::now();

    volatile uint64_t checksum = 0;

    std::thread consumer([&]()
                         {
        for (long long i = 0; i < n_ops; ++i)
        {
            std::optional<uint64_t> item;
            // Pure spin: no yield, no sleep. Hammers the CPU.
            // In production: this core is pinned and isolated.
            while (!(item = queue.try_pop()));
            checksum += *item;
        } });

    for (long long i = 0; i < n_ops; ++i)
    {
        // Producer also spins if queue is full (rare with 4096 slots)
        while (!queue.try_push(static_cast<uint64_t>(i)))
            ;
    }

    consumer.join();

    auto t_end = Clock::now();
    (void)checksum;
    return elapsed_ns(t_start, t_end);
}

// ============================================================================
// BONUS: Queue Size Demo
// ============================================================================
// Shows the memory footprint of our SPSCQueue vs a dynamic mutex queue.
// In HFT: memory footprint matters for cache fit.
// ============================================================================
static void print_memory_layout()
{
    // Instantiate at different capacities to show memory scaling
    using Q256 = MarketStream::SPSCQueue<uint64_t, 256>;
    using Q4096 = MarketStream::SPSCQueue<uint64_t, 4096>;
    using Q65536 = MarketStream::SPSCQueue<uint64_t, 65536>;

    std::cout << "\n╔═══════════════════════════════════════════════════════╗\n";
    std::cout << "║         SPSCQueue Memory Layout Analysis              ║\n";
    std::cout << "╠══════════════════════╦═══════════╦════════════════════╣\n";
    std::cout << "║ Queue Type           ║ Size      ║ Cache Fit          ║\n";
    std::cout << "╠══════════════════════╬═══════════╬════════════════════╣\n";

    auto fits = [](size_t bytes) -> std::string
    {
        if (bytes <= 32768)
            return "L1 cache (32KB)";
        if (bytes <= 262144)
            return "L2 cache (256KB)";
        if (bytes <= 3145728)
            return "L3 cache (3MB)";
        return "RAM (cache miss!)";
    };

    std::cout << "║ SPSC<uint64, 256>    ║ "
              << std::setw(7) << sizeof(Q256) << "B  ║ "
              << std::left << std::setw(18) << fits(sizeof(Q256)) << " ║\n";
    std::cout << "║ SPSC<uint64, 4096>   ║ "
              << std::setw(7) << sizeof(Q4096) << "B  ║ "
              << std::setw(18) << fits(sizeof(Q4096)) << " ║\n";
    std::cout << "║ SPSC<uint64, 65536>  ║ "
              << std::setw(7) << sizeof(Q65536) << "B  ║ "
              << std::setw(18) << fits(sizeof(Q65536)) << " ║\n";
    std::cout << std::right;
    std::cout << "╚══════════════════════╩═══════════╩════════════════════╝\n";
    std::cout << "\n";
    std::cout << "  head_ offset : " << offsetof(Q4096, head_) << " bytes\n";
    std::cout << "  tail_ offset : " << offsetof(Q4096, tail_) << " bytes\n";
    std::cout << "  Separation   : "
              << offsetof(Q4096, tail_) - offsetof(Q4096, head_) << " bytes"
              << " (should be >= 64 = one cache line)\n";
    std::cout << "\n";
}

// ============================================================================
// main()
// ============================================================================
int main()
{
    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | Phase 11: SPSC Benchmark\n";
    std::cout << "===================================================\n\n";

    // Show memory layout first
    print_memory_layout();

    // Number of operations per benchmark
    // 5 million: enough to be statistically meaningful, fast enough to not bore you
    const long long N = 5'000'000;

    std::cout << "Running " << N / 1'000'000 << "M push/pop operations per test...\n";
    std::cout << "(Each test: 1 producer thread + 1 consumer thread)\n\n";

    // Warmup: run each once to let the CPU ramp up clock speed (turbo boost)
    // and warm the caches. Without warmup, first measurement is artificially slow.
    std::cout << "[Warming up...]\n";
    bench_mutex_queue(100'000);
    bench_spsc_yield(100'000);
    bench_spsc_spin(100'000);
    std::cout << "[Warmup complete. Running benchmarks...]\n\n";

    // ── Run benchmarks ─────────────────────────────────────────────────────
    const long long mutex_ns = bench_mutex_queue(N);
    std::cout << "[1/3] Mutex queue done.\n";

    const long long spsc_yield_ns = bench_spsc_yield(N);
    std::cout << "[2/3] SPSC+yield done.\n";

    const long long spsc_spin_ns = bench_spsc_spin(N);
    std::cout << "[3/3] SPSC+spin done.\n\n";

    // ── Results Table ──────────────────────────────────────────────────────
    std::cout << "╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║     MarketStream — Phase 11: Queue Latency Benchmark       ║\n";
    std::cout << "╠════════════════════════╦══════════════╦══════════════╦═════════════╣\n";
    std::cout << "║ Queue Type             ║ ns/operation ║  M ops/sec   ║ Total (ms)  ║\n";
    std::cout << "╠════════════════════════╬══════════════╬══════════════╬═════════════╣\n";

    print_result("MutexQueue", mutex_ns, N);
    print_result("SPSCQueue + yield", spsc_yield_ns, N);
    print_result("SPSCQueue + spin", spsc_spin_ns, N);

    std::cout << "╚════════════════════════╩══════════════╩══════════════╩═════════════╝\n\n";

    // ── Speedup Analysis ───────────────────────────────────────────────────
    double spsc_vs_mutex = static_cast<double>(mutex_ns) / static_cast<double>(spsc_yield_ns);
    double spin_vs_mutex = static_cast<double>(mutex_ns) / static_cast<double>(spsc_spin_ns);
    double spin_vs_yield = static_cast<double>(spsc_yield_ns) / static_cast<double>(spsc_spin_ns);

    std::cout << "╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║               Speedup Analysis                             ║\n";
    std::cout << "╠════════════════════════════════════════════════════════════╣\n";
    std::cout << "║  SPSC+yield  vs  Mutex : "
              << std::fixed << std::setprecision(1) << std::setw(6)
              << spsc_vs_mutex << "x faster"
              << std::string(27 - std::to_string((int)spsc_vs_mutex).size(), ' ')
              << "║\n";
    std::cout << "║  SPSC+spin   vs  Mutex : "
              << std::fixed << std::setprecision(1) << std::setw(6)
              << spin_vs_mutex << "x faster"
              << std::string(27 - std::to_string((int)spin_vs_mutex).size(), ' ')
              << "║\n";
    std::cout << "║  SPSC+spin   vs  Yield : "
              << std::fixed << std::setprecision(1) << std::setw(6)
              << spin_vs_yield << "x faster"
              << std::string(27 - std::to_string((int)spin_vs_yield).size(), ' ')
              << "║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n\n";

    // ── Interview Talking Points ───────────────────────────────────────────
    std::cout << "INTERVIEW TALKING POINTS:\n";
    std::cout << "─────────────────────────\n";
    std::cout << "1. SPSC is safe without locks because producer owns tail,\n";
    std::cout << "   consumer owns head — they never contend the same variable.\n\n";
    std::cout << "2. Cache line padding (alignas(64)) prevents false sharing.\n";
    std::cout << "   Without it: cores fight over the same cache line → 10x slower.\n\n";
    std::cout << "3. acquire/release ordering without mutex:\n";
    std::cout << "   Producer: buffer[tail]=item; tail.store(release);\n";
    std::cout << "   Consumer: tail.load(acquire); item=buffer[tail];\n";
    std::cout << "   Acquire/release creates a happens-before relationship.\n\n";
    std::cout << "4. Capacity must be power-of-2: index & (N-1) replaces % N.\n";
    std::cout << "   Bitwise AND = 1 CPU instruction vs 10-40 for integer division.\n\n";
    std::cout << "5. Spin vs yield: spin = lowest latency, but burns a core.\n";
    std::cout << "   HFT firms pin one core per feed channel for exactly this.\n";

    return 0;
}