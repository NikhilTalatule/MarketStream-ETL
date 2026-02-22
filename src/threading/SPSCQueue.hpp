#pragma once

// ============================================================================
// SPSCQueue — Single Producer, Single Consumer Lock-Free Ring Buffer
// ============================================================================
//
// WHY THIS DATA STRUCTURE EXISTS
// ─────────────────────────────────────────────────────────────────────────────
// Every HFT system has two threads that need to pass data between each other:
//
//   [Feed Handler Thread]  →  market data  →  [Strategy Thread]
//   [Parser Thread]        →  parsed order →  [Risk Thread]
//   [Network Thread]       →  raw packet   →  [Decode Thread]
//
// The naive approach: a std::queue protected by a std::mutex.
// The problem:
//   mutex.lock()   = kernel syscall = OS involvement = ~500-1000 nanoseconds
//   mutex.unlock() = another syscall = another ~500-1000 nanoseconds
//
// At 1 million messages/second:
//   Mutex overhead = 1M × 1000ns = 1 full second of pure lock overhead
//   That is time where ZERO actual processing happens.
//
// The SPSC solution: NO mutex at all.
// Because there is exactly ONE writer and ONE reader, we can use atomic
// operations on head/tail indices to coordinate without any locking.
// Atomic operations cost ~5-50 nanoseconds — 20-100x faster than a mutex.
//
// CORRECTNESS WITHOUT LOCKING
// ─────────────────────────────────────────────────────────────────────────────
// Safety in SPSC comes from GEOMETRY, not locks:
//
//   Ring buffer slots: [0][1][2][3][4][5][6][7]
//                           ^head                ^tail
//                       Consumer reads here  Producer writes here
//
//   head and tail are NEVER the same slot (empty = head==tail, full = tail+1==head).
//   Producer ONLY writes to slot[tail] — consumer never touches tail.
//   Consumer ONLY reads from slot[head] — producer never touches head.
//   They operate on DIFFERENT MEMORY at all times → zero race condition.
//
// The only shared state: head_ and tail_ indices.
// These are std::atomic<size_t> — guaranteed single-instruction read/write.
// No mutex. No OS. No context switch. Pure userspace.
//
// CACHE LINE PADDING — The Hidden Performance Killer
// ─────────────────────────────────────────────────────────────────────────────
// Modern CPUs don't read individual bytes — they read 64-byte "cache lines."
// If head_ and tail_ share a cache line:
//
//   [head_ | tail_ | padding...]  ← ONE cache line (64 bytes)
//    ^Consumer       ^Producer
//
//   When Producer updates tail_ → CPU invalidates this cache line for ALL cores.
//   Consumer must reload the entire cache line to read head_ (which didn't change!).
//   Result: ~200ns of cache coherence traffic per operation.
//   This is called "FALSE SHARING" — two variables sharing a cache line
//   unnecessarily, causing cores to constantly invalidate each other's caches.
//
// Fix: put head_ and tail_ on SEPARATE cache lines:
//
//   [head_ | 60 bytes of padding]  ← Consumer's exclusive cache line
//   [tail_ | 60 bytes of padding]  ← Producer's exclusive cache line
//
//   Now updating tail_ does NOT touch Consumer's cache line.
//   Consumer reads head_ from L1 cache every time → ~1ns latency.
//
// INTERVIEW TALKING POINT:
//   "False sharing is the silent killer of concurrent performance.
//    Two threads can operate on completely separate logical data,
//    yet fight over the same physical cache line. The fix is padding
//    each hot variable to fill a complete cache line, ensuring exclusive
//    ownership per core."
//
// ============================================================================

#include <atomic>   // std::atomic — CPU-level atomic read/write
#include <array>    // std::array — stack-allocated fixed-size buffer
#include <optional> // std::optional — try_pop returns empty if queue is empty
#include <cstddef>  // std::hardware_destructive_interference_size

namespace MarketStream
{

// ============================================================================
// CACHE LINE SIZE
// ============================================================================
// hardware_destructive_interference_size = the CPU's actual cache line size.
// On x86/x64: almost always 64 bytes.
// On ARM (Apple M1/M2): 128 bytes.
// Using the std:: constant makes our code portable.
//
// WHY constexpr AND NOT A RUNTIME VALUE?
// Cache line size is a hardware constant — never changes during program execution.
// constexpr = computed at compile time = zero runtime cost.
// The compiler can use this to layout structs optimally.
// ============================================================================
#ifdef __cpp_lib_hardware_interference_size
    static constexpr size_t CACHE_LINE = std::hardware_destructive_interference_size;
#else
    // Fallback for MSVC / older GCC where the constant isn't defined
    static constexpr size_t CACHE_LINE = 64;
#endif

    // ============================================================================
    // SPSCQueue<T, Capacity>
    // ============================================================================
    // TEMPLATE PARAMETERS:
    //   T        — the type of element to store (Trade, Order, Packet, etc.)
    //   Capacity — number of slots in the ring. MUST be a power of 2.
    //              Why power of 2? index % Capacity becomes index & (Capacity-1).
    //              Bitwise AND is 1 CPU instruction vs integer division = ~10ns.
    //
    // USAGE EXAMPLE:
    //   SPSCQueue<Trade, 4096> queue;   // 4096 slots (power of 2)
    //
    //   // Producer thread:
    //   queue.try_push(my_trade);       // returns true if pushed, false if full
    //
    //   // Consumer thread:
    //   auto item = queue.try_pop();    // returns std::optional<T>
    //   if (item) process(*item);
    //
    // THREAD SAFETY:
    //   EXACTLY one thread calls push, EXACTLY one calls pop.
    //   No other pattern is safe. For multi-producer: use a different structure.
    // ============================================================================
    template <typename T, size_t Capacity>
    class SPSCQueue
    {
        // Compile-time check: Capacity must be a power of 2.
        // Power of 2 = exactly one bit set in binary representation.
        // 4   = 0b00000100 ✓  (4 & 3) = 0
        // 6   = 0b00000110 ✗  (6 & 5) = 4 ≠ 0
        //
        // static_assert fires at COMPILE TIME, not runtime.
        // Much better than a runtime crash or silent wrong behavior.
        static_assert((Capacity & (Capacity - 1)) == 0,
                      "SPSCQueue capacity must be a power of 2");
        static_assert(Capacity >= 2, "SPSCQueue capacity must be at least 2");

    public:
        // ========================================================================
        // Constructor
        // ========================================================================
        // head_ and tail_ both start at 0.
        // Empty condition: head_ == tail_
        // Full condition:  (tail_ + 1) & MASK == head_
        //                  (we leave one slot EMPTY to distinguish full from empty)
        //
        // WHY LEAVE ONE SLOT EMPTY?
        // If we fill all Capacity slots, (tail_ + 1) wraps to == head_.
        // But that's also the EMPTY condition (head_ == tail_).
        // We'd be unable to tell full from empty.
        // Solution: queue is "full" when there are only Capacity-1 items.
        // We sacrifice one slot for correctness.
        // ========================================================================
        SPSCQueue() : head_(0), tail_(0) {}

        // Prevent copying — a ring buffer has internal state that can't be copied safely.
        // If you copied an SPSCQueue mid-operation, both copies would share the same
        // conceptual "in-flight" data but disagree about head_/tail_ positions.
        SPSCQueue(const SPSCQueue &) = delete;
        SPSCQueue &operator=(const SPSCQueue &) = delete;

        // ========================================================================
        // try_push() — Called EXCLUSIVELY by the Producer thread
        // ========================================================================
        // RETURNS: true = item pushed successfully
        //          false = queue is full (caller should retry or handle backpressure)
        //
        // STEP BY STEP:
        // 1. Read current tail (we OWN tail — no other thread writes it)
        // 2. Compute next_tail = (tail + 1) & MASK  (wrap around at Capacity)
        // 3. Check if next_tail == head              (would mean queue is full)
        //    → Read head with memory_order_acquire   (see memory ordering note below)
        // 4. If not full: write item to buffer[tail]
        // 5. Publish tail update with memory_order_release
        //    → Consumer's acquire on tail_ sees buffer[tail] as fully written
        //
        // MEMORY ORDERING — The Core Correctness Guarantee:
        //
        //   Producer side: store(tail, release)
        //   Consumer side: load(tail, acquire)
        //
        //   "release" = all PREVIOUS writes are visible to any thread that subsequently
        //               does an "acquire" load of the same variable.
        //
        //   This guarantees:
        //     buffer[tail] = item           ← written BEFORE release store of tail
        //     tail_.store(next_tail, release)
        //     ...
        //     size_t t = tail_.load(acquire) ← Consumer sees tail updated
        //     item = buffer[t-1]             ← AND also sees buffer[t-1] written
        //
        //   Without this ordering: CPU or compiler might reorder the buffer write
        //   to AFTER the tail update → Consumer reads uninitialized buffer slot.
        //   That is a silent data corruption bug — the hardest kind to find.
        // ========================================================================
        [[nodiscard]]
        bool try_push(const T &item)
        {
            // Load our own tail — we are the exclusive writer of tail_.
            // memory_order_relaxed = no ordering constraints on THIS load.
            // WHY RELAXED HERE?
            // tail_ is only written by us (producer). We're just reading our own
            // last-written value — no synchronization with consumer needed here.
            const size_t tail = tail_.load(std::memory_order_relaxed);

            // Compute where the next item would land (with power-of-2 wrap)
            // MASK = Capacity - 1 = binary mask for fast modulo
            // Example: Capacity=8, MASK=7=0b111
            //   tail=7, next_tail = 8 & 7 = 0  (wraps to beginning)
            const size_t next_tail = (tail + 1) & MASK;

            // Check if queue is full by comparing next_tail to head
            // Consumer updates head_ after consuming — acquire sees all those writes.
            // WHY ACQUIRE here (not relaxed)?
            // We need to SEE the consumer's latest head_ update.
            // If we used relaxed, the CPU might use a stale cached value of head_
            // and think the queue is full when it isn't — or worse, think it's
            // not full when it is (buffer overwrite = silent corruption).
            if (next_tail == head_.load(std::memory_order_acquire))
            {
                return false; // Queue full — caller handles backpressure
            }

            // Write item to the slot at current tail
            // This write must complete BEFORE we update tail_ below.
            // The release store of tail_ (below) provides this guarantee.
            buffer_[tail] = item;

            // PUBLISH the new tail — makes the item visible to consumer.
            // memory_order_release = "all writes before this store are visible
            //                          to any thread that acquires this atomic"
            // Consumer's acquire-load of tail_ will see buffer_[tail] fully written.
            tail_.store(next_tail, std::memory_order_release);

            return true;
        }

        // Move-enabled push for efficiency with non-copyable types
        [[nodiscard]]
        bool try_push(T &&item)
        {
            const size_t tail = tail_.load(std::memory_order_relaxed);
            const size_t next_tail = (tail + 1) & MASK;

            if (next_tail == head_.load(std::memory_order_acquire))
                return false;

            // std::move transfers ownership — no copy for types like std::string
            buffer_[tail] = std::move(item);
            tail_.store(next_tail, std::memory_order_release);
            return true;
        }

        // ========================================================================
        // try_pop() — Called EXCLUSIVELY by the Consumer thread
        // ========================================================================
        // RETURNS: std::optional<T> — contains item if queue was non-empty,
        //                             std::nullopt if queue was empty
        //
        // WHY std::optional AND NOT A BOOL + OUT PARAMETER?
        // Optional is the idiomatic C++17 way to express "value OR nothing."
        // Compare:
        //   bool try_pop(T& out)       — old style, requires pre-existing variable
        //   std::optional<T> try_pop() — modern, composes cleanly with if/while
        //
        //   if (auto item = queue.try_pop()) {
        //       process(*item);  // 'item' is std::optional<T>, *item is T
        //   }
        // ========================================================================
        [[nodiscard]]
        std::optional<T> try_pop()
        {
            // Read our own head — we are the exclusive writer of head_.
            // Relaxed: we're reading our own last-written value, no sync needed.
            const size_t head = head_.load(std::memory_order_relaxed);

            // Check if queue is empty: empty when head == tail
            // WHY ACQUIRE here?
            // We need to see the producer's buffer write that happened BEFORE
            // the producer's release-store of tail_.
            // If we used relaxed: might see new tail but stale buffer contents.
            // That would read uninitialized/old data — silent corruption.
            if (head == tail_.load(std::memory_order_acquire))
            {
                return std::nullopt; // Queue empty
            }

            // Read the item from the slot at current head
            // This read happens AFTER the acquire-load of tail_ above,
            // which guarantees we see the producer's buffer_[head] write.
            T item = std::move(buffer_[head]);

            // PUBLISH the new head — makes this slot available for producer to reuse.
            // memory_order_release = "producer's acquire-load of head_ will see
            //                          that this slot is now free"
            // This is symmetric to the producer's release-store of tail_.
            head_.store((head + 1) & MASK, std::memory_order_release);

            return item;
        }

        // ========================================================================
        // Utility queries — approximate (can be stale by the time you use the value)
        // ========================================================================
        // WHY "APPROXIMATE"?
        // By the time size() returns, another thread may have pushed or popped.
        // The value is only instantaneously accurate. Don't make critical decisions
        // based on it — use the return values of try_push/try_pop instead.
        // ========================================================================
        [[nodiscard]]
        bool empty() const
        {
            return head_.load(std::memory_order_acquire) ==
                   tail_.load(std::memory_order_acquire);
        }

        [[nodiscard]]
        bool full() const
        {
            const size_t tail = tail_.load(std::memory_order_acquire);
            const size_t next_tail = (tail + 1) & MASK;
            return next_tail == head_.load(std::memory_order_acquire);
        }

        [[nodiscard]]
        size_t size() const
        {
            const size_t tail = tail_.load(std::memory_order_acquire);
            const size_t head = head_.load(std::memory_order_acquire);
            return (tail - head + Capacity) & MASK;
        }

        static constexpr size_t capacity() { return Capacity; }

    private:
        // ========================================================================
        // CACHE LINE ISOLATION — The false sharing prevention
        // ========================================================================
        // alignas(CACHE_LINE) = align this variable to the start of a cache line.
        // Then pad it to fill the rest of the cache line.
        //
        // Memory layout WITHOUT padding (bad):
        //   Offset 0:  head_    (8 bytes)
        //   Offset 8:  tail_    (8 bytes)
        //   Both fit in one 64-byte cache line → false sharing
        //
        // Memory layout WITH padding (correct):
        //   Offset 0:  head_    (8 bytes) ← START of cache line 1
        //   Offset 8:  padding  (56 bytes to fill rest of cache line)
        //   Offset 64: tail_    (8 bytes) ← START of cache line 2
        //   Offset 72: padding  (56 bytes to fill rest of cache line)
        //
        // Consumer reads/writes head_ → touches only cache line 1.
        // Producer reads/writes tail_ → touches only cache line 2.
        // NO CROSS-CONTAMINATION. Each core keeps its cache line exclusively warm.
        //
        // The char padding[CACHE_LINE - sizeof(std::atomic<size_t>)] fills the rest
        // of the cache line so the next variable starts on a NEW cache line boundary.
        // ========================================================================

        // Consumer's cache line — only the Consumer thread touches head_
        alignas(CACHE_LINE) std::atomic<size_t> head_;
        char pad_head_[CACHE_LINE - sizeof(std::atomic<size_t>)];

        // Producer's cache line — only the Producer thread touches tail_
        alignas(CACHE_LINE) std::atomic<size_t> tail_;
        char pad_tail_[CACHE_LINE - sizeof(std::atomic<size_t>)];

        // The actual ring buffer — stored inline (no heap allocation)
        //
        // WHY std::array AND NOT std::vector?
        // std::vector = heap allocation (malloc call at construction) + pointer indirection.
        // std::array  = inline storage inside the object (no heap, no pointer).
        //
        // For HFT: the ring buffer itself lives in one contiguous memory region.
        // Allocating it once at program start = deterministic, no allocator overhead.
        // std::array with a compile-time Capacity achieves exactly this.
        //
        // alignas(CACHE_LINE): start buffer_ on a cache line boundary.
        // Sequential access to buffer_[0], buffer_[1], ... maps well to
        // hardware prefetcher — CPU predicts we'll access the next elements
        // and fetches them into L1 cache before we need them.
        alignas(CACHE_LINE) std::array<T, Capacity> buffer_;

        // Fast modulo mask — Capacity must be power of 2 so this works
        // index % Capacity   = index & MASK (identical result, 10x faster)
        // 4096 % 4096 = 0    ↔   4096 & 4095 = 0 ✓
        // 4095 % 4096 = 4095 ↔   4095 & 4095 = 4095 ✓
        static constexpr size_t MASK = Capacity - 1;
    };

} // namespace MarketStream