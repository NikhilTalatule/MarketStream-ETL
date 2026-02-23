#pragma once

// ============================================================================
// SPSCQueueNoPadding — The WRONG version. No cache line isolation.
// ============================================================================
//
// THIS FILE EXISTS PURELY FOR BENCHMARKING.
// Do NOT use this in production code.
//
// This is SPSCQueue with exactly ONE change:
//   - No alignas(CACHE_LINE) on head_ or tail_
//   - No padding arrays between head_ and tail_
//
// Everything else is IDENTICAL: same atomic types, same acquire/release
// memory ordering, same ring buffer logic, same power-of-2 capacity.
//
// The ONLY difference is the physical memory address of head_ vs tail_.
//
// With padding (SPSCQueue):
//   head_ lives at address X       (cache line 1)
//   tail_ lives at address X + 64  (cache line 2, guaranteed by alignas)
//   → Two different cache lines. Producer owns line 2. Consumer owns line 1.
//
// Without padding (THIS FILE):
//   head_ lives at address X       (cache line 1, offset 0)
//   tail_ lives at address X + 8   (cache line 1, offset 8) ← SAME LINE
//   → Both on the same 64-byte cache line.
//
// WHAT HAPPENS WITHOUT PADDING — THE MESI PROTOCOL:
// ─────────────────────────────────────────────────────────────────────────────
// Modern CPUs maintain cache coherence using the MESI protocol:
//   M = Modified (I own this line, it's dirty)
//   E = Exclusive (I own this line, it's clean)
//   S = Shared    (multiple cores have a copy, all clean)
//   I = Invalid   (my copy is stale, must reload from RAM/L3)
//
// Timeline with NO PADDING (head_ and tail_ share a cache line):
//
//   Time 0: Both cores load the shared cache line into their L1 cache.
//
//   Time 1: Producer writes tail_ (at offset +8 on the shared line).
//           → Producer's cache line transitions: S → M (Modified)
//           → Producer broadcasts: "I modified address X, invalidate your copy"
//           → Consumer's cache line transitions: S → I (Invalid)
//
//   Time 2: Consumer tries to read tail_ (to check if queue has items).
//           → Consumer's copy is INVALID. Must reload from L3 or RAM.
//           → Cache miss penalty: 40-200 nanoseconds
//           → Consumer reloads the ENTIRE 64-byte line (both head_ AND tail_)
//           → Consumer's line transitions: I → S
//
//   Time 3: Consumer reads head_ (its own counter).
//           → head_ is now in cache (it came along with the reload of tail_).
//
//   Time 4: Consumer writes head_ (after popping an item).
//           → Consumer's cache line transitions: S → M (Modified)
//           → Consumer broadcasts: "I modified address X, invalidate your copy"
//           → Producer's cache line transitions: S → I (Invalid)
//
//   Time 5: Producer tries to read head_ (to check if queue is full).
//           → Producer's copy is INVALID. Must reload from L3.
//           → Another 40-200 nanosecond cache miss.
//
// RESULT: Every single push/pop causes a cache miss on both cores.
//         Two cores fighting over one 64-byte cache line = 40-200ns per op.
//
// With padding, each core owns its variable exclusively. No invalidation.
// Cache hit cost: ~1-4ns (L1 cache). This is the 10-50x difference.
// ─────────────────────────────────────────────────────────────────────────────

#include <atomic>
#include <array>
#include <optional>

namespace MarketStream
{

    template <typename T, size_t Capacity>
    class SPSCQueueNoPadding
    {
        static_assert((Capacity & (Capacity - 1)) == 0,
                      "SPSCQueueNoPadding capacity must be a power of 2");
        static_assert(Capacity >= 2, "SPSCQueueNoPadding capacity must be at least 2");

    public:
        SPSCQueueNoPadding() : head_(0), tail_(0) {}

        SPSCQueueNoPadding(const SPSCQueueNoPadding &) = delete;
        SPSCQueueNoPadding &operator=(const SPSCQueueNoPadding &) = delete;

        // =========================================================================
        // try_push — IDENTICAL logic to SPSCQueue::try_push.
        // The only difference is where head_ and tail_ live in memory.
        // =========================================================================
        [[nodiscard]]
        bool try_push(const T &item)
        {
            const size_t tail = tail_.load(std::memory_order_relaxed);
            const size_t next_tail = (tail + 1) & MASK;

            if (next_tail == head_.load(std::memory_order_acquire))
                return false;

            buffer_[tail] = item;
            tail_.store(next_tail, std::memory_order_release);
            return true;
        }

        [[nodiscard]]
        bool try_push(T &&item)
        {
            const size_t tail = tail_.load(std::memory_order_relaxed);
            const size_t next_tail = (tail + 1) & MASK;

            if (next_tail == head_.load(std::memory_order_acquire))
                return false;

            buffer_[tail] = std::move(item);
            tail_.store(next_tail, std::memory_order_release);
            return true;
        }

        // =========================================================================
        // try_pop — IDENTICAL logic to SPSCQueue::try_pop.
        // =========================================================================
        [[nodiscard]]
        std::optional<T> try_pop()
        {
            const size_t head = head_.load(std::memory_order_relaxed);

            if (head == tail_.load(std::memory_order_acquire))
                return std::nullopt;

            T item = std::move(buffer_[head]);
            head_.store((head + 1) & MASK, std::memory_order_release);
            return item;
        }

        [[nodiscard]]
        bool empty() const
        {
            return head_.load(std::memory_order_acquire) ==
                   tail_.load(std::memory_order_acquire);
        }

        static constexpr size_t capacity() { return Capacity; }

    private:
        // =========================================================================
        // THE CRITICAL DIFFERENCE FROM SPSCQueue:
        // =========================================================================
        //
        // SPSCQueue (CORRECT):
        //   alignas(64) std::atomic<size_t> head_;  // cache line 1, offset 0
        //   char pad_head_[56];                     // fills rest of cache line 1
        //   alignas(64) std::atomic<size_t> tail_;  // cache line 2, offset 64
        //   char pad_tail_[56];                     // fills rest of cache line 2
        //
        // SPSCQueueNoPadding (THIS — INTENTIONALLY WRONG):
        //   std::atomic<size_t> head_;  // offset 0  ─┐ SAME cache line
        //   std::atomic<size_t> tail_;  // offset 8  ─┘ (64 bytes wide)
        //
        // head_ and tail_ are 8 bytes each.
        // Both fit in the first 16 bytes of the struct's memory region.
        // One 64-byte cache line covers bytes 0-63.
        // Both variables are on that same line. False sharing guaranteed.
        // =========================================================================
        std::atomic<size_t> head_; // ← NO alignas. Adjacent to tail_.
        std::atomic<size_t> tail_; // ← Offset 8 bytes. SAME cache line as head_.

        // Buffer is large — starts well after the control variables.
        // It does NOT suffer false sharing (each slot is accessed by only one thread
        // at a time, same as the padded version).
        // The false sharing is EXCLUSIVELY between head_ and tail_.
        alignas(64) std::array<T, Capacity> buffer_;

        static constexpr size_t MASK = Capacity - 1;
    };

} // namespace MarketStream