#pragma once

// ============================================================================
// ThreadPool — A production-grade, generic thread pool
// ============================================================================
//
// WHY BUILD A CUSTOM THREAD POOL INSTEAD OF USING std::async?
//
// std::async is great for "run this one task in the background."
// But it has a critical limitation: every call to std::async MAY create
// a brand new OS thread. Creating a thread costs ~50–100 microseconds
// (the OS must allocate a stack, register the thread, schedule it).
//
// A thread POOL creates threads ONCE at startup, then reuses them.
// For a pipeline that runs 1,000 times per day:
//   std::async: 1,000 × thread creation cost = seconds of overhead
//   ThreadPool: 1 × thread creation cost + 999 × task submission = microseconds
//
// This is exactly why Citadel, HRT, and all HFT firms use thread pools —
// not raw threads — for recurring tasks.
//
// ARCHITECTURE:
//
//   Main Thread                     Worker Threads (N)
//   ───────────                     ──────────────────
//   submit(task_A) ──┐              Worker 0: sleeping...
//   submit(task_B) ──┤──> queue ──> Worker 1: sleeping...
//   submit(task_C) ──┤              Worker 2: sleeping...
//   submit(task_D) ──┘              Worker 3: sleeping...
//                                        │
//                    condition_variable.notify() wakes a worker
//                                        │
//                                   Worker 0: executes task_A
//                                   Worker 1: executes task_B
//                                   Worker 2: executes task_C
//                                   Worker 3: executes task_D
//   wait_all() ──────────────────────────┘
//   (main blocks here until all workers done)
//
// KEY SYNCHRONIZATION PRIMITIVES:
//
//   std::mutex        — A lock. Only one thread can hold it at a time.
//                       Protects shared data (the task queue) from
//                       simultaneous access by multiple threads.
//                       Without mutex: two threads grab the same task = crash.
//
//   condition_variable — A signaling mechanism. Allows a thread to SLEEP
//                        (releasing the CPU) until another thread wakes it.
//                        Without this: workers would spin in a busy loop
//                        burning 100% CPU while waiting for work.
//                        With this: workers sleep, consuming 0% CPU when idle.
//
//   std::packaged_task — Wraps any callable (function, lambda) so its
//                        return value and exceptions can be retrieved later
//                        via a std::future. This is how we propagate errors
//                        from worker threads back to the main thread.
// ============================================================================

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <stdexcept>
#include <memory>
#include <iostream>
#include <iomanip>
#include <chrono>

namespace MarketStream
{

    class ThreadPool
    {
    public:
        // ====================================================================
        // Constructor — Creates and starts N worker threads
        // ====================================================================
        // WHY explicit?
        // 'explicit' prevents accidental implicit conversion.
        // Without it: ThreadPool pool = 4; would compile (converting int to pool).
        // With it: must write ThreadPool pool(4); — more readable, less surprising.
        // ====================================================================
        explicit ThreadPool(size_t num_threads)
            : active_tasks_(0), shutdown_(false)
        {
            // Reserve space — we know exactly how many threads we'll create.
            // Avoids reallocation when push_back triggers a vector resize
            // (which would move existing thread objects = undefined behavior).
            workers_.reserve(num_threads);

            for (size_t i = 0; i < num_threads; ++i)
            {
                // std::thread constructor takes a callable.
                // We pass a member function pointer + 'this' (the pool itself).
                // Each worker thread runs worker_loop() indefinitely until shutdown.
                //
                // WHY emplace_back INSTEAD OF push_back?
                // emplace_back constructs the thread IN PLACE inside the vector.
                // push_back would construct it, then move it — one extra operation.
                workers_.emplace_back(&ThreadPool::worker_loop, this);
            }
        }

        // ====================================================================
        // Destructor — Shuts down all worker threads cleanly
        // ====================================================================
        // WHY IS CLEAN SHUTDOWN CRITICAL?
        // If we just destroy the pool while workers are running:
        //   - Workers access queue_ which is already destroyed = undefined behavior
        //   - std::thread destructor calls std::terminate() if thread is joinable
        //     = your entire process crashes with no error message
        //
        // Clean shutdown:
        //   1. Set shutdown_ = true (workers will see this on next wake)
        //   2. notify_all() — wake ALL sleeping workers (they'll check shutdown_)
        //   3. join() each worker — wait for it to finish its current task and exit
        //
        // This guarantees: no tasks are abandoned, no memory is corrupted.
        // ====================================================================
        ~ThreadPool()
        {
            {
                // Lock before modifying shutdown_ — workers read it under the same lock
                std::lock_guard<std::mutex> lock(queue_mutex_);
                shutdown_ = true;
            }

            // Wake ALL workers — they'll check shutdown_ and exit their loop
            task_cv_.notify_all();

            // Wait for each worker to finish its current task and exit
            // join() = "I will block here until this thread exits"
            // Without join(): destructor returns, stack unwinds, workers crash
            for (auto &worker : workers_)
            {
                if (worker.joinable())
                    worker.join();
            }
        }

        // ====================================================================
        // submit() — Enqueue a task and return a future for its result
        // ====================================================================
        // HOW IT WORKS:
        //   auto future = pool.submit([]() -> long long {
        //       return do_some_work();
        //   });
        //   // ... main thread continues doing other things ...
        //   long long result = future.get(); // blocks until task completes
        //
        // TEMPLATE PARAMETERS:
        //   F    — the callable type (lambda, function pointer, std::function...)
        //   Args — argument types to forward to the callable
        //
        // std::invoke_result_t<F, Args...>
        //   — determines the return type of F(Args...) at compile time
        //   — lets us declare the future<T> with the correct T automatically
        //
        // std::packaged_task<ReturnType()>
        //   — wraps the callable so its result can be retrieved via future
        //   — if the callable throws, the exception is stored and re-thrown
        //     when future.get() is called on the main thread
        //
        // std::shared_ptr<packaged_task>
        //   — WHY SHARED_PTR? packaged_task is NOT copyable (futures are unique).
        //     The lambda stored in the queue needs to capture it by value.
        //     shared_ptr IS copyable. So we wrap the task in a shared_ptr,
        //     and the lambda captures the shared_ptr by value = safe copy.
        // ====================================================================
        template <typename F, typename... Args>
        auto submit(F &&f, Args &&...args)
            -> std::future<std::invoke_result_t<F, Args...>>
        {
            // The actual return type of calling f(args...)
            using ReturnType = std::invoke_result_t<F, Args...>;

            // Package the task: wraps callable + stores result for future retrieval
            // std::bind creates a zero-argument callable from f + its arguments
            auto task = std::make_shared<std::packaged_task<ReturnType()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...));

            // Get the future BEFORE moving the task into the queue
            // (after moving, we can no longer access the task)
            std::future<ReturnType> future = task->get_future();

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);

                if (shutdown_)
                    throw std::runtime_error("[ThreadPool] Cannot submit to a shut-down pool");

                // Push a zero-argument lambda that calls the packaged_task
                // The lambda captures task by value (via shared_ptr copy)
                task_queue_.push([task]()
                                 { (*task)(); });

                // Increment BEFORE releasing lock — ensures wait_all() never
                // sees a "done" state between task submission and execution start
                ++active_tasks_;
            }

            // Wake ONE sleeping worker — it will pick up this task
            // notify_one (not notify_all) — no point waking workers that won't get work
            task_cv_.notify_one();

            return future;
        }

        // ====================================================================
        // wait_all() — Block main thread until all submitted tasks complete
        // ====================================================================
        // CONDITION TO WAIT FOR:
        //   task_queue_.empty()  → no tasks waiting to start
        //   AND
        //   active_tasks_ == 0   → no tasks currently executing
        //
        // WHY BOTH CONDITIONS?
        // If we only checked queue empty: a task could be dequeued but still
        // executing. We'd return before the work is done.
        // If we only checked active_tasks_ == 0: a task could be queued but
        // not yet picked up. Same problem.
        // Both together: truly done.
        //
        // done_cv_.wait(lock, predicate):
        //   1. Checks predicate — if true, returns immediately (done already)
        //   2. If false: releases lock, puts main thread to sleep
        //   3. When a worker calls done_cv_.notify_all(), main thread wakes
        //   4. Re-acquires lock, re-checks predicate (guards against spurious wakeups)
        //   5. If still false: sleeps again. If true: returns.
        // ====================================================================
        void wait_all()
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            done_cv_.wait(lock, [this]
                          { return task_queue_.empty() && active_tasks_ == 0; });
        }

        // Accessors for diagnostics
        size_t thread_count() const { return workers_.size(); }

        // Delete copy and move — a thread pool must not be copied or moved
        // (worker threads hold a pointer to 'this' — if we moved the pool,
        //  that pointer would dangle)
        ThreadPool(const ThreadPool &) = delete;
        ThreadPool &operator=(const ThreadPool &) = delete;
        ThreadPool(ThreadPool &&) = delete;
        ThreadPool &operator=(ThreadPool &&) = delete;

    private:
        // ====================================================================
        // worker_loop() — The function each worker thread runs indefinitely
        // ====================================================================
        // STATE MACHINE:
        //
        //   ┌─────────────┐
        //   │   SLEEPING  │ ← condition_variable::wait() — CPU cost = 0
        //   └──────┬──────┘
        //          │ notify_one() (new task OR shutdown)
        //          ▼
        //   ┌─────────────┐
        //   │   CHECKING  │ ← re-acquires mutex, checks predicates
        //   └──────┬──────┘
        //          │
        //    ┌─────┴─────┐
        //    │           │
        //  shutdown?    task?
        //    │           │
        //  EXIT       dequeue task
        //             release mutex
        //             EXECUTE task
        //             lock mutex
        //             --active_tasks_
        //             release mutex
        //             notify done_cv_
        //             │
        //             └──► back to SLEEPING
        // ====================================================================
        void worker_loop()
        {
            while (true)
            {
                std::function<void()> task;

                {
                    // Acquire mutex before checking queue or shutdown flag
                    std::unique_lock<std::mutex> lock(queue_mutex_);

                    // Sleep until: there's a task in the queue, OR we're shutting down
                    // WHY A LAMBDA PREDICATE?
                    // condition_variable can have "spurious wakeups" — waking for no reason.
                    // The predicate re-checks the condition after each wakeup.
                    // If the condition isn't actually true: go back to sleep.
                    // This is the correct pattern — never trust a wakeup blindly.
                    task_cv_.wait(lock, [this]
                                  { return shutdown_ || !task_queue_.empty(); });

                    // Exit condition: we're shutting down AND there are no tasks left
                    // (we DO process remaining tasks even during shutdown)
                    if (shutdown_ && task_queue_.empty())
                        return;

                    // Grab the next task from the front of the queue
                    // std::move transfers ownership of the function object = no copy
                    task = std::move(task_queue_.front());
                    task_queue_.pop();

                } // ← Mutex released here. CRITICAL: don't hold mutex while executing task.
                  // Holding it would block other workers from picking up tasks = no parallelism.

                // Execute the task OUTSIDE the lock
                task();

                // Task complete — decrement counter and notify wait_all()
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    --active_tasks_;
                }

                // Notify main thread (in wait_all) that a task completed
                // notify_all because wait_all uses a compound predicate —
                // we want it to re-check both conditions
                done_cv_.notify_all();
            }
        }

        // ──────────────────────────────────────────────────────────────────
        // Member variables — ORDER MATTERS for cache layout
        // ──────────────────────────────────────────────────────────────────

        std::vector<std::thread> workers_; // The actual OS threads

        std::queue<std::function<void()>> task_queue_; // Pending work
        std::mutex queue_mutex_;                       // Protects queue_ and active_tasks_
        std::condition_variable task_cv_;              // Workers sleep here
        std::condition_variable done_cv_;              // wait_all() sleeps here

        size_t active_tasks_; // Tasks dequeued but not yet complete (protected by queue_mutex_)
        bool shutdown_;       // Set to true in destructor (protected by queue_mutex_)
    };

} // namespace MarketStream