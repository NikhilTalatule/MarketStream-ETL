// =============================================================================
// DatabaseLoader.cpp — All 5 methods at FLAT namespace scope
// =============================================================================
// WHY IS FLAT SCOPE IMPORTANT?
//
// In C++, a function definition must exist at namespace or class scope.
// You CANNOT define a function inside another function body.
//
//   ILLEGAL (what we had before):
//     void Foo::methodA() {
//         void Foo::methodB() { ... }   // ← compiler error / GCC extension abuse
//     }
//
//   LEGAL (what we have now):
//     void Foo::methodA() { ... }       // ← top-level definition
//     void Foo::methodB() { ... }       // ← separate top-level definition
//
// All 5 DatabaseLoader methods are defined here at flat scope:
//   1. DatabaseLoader()           — constructor
//   2. init_schema()              — creates tables + indexes
//   3. bulk_load()                — single-connection COPY (small datasets)
//   4. save_indicators()          — saves computed RSI/SMA/VWAP rows
//   5. prepare_for_parallel_load() — TRUNCATE + DROP PK (must run BEFORE threads)
//   6. copy_chunk()               — per-thread COPY stream (runs IN parallel)
//   7. finalize_parallel_load()   — REBUILD PK + index (runs AFTER all threads)
// =============================================================================

#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple>      // std::make_tuple — used to pass a row to pqxx::stream_to
#include <chrono>     // std::chrono::system_clock — for timestamping indicators
#include <span>       // std::span — C++20 zero-copy slice of a vector

namespace MarketStream
{

// =============================================================================
// CONSTRUCTOR
// =============================================================================
// WHY STORE A CONNECTION STRING INSTEAD OF A CONNECTION?
//
// pqxx::connection opens a TCP socket to PostgreSQL immediately.
// If we stored the connection as a member, ONE DatabaseLoader = ONE socket.
//
// For parallel loads, each thread creates its OWN DatabaseLoader instance.
// Each instance opens its OWN socket when it needs one (inside each method).
// This gives us N independent COPY streams for N threads.
//
// Storing just the string = cheap to construct, cheap to copy between threads.
// Opening the connection only when needed = "lazy connection" pattern.
// =============================================================================
DatabaseLoader::DatabaseLoader(const std::string& connection_string)
    : conn_str(connection_string)
{
    // No connection opened here intentionally.
    // Each method opens its own pqxx::connection locally.
    // When the method returns, the connection closes automatically (RAII).
}

// =============================================================================
// METHOD 1: init_schema()
// =============================================================================
// PURPOSE: Creates the 'trades' and 'technical_indicators' tables if they
//          don't already exist, along with their indexes.
//
// CREATE TABLE IF NOT EXISTS:
//   The "IF NOT EXISTS" means: if the table already exists, do nothing.
//   Without it: running the pipeline twice would throw "table already exists".
//   With it: idempotent — safe to call as many times as you want.
//
// WHY TWO SEPARATE TABLES?
//   trades               = raw event log (append-only, immutable)
//   technical_indicators = derived analytics (computed from trades)
//   Separating them follows the Single Responsibility Principle.
//   You can query indicators without touching the huge trades table.
//
// pqxx::work W(C):
//   Opens a transaction on connection C.
//   All W.exec() calls are part of this transaction.
//   W.commit() makes them permanent. If we never commit (e.g., exception),
//   PostgreSQL rolls everything back automatically. Safe by default.
//
// R"(...)":
//   Raw string literal — no need to escape quotes or newlines inside.
//   Everything between R"( and )" is treated as literal text.
//   This makes multi-line SQL readable.
// =============================================================================
void DatabaseLoader::init_schema()
{
    try
    {
        pqxx::connection C(conn_str);  // Opens TCP connection to PostgreSQL
        pqxx::work W(C);               // Starts a transaction

        // Create trades table
        // BIGINT         = 8-byte integer (64-bit). Fits all exchange trade IDs.
        // DOUBLE PRECISION = 64-bit floating point. Standard for prices.
        // CHECK (price > 0) = constraint: PostgreSQL rejects any row with price <= 0
        // CHAR(1) = exactly 1 character. Perfect for side ('B'/'S') and type ('M'/'L'/'I').
        W.exec(R"(
            CREATE TABLE IF NOT EXISTS trades (
                trade_id  BIGINT           PRIMARY KEY,
                order_id  BIGINT           NOT NULL,
                timestamp BIGINT           NOT NULL,
                symbol    VARCHAR(10)      NOT NULL,
                price     DOUBLE PRECISION NOT NULL CHECK (price > 0),
                volume    INTEGER          NOT NULL CHECK (volume > 0),
                side      CHAR(1)          NOT NULL CHECK (side IN ('B','S','N')),
                type      CHAR(1)          NOT NULL CHECK (type IN ('M','L','I')),
                is_pro    BOOLEAN          NOT NULL
            );
        )");

        // Composite index on (symbol, timestamp):
        // WHY COMPOSITE AND NOT SEPARATE INDEXES?
        // "Give me all RELIANCE trades between 9:15am and 9:30am"
        // This query filters by BOTH symbol AND timestamp.
        // A composite index (symbol, timestamp) satisfies this in ONE index scan.
        // Two separate indexes would require a more expensive "index merge".
        // Composite indexes work for: WHERE symbol='X', WHERE symbol='X' AND timestamp>Y.
        // They do NOT help for: WHERE timestamp>Y (only — must lead with the left column).
        W.exec(R"(
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
            ON trades (symbol, timestamp);
        )");

        // Create technical_indicators table
        // BIGSERIAL = auto-incrementing 64-bit integer. PostgreSQL assigns it automatically.
        // computed_at BIGINT = nanoseconds since epoch — when we ran the pipeline.
        //   This is the "append-only log" pattern: each run adds new rows.
        //   Never UPDATE, never DELETE. Always INSERT.
        //   This lets you answer: "What was RELIANCE RSI at 10:30am yesterday?"
        W.exec(R"(
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id          BIGSERIAL        PRIMARY KEY,
                symbol      VARCHAR(10)      NOT NULL,
                computed_at BIGINT           NOT NULL,
                sma         DOUBLE PRECISION NOT NULL,
                rsi         DOUBLE PRECISION NOT NULL CHECK (rsi >= 0 AND rsi <= 100),
                vwap        DOUBLE PRECISION NOT NULL CHECK (vwap > 0),
                period      INTEGER          NOT NULL CHECK (period > 0)
            );
        )");

        W.exec(R"(
            CREATE INDEX IF NOT EXISTS idx_indicators_symbol_time
            ON technical_indicators (symbol, computed_at);
        )");

        W.commit();  // Makes all the above permanent in the database
        std::cout << "[DB] Schema initialized (tables: trades, technical_indicators).\n";
    }
    catch (const std::exception& e)
    {
        // std::exception is the base class for ALL C++ exceptions.
        // e.what() returns a human-readable description of what went wrong.
        std::cerr << "[DB ERROR] Init Schema failed: " << e.what() << "\n";
        throw;  // Re-throw: let main() handle it (it will print [CRITICAL ERROR])
    }
}

// =============================================================================
// METHOD 2: bulk_load()
// =============================================================================
// PURPOSE: Loads trades into PostgreSQL using the COPY protocol.
//          This is the SINGLE-THREAD version. Use for < 100K rows or
//          incremental loads (adding new trades to an existing table).
//
// WHY COPY INSTEAD OF INSERT?
//   INSERT INTO trades VALUES (...) — 1 SQL round-trip per row
//   COPY protocol                  — 1 TCP stream for all rows
//
//   For 1M rows: INSERT = 1M round-trips = ~100 seconds
//                COPY   = 1 streaming TCP connection = ~4 seconds
//
// THE DROP + RELOAD + REBUILD PATTERN:
//   1. DROP PRIMARY KEY         → table has no index → COPY is pure disk writes
//   2. COPY all rows            → maximum throughput
//   3. ADD PRIMARY KEY          → one O(N log N) sort + one B-tree build
//
//   Why is this faster than COPY with the index present?
//   With index: each COPY row triggers a B-tree lookup + insert = O(log N) per row
//   Total: O(N log N) scattered random I/O = slow (cache misses everywhere)
//   Without index then rebuild: O(N log N) sorted sequential I/O = fast
//   Same complexity, but sequential access is 10-100x faster than random.
// =============================================================================
void DatabaseLoader::bulk_load(const std::vector<Trade>& trades)
{
    if (trades.empty())
    {
        std::cout << "[DB] No trades to load.\n";
        return;
    }

    try
    {
        pqxx::connection C(conn_str);
        pqxx::work W(C);

        // Drop the primary key constraint (which also drops the underlying index)
        // IF EXISTS = don't error if it was already dropped
        W.exec("ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_pkey");
        W.exec("DROP INDEX IF EXISTS idx_trades_symbol_time");

        // pqxx::stream_to::table() opens a PostgreSQL COPY FROM STDIN stream.
        // Data flows: our C++ loop → TCP socket → PostgreSQL → table pages on disk.
        // No SQL parsing. No row-level transaction overhead. Pure data transfer.
        //
        // The column list tells PostgreSQL which columns we're providing and in what order.
        // They MUST match what we push with <<.
        auto stream = pqxx::stream_to::table(
            W,
            {"trades"},
            {"trade_id", "order_id", "timestamp", "symbol",
             "price", "volume", "side", "type", "is_pro"}
        );

        // Loop through every Trade struct and serialize each field into the COPY stream.
        // std::make_tuple() packages the fields as a typed tuple — pqxx knows how to
        // serialize each type (int64→text, double→text, bool→t/f, etc.) for COPY format.
        //
        // WHY static_cast<int>(t.volume)?
        // t.volume is uint32_t. PostgreSQL's COPY TEXT format for INTEGER expects
        // a signed value. Casting to int keeps the value the same (volumes are small
        // enough to fit in int) but makes the type match PostgreSQL's expectation.
        //
        // WHY std::string(1, t.side)?
        // t.side is a char. COPY TEXT format wants a string, not a raw char.
        // std::string(1, 'B') = a string containing exactly one character 'B'.
        for (const auto& t : trades)
        {
            stream << std::make_tuple(
                t.trade_id,
                t.order_id,
                t.timestamp,
                t.symbol,
                t.price,
                static_cast<int>(t.volume),
                std::string(1, t.side),
                std::string(1, t.type),
                t.is_pro
            );
        }

        // complete() signals "end of COPY data" to PostgreSQL.
        // PostgreSQL writes all buffered rows to the table file.
        // Without complete(): rows stay in PostgreSQL's buffer, never written.
        stream.complete();

        // Rebuild the primary key after all data is loaded.
        // PostgreSQL now:
        //   1. Scans all 1M trade_id values in one pass
        //   2. Sorts them (O(N log N), but sequential memory = cache-friendly)
        //   3. Verifies uniqueness (easy — sorted means duplicates are adjacent)
        //   4. Builds B-tree bottom-up (pages filled to ~90% vs ~50% for incremental)
        std::cout << "[DB] COPY complete. Rebuilding indexes...\n";
        W.exec("ALTER TABLE trades ADD PRIMARY KEY (trade_id)");
        W.exec("CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, timestamp)");

        W.commit();
        std::cout << "[DB] Trades load complete.\n";
        std::cout << "[DB]   Inserted : " << trades.size() << " new trades\n";
    }
    catch (const std::exception& e)
    {
        std::cerr << "[DB ERROR] Bulk load failed: " << e.what() << "\n";
        throw;
    }
}

// =============================================================================
// METHOD 3: save_indicators()
// =============================================================================
// PURPOSE: Saves the computed technical indicators (RSI, SMA, VWAP per symbol)
//          into the technical_indicators table.
//
// WHY PARAMETERIZED INSERT INSTEAD OF COPY HERE?
//   We only have 6-10 rows (one per symbol). COPY has setup overhead —
//   opening a stream, sending COPY commands, etc. For 10 rows, a normal
//   parameterized INSERT is simpler and just as fast.
//
// WHY PARAMETERIZED ($1, $2, ...) INSTEAD OF STRING CONCATENATION?
//   SQL injection prevention. If we built the SQL as a string:
//     "INSERT INTO ... VALUES ('" + ind.symbol + "', ...)"
//   A malicious symbol like "'; DROP TABLE trades; --" would execute as SQL.
//   Parameterized queries send data and SQL separately — the database never
//   confuses data with commands.
//   In finance: your data comes from exchanges. Always treat it as untrusted.
//
// now_ns:
//   We timestamp each indicator row with the current wall-clock time
//   in nanoseconds. This is the "computed_at" column.
//   It lets you answer: "What were indicators as of the 9:30am run?"
//   by querying WHERE computed_at BETWEEN [9:30am_ns] AND [9:31am_ns].
// =============================================================================
void DatabaseLoader::save_indicators(const std::vector<IndicatorResult>& indicators)
{
    if (indicators.empty())
    {
        std::cout << "[DB] No indicators to save.\n";
        return;
    }

    try
    {
        pqxx::connection C(conn_str);
        pqxx::work W(C);

        // Get current time as nanoseconds since Unix epoch (Jan 1, 1970).
        // WHY NANOSECONDS?
        // PostgreSQL TIMESTAMP has microsecond precision, but we want to stay
        // consistent with our trades.timestamp column (also nanoseconds).
        // Storing as a raw BIGINT also avoids timezone conversion headaches.
        auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        for (const auto& ind : indicators)
        {
            // W.exec(sql, params):
            //   $1 = first parameter (ind.symbol)
            //   $2 = second parameter (now_ns)
            //   etc. PostgreSQL substitutes them safely.
            W.exec(
                "INSERT INTO technical_indicators "
                "(symbol, computed_at, sma, rsi, vwap, period) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                pqxx::params{ind.symbol, now_ns, ind.sma, ind.rsi, ind.vwap, ind.period}
            );
        }

        W.commit();
        std::cout << "[DB] Saved " << indicators.size()
                  << " indicator rows to technical_indicators.\n";
    }
    catch (const std::exception& e)
    {
        std::cerr << "[DB ERROR] save_indicators failed: " << e.what() << "\n";
        throw;
    }
}

// =============================================================================
// METHOD 4: prepare_for_parallel_load()
// =============================================================================
// PURPOSE: This is STEP 1 of the 3-step parallel load sequence.
//          Must be called ONCE on the MAIN THREAD before launching workers.
//
// WHAT IT DOES:
//   1. TRUNCATE TABLE trades   — wipe all existing rows instantly
//   2. DROP PRIMARY KEY        — remove the B-tree index
//   3. DROP composite index    — remove the symbol+timestamp index
//
// WHY TRUNCATE AND NOT DELETE?
//   DELETE FROM trades  = scans every row, deletes one by one, updates indexes
//                       = O(N) with heavy I/O. For 1M rows: seconds.
//   TRUNCATE TABLE trades = deallocates entire data pages in one OS operation
//                         = O(1) effectively. Milliseconds regardless of size.
//   TRUNCATE also resets the table to zero pages, so subsequent COPY writes
//   fill pages sequentially from the beginning — no fragmentation.
//
// WHY DROP INDEXES BEFORE PARALLEL COPY?
//   With index present: each COPY row triggers 1 B-tree page lookup + insert.
//   4 threads × 250K rows = 1M individual index updates.
//   Each update is a random I/O (cache misses) = catastrophically slow.
//
//   Without index: COPY is pure sequential disk writes at full disk speed.
//   We rebuild the index ONCE afterward in finalize_parallel_load().
//   One O(N log N) sequential sort >> 1M random B-tree insertions.
//
// WHY MUST THIS BE SEQUENTIAL (not parallel)?
//   ALTER TABLE acquires ACCESS EXCLUSIVE LOCK — the strongest PostgreSQL lock.
//   While this lock is held, NO other query can even read the table.
//   If two connections tried to ALTER simultaneously, one would block forever.
//   This is why we call prepare on the main thread before spawning workers.
// =============================================================================
void DatabaseLoader::prepare_for_parallel_load()
{
    try
    {
        pqxx::connection C(conn_str);
        pqxx::work W(C);

        // TRUNCATE: wipe table instantly. Faster than DELETE for large tables.
        // No CASCADE needed — no other tables have FOREIGN KEY references to trades.
        // No RESTART IDENTITY needed — trade_id is externally assigned (from exchange).
        W.exec("TRUNCATE TABLE trades");

        // DROP PRIMARY KEY: removes the B-tree index on trade_id.
        // "IF EXISTS" = don't error if it was already dropped by a previous run.
        W.exec("ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_pkey");

        // DROP the composite index as well.
        W.exec("DROP INDEX IF EXISTS idx_trades_symbol_time");

        W.commit();
        std::cout << "[PARALLEL-LOAD] Table truncated. Constraints dropped. Ready for parallel COPY.\n";
    }
    catch (const std::exception& e)
    {
        std::cerr << "[DB ERROR] prepare_for_parallel_load failed: " << e.what() << "\n";
        throw;
    }
}

// =============================================================================
// METHOD 5: copy_chunk()
// =============================================================================
// PURPOSE: This is STEP 2 of the 3-step parallel load sequence.
//          Called by EACH WORKER THREAD simultaneously.
//          Each call opens its OWN connection and COPY stream.
//
// PARAMETER: std::span<const Trade> chunk
//   std::span is a C++20 "non-owning view" over a contiguous range of data.
//   Think of it as a (pointer, length) pair — just 16 bytes on 64-bit systems.
//   The trades data lives in main()'s vector. The span just POINTS to a slice.
//   NO memory is copied. The word "const" means we can read but not modify.
//
//   Why span and not a pair of iterators?
//   span is self-documenting: "here is a contiguous slice of Trades."
//   It's the C++20 idiomatic way to pass "a slice of an array."
//
// PARAMETER: int thread_id
//   Used only for logging — tells us which thread printed which message.
//
// THREAD SAFETY:
//   Each thread calls this with its OWN DatabaseLoader instance.
//   Each DatabaseLoader opens its OWN pqxx::connection (OWN TCP socket).
//   Each connection has its OWN pqxx::work transaction.
//   PostgreSQL's MVCC handles 4 concurrent COPY streams to the same table:
//   each transaction sees its own snapshot, rows don't conflict (different IDs).
//   ZERO shared mutable state between threads = ZERO data races.
//
// WHY DOES THIS WORK WITHOUT LOCKS?
//   The trades vector is READ-ONLY (const span<const Trade>).
//   Multiple threads reading the same memory simultaneously = always safe.
//   Only writes cause races. We never write to shared memory here.
// =============================================================================
void DatabaseLoader::copy_chunk(std::span<const Trade> chunk, int thread_id)
{
    if (chunk.empty()) return;

    try
    {
        // Each thread opens its OWN connection.
        // This is the key to parallelism: 4 threads = 4 TCP sockets = 4 COPY pipes.
        // PostgreSQL processes them concurrently on its own worker threads.
        pqxx::connection C(conn_str);
        pqxx::work W(C);

        // Open a COPY stream to the trades table.
        // This sends the PostgreSQL COPY FROM STDIN command to the server.
        // From this point, data flows: our loop → socket → PostgreSQL → disk.
        auto stream = pqxx::stream_to::table(
            W,
            {"trades"},
            {"trade_id", "order_id", "timestamp", "symbol",
             "price", "volume", "side", "type", "is_pro"}
        );

        // Iterate over our slice of trades.
        // chunk[0] through chunk[chunk.size()-1].
        // The data physically lives in main's vector — span is just a window.
        for (const auto& t : chunk)
        {
            stream << std::make_tuple(
                t.trade_id,
                t.order_id,
                t.timestamp,
                t.symbol,
                t.price,
                static_cast<int>(t.volume),
                std::string(1, t.side),
                std::string(1, t.type),
                t.is_pro
            );
        }

        // complete() flushes the COPY buffer and signals end-of-data to PostgreSQL.
        // PostgreSQL writes all buffered rows to the table pages on disk.
        stream.complete();

        // Commit this thread's transaction.
        // This makes the chunk's rows VISIBLE to future queries.
        // Before commit: rows exist in PostgreSQL's memory but aren't durable.
        W.commit();
    }
    catch (const std::exception& e)
    {
        // Re-throw: the ThreadPool captures this via std::packaged_task.
        // When main() calls future.get(), this exception re-surfaces there.
        // This is why std::async / packaged_task is superior to raw std::thread —
        // exceptions propagate automatically across thread boundaries.
        std::cerr << "[DB ERROR] copy_chunk (thread " << thread_id
                  << ") failed: " << e.what() << "\n";
        throw;
    }
}

// =============================================================================
// METHOD 6: finalize_parallel_load()
// =============================================================================
// PURPOSE: This is STEP 3 of the 3-step parallel load sequence.
//          Called ONCE on the MAIN THREAD after all worker threads finish.
//          Rebuilds PRIMARY KEY and composite index over the loaded data.
//
// PARAMETER: size_t total_rows
//   Only used for logging. Tells us how many rows to expect.
//
// HOW PostgreSQL REBUILDS THE PRIMARY KEY INTERNALLY:
//   1. Sequential scan of all N trade_id values (one pass, cache-friendly)
//   2. External merge sort of the values (O(N log N), sorted in temp files)
//   3. Uniqueness check (trivial — adjacent duplicates visible after sort)
//   4. Bottom-up B-tree construction from sorted data:
//      → Leaf pages filled left-to-right at ~90% capacity
//      → Internal pages built after leaves are placed
//      → Result: compact, optimally filled B-tree
//
// WHY IS THIS DRAMATICALLY FASTER THAN INCREMENTAL BUILD?
//   Incremental (COPY with index present):
//     Each row: traverse B-tree from root to leaf (random I/O, cache misses)
//               potentially split a page (expensive, causes rebalancing)
//               write WAL record per insert (write amplification)
//     For 1M rows: 1M random I/O operations
//
//   Batch rebuild (what we do):
//     One sequential scan + one sequential sort + one sequential write
//     CPU hardware prefetcher loves sequential access — stays in L2 cache
//     Result: 5-10x faster, smaller index file, better cache utilization for queries
//
// THIS IS EXACTLY HOW pg_restore -j N WORKS.
// Same principle used by pgloader, pg_bulkload, and every serious ETL tool.
//
// WHY MUST THIS BE SEQUENTIAL?
//   Same reason as prepare: ACCESS EXCLUSIVE lock on ALTER TABLE.
//   Only one connection can rebuild the primary key at a time.
//   We call this on the main thread after all workers have joined.
// =============================================================================
void DatabaseLoader::finalize_parallel_load(size_t total_rows)
{
    try
    {
        pqxx::connection C(conn_str);
        pqxx::work W(C);

        // This is the slow step. It does the sort + uniqueness check + B-tree build.
        // Expected time for 1M rows: 1-3 seconds. Normal and expected.
        std::cout << "[DB] Building PRIMARY KEY index over " << total_rows << " rows...\n";
        W.exec("ALTER TABLE trades ADD PRIMARY KEY (trade_id)");

        // Rebuild the composite index for query performance.
        // "Give me all RELIANCE trades after 10:30am" — this index makes it instant.
        std::cout << "[DB] Building composite index (symbol, timestamp)...\n";
        W.exec(R"(
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
            ON trades (symbol, timestamp)
        )");

        W.commit();

        std::cout << "[DB] Constraints rebuilt. Load finalized.\n";
        std::cout << "[DB]   Total rows : " << total_rows << "\n";
    }
    catch (const std::exception& e)
    {
        std::cerr << "[DB ERROR] finalize_parallel_load failed: " << e.what() << "\n";
        throw;
    }
}

} // namespace MarketStream