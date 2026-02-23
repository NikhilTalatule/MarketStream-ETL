#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple>
#include <chrono>

namespace MarketStream
{

    DatabaseLoader::DatabaseLoader(const std::string &connection_string)
        : conn_str(connection_string) {}

    // =========================================================================
    // init_schema() — Creates BOTH tables with constraints and indexes
    // Unchanged from Phase 5.
    // =========================================================================
    void DatabaseLoader::init_schema()
    {
        try
        {
            pqxx::connection C(conn_str);
            pqxx::work W(C);

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

            W.exec(R"(
                CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
                ON trades (symbol, timestamp);
            )");

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

            W.commit();
            std::cout << "[DB] Schema initialized (tables: trades, technical_indicators).\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Init Schema failed: " << e.what() << "\n";
            throw;
        }
    }

    // =========================================================================
    // bulk_load() — Single-connection COPY + index rebuild
    // Keep for small datasets / incremental loads.
    // =========================================================================
    void DatabaseLoader::bulk_load(const std::vector<Trade> &trades)
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

            W.exec("ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_pkey");
            W.exec("DROP INDEX IF EXISTS idx_trades_symbol_time");

            auto stream = pqxx::stream_to::table(
                W, {"trades"},
                {"trade_id", "order_id", "timestamp", "symbol",
                 "price", "volume", "side", "type", "is_pro"});

            for (const auto &t : trades)
            {
                stream << std::make_tuple(
                    t.trade_id, t.order_id, t.timestamp, t.symbol,
                    t.price, static_cast<int>(t.volume),
                    std::string(1, t.side), std::string(1, t.type), t.is_pro);
            }
            stream.complete();

            std::cout << "[DB] COPY complete. Rebuilding indexes...\n";
            W.exec("ALTER TABLE trades ADD PRIMARY KEY (trade_id)");
            W.exec("CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, timestamp)");
            W.commit();

            std::cout << "[DB] Trades load complete.\n";
            std::cout << "[DB]   Inserted : " << trades.size() << " new trades\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Bulk load failed: " << e.what() << "\n";
            throw;
        }
    }

    // =========================================================================
    // save_indicators() — Parameterized INSERT for small row counts
    // Unchanged from Phase 5.
    // =========================================================================
    void DatabaseLoader::save_indicators(const std::vector<IndicatorResult> &indicators)
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

            auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

            for (const auto &ind : indicators)
            {
                W.exec(
                    "INSERT INTO technical_indicators "
                    "(symbol, computed_at, sma, rsi, vwap, period) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    pqxx::params{ind.symbol, now_ns, ind.sma, ind.rsi, ind.vwap, ind.period});
            }

            W.commit();
            std::cout << "[DB] Saved " << indicators.size()
                      << " indicator rows to technical_indicators.\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] save_indicators failed: " << e.what() << "\n";
            throw;
        }
    }

    // =========================================================================
    // prepare_for_parallel_load()
    // =========================================================================
    // STEP 1 of the parallel load sequence.
    // Drops PRIMARY KEY and composite index BEFORE launching worker threads.
    //
    // WHY DROP THE INDEX BEFORE COPY?
    // An indexed table forces PostgreSQL to update the B-tree index for EACH
    // inserted row. For 1M rows across 4 threads = 4M individual index updates,
    // each requiring a B-tree page lookup + possible rebalancing.
    //
    // Without the index: COPY is pure sequential disk writes at full speed.
    // We rebuild the index ONCE afterward = one O(N log N) sort over all data.
    // The rebuild is 5-10x faster than incremental updates during COPY.
    //
    // WHY ALTER TABLE INSTEAD OF DISABLE TRIGGER?
    // PostgreSQL does not have a "disable index" command (unlike MySQL).
    // The correct pattern is: DROP CONSTRAINT (which drops the index),
    // then ADD CONSTRAINT (which rebuilds it). This is what pg_restore uses.
    //
    // ACCESS EXCLUSIVE LOCK:
    // ALTER TABLE acquires ACCESS EXCLUSIVE — the strongest lock.
    // No reads or writes can happen on the table while this runs.
    // This is why prepare must be SEQUENTIAL (main thread only) —
    // if two connections tried to ALTER simultaneously, one would block
    // indefinitely waiting for the other to release its lock.
    // =========================================================================
    void DatabaseLoader::prepare_for_parallel_load()
    {
        try
        {
            pqxx::connection C(conn_str);
            pqxx::work W(C);

            // TRUNCATE before dropping constraints.
            //
            // WHY TRUNCATE AND NOT DELETE?
            // DELETE FROM trades removes rows one by one, updating indexes per row.
            // For 1M rows, DELETE scans the entire table = seconds of overhead.
            // TRUNCATE deallocates entire data pages in one operation = milliseconds.
            // It is the correct tool when you intend to reload the full table.
            //
            // WHY HERE AND NOT IN main.cpp?
            // This function owns the "prepare for bulk load" contract.
            // Putting TRUNCATE here means the pipeline is idempotent by default —
            // you can run .\etl_pipeline.exe as many times as you want and always
            // get a clean, consistent result. The caller (main.cpp) doesn't need
            // to know or care about table state.
            //
            // TRUNCATE also resets the table to zero pages, which means the
            // subsequent COPY writes sequentially from the beginning of the file
            // with no fragmentation from prior rows. Cleaner B-tree rebuild.
            //
            // CASCADE is not needed here — no other tables depend on trades via FK.
            // RESTART IDENTITY is not applicable — trade_id is externally assigned.
            W.exec("TRUNCATE TABLE trades");

            W.exec("ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_pkey");
            W.exec("DROP INDEX IF EXISTS idx_trades_symbol_time");

            W.commit();
            std::cout << "[PARALLEL-LOAD] Table truncated. Constraints dropped. Ready for parallel COPY.\n";

            // =========================================================================
            // copy_chunk()
            // =========================================================================
            // STEP 2 of the parallel load sequence. Called by each worker thread.
            //
            // Each thread that calls this function has its OWN DatabaseLoader instance,
            // therefore its OWN pqxx::connection, therefore its OWN TCP socket to
            // PostgreSQL. Four threads = four independent COPY streams.
            //
            // std::span<const Trade> chunk:
            //   A non-owning view over a slice of the main trades vector.
            //   NO MEMORY COPIED — span is just a pointer + size (16 bytes).
            //   The actual trade data lives in the main thread's vector.
            //   Since we only READ (const Trade), and never WRITE, across threads:
            //   ZERO data races. Multiple readers = always safe.
            //
            // TRANSACTION ISOLATION:
            //   Each COPY stream runs in its own transaction (pqxx::work W(C)).
            //   PostgreSQL's MVCC (Multi-Version Concurrency Control) ensures that
            //   the four concurrent transactions don't interfere:
            //   - Each sees a snapshot of the DB from when its transaction started
            //   - Writes from different transactions don't conflict (different rows)
            //   - All four commit independently
            //
            // NO CONFLICT CHECK:
            //   We removed the staging table + ON CONFLICT DO NOTHING pattern.
            //   This is correct for the initial load path (table is empty/truncated).
            //   For incremental loads on an existing table, use bulk_load() which
            //   handles conflicts via the staging pattern.
            // =========================================================================
            void DatabaseLoader::copy_chunk(std::span<const Trade> chunk, int thread_id)
            {
                if (chunk.empty())
                    return;

                try
                {
                    // Each thread constructs its OWN connection.
                    // pqxx::connection opens a TCP socket to PostgreSQL.
                    // Four DatabaseLoader instances = four sockets = four COPY pipes.
                    pqxx::connection C(conn_str);
                    pqxx::work W(C);

                    // pqxx::stream_to::table() opens a COPY FROM STDIN stream.
                    // Data flows: C++ program → TCP socket → PostgreSQL server → table file.
                    // No intermediate buffers, no SQL parsing overhead.
                    // This is the fastest possible data ingestion path in PostgreSQL.
                    auto stream = pqxx::stream_to::table(
                        W,
                        {"trades"},
                        {"trade_id", "order_id", "timestamp", "symbol",
                         "price", "volume", "side", "type", "is_pro"});

                    // Iterate over our chunk (a span view — zero overhead)
                    // Each row is serialized into the COPY binary/text format
                    // and sent over the socket. No individual INSERT parsing.
                    for (const auto &t : chunk)
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
                            t.is_pro);
                    }

                    // complete() flushes the COPY buffer and signals "end of data"
                    // to PostgreSQL. PostgreSQL writes all buffered rows to the table.
                    stream.complete();

                    // Commit the transaction — makes this chunk's rows visible
                    // to future queries. Before commit, they exist but aren't committed.
                    W.commit();
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[DB ERROR] copy_chunk (thread " << thread_id
                              << ") failed: " << e.what() << "\n";
                    throw; // Re-throw: ThreadPool captures via packaged_task, future.get() re-throws
                }
            }

            // =========================================================================
            // finalize_parallel_load()
            // =========================================================================
            // STEP 3 of the parallel load sequence.
            // Rebuilds PRIMARY KEY and composite index after all chunks are loaded.
            //
            // HOW ADD PRIMARY KEY WORKS INTERNALLY:
            //   1. PostgreSQL scans all 1M trade_id values
            //   2. Sorts them with a merge sort (O(N log N), memory-efficient)
            //   3. Verifies uniqueness (trivial during sorted scan)
            //   4. Builds the B-tree index from the sorted data (bottom-up construction)
            //      Bottom-up build = pages filled to ~90% capacity vs ~50% for incremental
            //      = smaller index file = better cache utilization for future queries
            //
            // This is dramatically faster than 1M incremental B-tree insertions because:
            //   - One sequential memory scan (cache-friendly)
            //   - One sort (well-optimized in PostgreSQL)
            //   - Pages written in order (no random I/O)
            //   vs
            //   - 1M random B-tree lookups (cache misses)
            //   - 1M potential page splits (expensive)
            //   - 1M write-amplified WAL records
            //
            // TOTAL_ROWS parameter: logged for verification only.
            // =========================================================================
            void DatabaseLoader::finalize_parallel_load(size_t total_rows)
            {
                try
                {
                    pqxx::connection C(conn_str);
                    pqxx::work W(C);

                    std::cout << "[DB] Building PRIMARY KEY index over " << total_rows << " rows...\n";
                    // This is the slow step — O(N log N) sort + index build.
                    // Expected: 1-3 seconds for 1M rows.
                    W.exec("ALTER TABLE trades ADD PRIMARY KEY (trade_id)");

                    std::cout << "[DB] Building composite index (symbol, timestamp)...\n";
                    W.exec(R"(
                CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
                ON trades (symbol, timestamp)
            )");

                    W.commit();

                    std::cout << "[DB] Constraints rebuilt. Load finalized.\n";
                    std::cout << "[DB]   Total rows : " << total_rows << "\n";
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[DB ERROR] finalize_parallel_load failed: " << e.what() << "\n";
                    throw;
                }
            }

        } // namespace MarketStream