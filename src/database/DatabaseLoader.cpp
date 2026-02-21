#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple>

namespace MarketStream
{

    DatabaseLoader::DatabaseLoader(const std::string &connection_string)
        : conn_str(connection_string) {}

    // =========================================================================
    // init_schema() — Creates BOTH tables with constraints and indexes
    // =========================================================================
    void DatabaseLoader::init_schema()
    {
        try
        {
            pqxx::connection C(conn_str);
            pqxx::work W(C);

            // ------------------------------------------------------------------
            // TABLE 1: trades — Raw trade executions
            // This is identical to before. Unchanged.
            // ------------------------------------------------------------------
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

            // ------------------------------------------------------------------
            // TABLE 2: technical_indicators — Computed signals per symbol
            // ------------------------------------------------------------------
            // WHY A SEPARATE TABLE AND NOT COLUMNS IN trades?
            //
            // trades = one row per TRADE EVENT (individual execution)
            // technical_indicators = one row per SYMBOL per COMPUTATION RUN
            //
            // These are fundamentally different granularities.
            // A trade is immutable — it happened once, never changes.
            // An indicator is recomputed every pipeline run with fresh data.
            //
            // Mixing them into one table would mean:
            //   - NULL columns for most trade rows (no indicator yet)
            //   - Updating rows when indicators refresh (breaks immutability)
            //   - Impossible to query "what was RSI 3 pipeline runs ago?"
            //
            // Separate tables = clean separation of concerns.
            // This is standard data warehouse design: fact table + derived table.
            //
            // COMPUTED_AT BIGINT:
            //   We store the time of computation as a Unix nanosecond timestamp.
            //   This lets you query: "show me RSI history for RELIANCE over time"
            //   Every pipeline run adds a new row, preserving history.
            //   This is the foundation of a time-series indicator store.
            // ------------------------------------------------------------------
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

            // Index for the most common query pattern:
            // "Give me RSI history for RELIANCE ordered by time"
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
    // bulk_load() — Streams trades via COPY + staging table pattern
    // Unchanged from Phase 4.
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

            W.exec(R"(
                CREATE TEMP TABLE trades_staging (
                    trade_id  BIGINT,
                    order_id  BIGINT,
                    timestamp BIGINT,
                    symbol    VARCHAR(10),
                    price     DOUBLE PRECISION,
                    volume    INTEGER,
                    side      CHAR(1),
                    type      CHAR(1),
                    is_pro    BOOLEAN
                ) ON COMMIT DROP;
            )");

            auto stream = pqxx::stream_to::table(
                W,
                {"trades_staging"},
                {"trade_id", "order_id", "timestamp", "symbol",
                 "price", "volume", "side", "type", "is_pro"});

            for (const auto &t : trades)
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
            stream.complete();

            pqxx::result result = W.exec(R"(
                INSERT INTO trades
                SELECT * FROM trades_staging
                ON CONFLICT (trade_id) DO NOTHING;
            )");

            W.commit();

            auto inserted = result.affected_rows();
            auto skipped = trades.size() - inserted;

            std::cout << "[DB] Trades load complete.\n";
            std::cout << "[DB]   Inserted : " << inserted << " new trades\n";
            std::cout << "[DB]   Skipped  : " << skipped << " duplicates\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Bulk load failed: " << e.what() << "\n";
            throw;
        }
    }

    // =========================================================================
    // save_indicators() — Persists computed indicators to technical_indicators
    // =========================================================================
    // WHY NOT USE COPY STREAM HERE?
    // Indicator rows are few (one per symbol, typically 5-50 rows).
    // COPY protocol has connection overhead that dominates for small batches.
    // For small row counts, a regular INSERT inside one transaction is faster
    // than COPY overhead + transaction + commit.
    //
    // The rule of thumb: use COPY for 1000+ rows. Use INSERT for < 100 rows.
    //
    // WHY INSERT EVERY RUN INSTEAD OF UPDATE?
    // We WANT a new row every pipeline run. This preserves historical indicator
    // values so you can answer: "What was RELIANCE RSI at 10:30am yesterday?"
    // Updating would destroy that history. Inserting preserves it.
    // This is the append-only / immutable log pattern used in data lakes.
    //
    // computed_at: We use std::chrono to get current nanosecond timestamp.
    // This stamps exactly when this computation run happened.
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

            // Get current timestamp in nanoseconds — stamps this computation run.
            // Every indicator row from this run gets the SAME computed_at value,
            // so you can query "give me all indicators from run X" easily.
            auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

            for (const auto &ind : indicators)
            {
                // W.exec() with $1, $2... = parameterized query.
                // WHY PARAMETERIZED AND NOT STRING CONCATENATION?
                // String concatenation: "INSERT ... VALUES ('" + symbol + "', ...)"
                // If symbol = "'; DROP TABLE trades; --" → SQL INJECTION attack.
                // Parameterized: pqxx escapes the values before sending to PostgreSQL.
                // The database sees them as DATA, not executable SQL.
                // This is non-negotiable in production — always use parameters.
                //
                // pqxx::params{v1, v2, ...} passes typed parameters safely.
                W.exec(
                    "INSERT INTO technical_indicators "
                    "(symbol, computed_at, sma, rsi, vwap, period) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    pqxx::params{
                        ind.symbol,
                        now_ns,
                        ind.sma,
                        ind.rsi,
                        ind.vwap,
                        ind.period});
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

} // namespace MarketStream