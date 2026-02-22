#pragma once

#include <string>
#include <vector>
#include <span> // C++20: zero-copy slice view
#include <pqxx/pqxx>
#include "../model/Trade.hpp"
#include "../indicators/TechnicalIndicators.hpp"

namespace MarketStream
{

    class DatabaseLoader
    {
    public:
        DatabaseLoader(const std::string &connection_string);
        ~DatabaseLoader() = default;

        // ── Existing methods (unchanged) ──────────────────────────────────

        // Creates both tables: 'trades' and 'technical_indicators'
        void init_schema();

        // Single-connection bulk load (staging table + ON CONFLICT pattern)
        // Keep for small datasets (< 100K rows) or incremental loads
        void bulk_load(const std::vector<Trade> &trades);

        // Saves computed indicators to technical_indicators table
        void save_indicators(const std::vector<IndicatorResult> &indicators);

        // ── Phase 9: Parallel COPY methods (call in this exact order) ────
        //
        // USAGE:
        //   DatabaseLoader prep(conn); prep.prepare_for_parallel_load();
        //   // ... launch N threads, each calling copy_chunk() ...
        //   DatabaseLoader fin(conn);  fin.finalize_parallel_load(total);
        //
        // WHY THREE SEPARATE METHODS INSTEAD OF ONE?
        // prepare and finalize MUST be sequential (DDL locks).
        // copy_chunk MUST be parallel (performance).
        // Splitting them lets the caller control the threading strategy.
        // This is the "Strategy Pattern" — behavior separated from mechanism.

        // Step 1 (sequential): Drop PRIMARY KEY and index before parallel COPY
        void prepare_for_parallel_load();

        // Step 2 (parallel): COPY one chunk of trades directly into trades table
        // Each thread calls this with its own DatabaseLoader instance (= own connection)
        void copy_chunk(std::span<const Trade> chunk, int thread_id);

        // Step 3 (sequential): Rebuild PRIMARY KEY and index after all chunks loaded
        void finalize_parallel_load(size_t total_rows);

    private:
        std::string conn_str;
    };

} // namespace MarketStream