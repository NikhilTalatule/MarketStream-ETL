#pragma once

// ============================================================================
// ParquetWriter — Converts vector<Trade> into Apache Parquet format
// ============================================================================
//
// WHY PARQUET MATTERS FOR YOUR CAREER:
//
// Parquet is the universal columnar format for the modern data stack.
// Every platform below reads it natively — no conversion needed:
//
//   AWS:        S3 + Athena queries Parquet directly (pay per byte scanned)
//   Snowflake:  COPY INTO from S3 Parquet — 5x faster than CSV
//   Databricks: Delta Lake is built on Parquet + ACID transaction log
//   Spark:      default output format for DataFrames
//   Power BI:   imports Parquet from Data Lake Storage Gen2
//   DuckDB:     SELECT * FROM 'file.parquet' — works out of the box
//   Python:     pd.read_parquet() / pl.read_parquet() — one line
//
// Your C++ pipeline writing Parquet = your data feeds every analytics
// tool in the modern stack without ANY conversion step.
//
// COLUMNAR LAYOUT — why it's 10-100x faster for analytics:
//
//   Row layout (how Trade struct lives in RAM):
//     [id=1, price=2456, sym=REL, vol=100, ...]
//     [id=2, price=3567, sym=TCS, vol=75,  ...]
//     [id=3, price=1423, sym=INF, vol=500, ...]
//
//   Columnar layout (how Arrow + Parquet stores it):
//     prices:  [2456.75, 3567.50, 1423.25, ...]  ← contiguous doubles
//     symbols: [REL,     TCS,     INF,     ...]  ← contiguous strings
//     volumes: [100,     75,      500,     ...]  ← contiguous uint32s
//
//   "SELECT AVG(price)" on row layout:
//     → reads 100% of bytes (must skip over every other field to get prices)
//
//   "SELECT AVG(price)" on columnar layout:
//     → reads ~8% of bytes (only the price column)
//     → all prices contiguous in memory → L1/L2 cache stays hot
//     → SIMD processes 4-8 doubles per CPU clock cycle
//
// ============================================================================

#include <string>
#include <vector>
#include <filesystem>
#include "../model/Trade.hpp"

namespace MarketStream
{

    class ParquetWriter
    {
    public:
        // ====================================================================
        // write()
        // ====================================================================
        // Converts vector<Trade> → Arrow Table → Parquet file on disk.
        //
        // PARAMETERS:
        //   trades      — validated trade data (typically 1M rows)
        //   output_path — destination .parquet file path
        //
        // RETURNS: duration in nanoseconds (plugs into BenchmarkResult)
        //
        // THROWS: std::runtime_error if Arrow/Parquet operations fail
        //
        // THREAD SAFETY:
        //   Reads trades (const ref) — safe to call from any thread.
        //   Writes to output_path — caller must ensure unique paths per thread.
        // ====================================================================
        [[nodiscard]]
        static long long write(
            const std::vector<Trade> &trades,
            const std::filesystem::path &output_path);

        // ====================================================================
        // make_output_path()
        // ====================================================================
        // Generates a timestamped filename: trades_20241025_091500.parquet
        // WHY TIMESTAMP IN FILENAME?
        // Each pipeline run produces a new Parquet file.
        // Timestamped names = natural partitioning by run time.
        // S3 prefix: s3://bucket/trades/trades_20241025_*.parquet
        // Athena can query a specific date's files using partition filters.
        // ====================================================================
        static std::filesystem::path make_output_path(
            const std::filesystem::path &directory = ".");
    };

} // namespace MarketStream