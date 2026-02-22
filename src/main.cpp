#include <iostream>
#include <vector>
#include <filesystem>
#include "parser/CsvParser.hpp"
#include "database/DatabaseLoader.hpp"
#include "validator/TradeValidator.hpp"
#include "benchmark/Benchmarker.hpp"
#include "indicators/TechnicalIndicators.hpp"
#include "threading/ParallelLoader.hpp" // Phase 9: replaces PipelineExecutor

int main()
{
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n\n";

    std::filesystem::path csv_file = "large_data.csv"; // 1M row dataset
    std::string db_conn = "user=postgres password=Nikhil@10 host=localhost port=5432 dbname=etl_pipeline_db";

    std::vector<MarketStream::BenchmarkResult> bench_results;

    try
    {
        // ── STAGE 1: EXTRACT ──────────────────────────────────────────────
        std::cout << "[STAGE 1] EXTRACT\n";
        std::vector<MarketStream::Trade> raw_trades;
        {
            MarketStream::Benchmarker bm("Parse", 0, bench_results);
            raw_trades = MarketStream::CsvParser().parse(csv_file);
        }
        bench_results.back().item_count = raw_trades.size();
        std::cout << "[SUCCESS] Parsed " << raw_trades.size() << " raw trades.\n\n";

        // ── STAGE 2: VALIDATE ─────────────────────────────────────────────
        std::cout << "[STAGE 2] VALIDATE\n";
        std::vector<MarketStream::Trade> valid_trades;
        {
            MarketStream::Benchmarker bm("Validate", raw_trades.size(), bench_results);
            valid_trades = MarketStream::TradeValidator::validate_batch(raw_trades);
        }
        if (valid_trades.empty())
        {
            std::cerr << "[CRITICAL] Zero valid trades. Aborting.\n";
            return 1;
        }
        std::cout << "\n";

        // ── STAGE 3: COMPUTE INDICATORS ───────────────────────────────────
        std::cout << "[STAGE 3] COMPUTE INDICATORS\n";
        std::vector<MarketStream::IndicatorResult> indicators;
        {
            MarketStream::Benchmarker bm("Indicators", valid_trades.size(), bench_results);
            indicators = MarketStream::TechnicalIndicators::compute_all(valid_trades, 5);
        }
        MarketStream::TechnicalIndicators::print_results(indicators);

        // ── STAGE 4: INIT SCHEMA ──────────────────────────────────────────
        // Schema init is always sequential — tables must exist before any load.
        // This also handles CREATE TABLE IF NOT EXISTS idempotently.
        std::cout << "[STAGE 4] INIT SCHEMA\n";
        {
            MarketStream::DatabaseLoader schema_loader(db_conn);
            schema_loader.init_schema();
        }
        std::cout << "\n";

        // ── STAGE 5: PARALLEL LOAD ────────────────────────────────────────
        // Phase 9: 4 threads × 250K rows each, all COPYing simultaneously.
        //
        // WHY STAGE 5 AND NOT STAGE 4b?
        // In Phase 6, parallel was a sub-step of Stage 4.
        // Now it's a full stage — it has its own thread pool, its own
        // prepare/finalize sequence, and its own benchmark entries.
        // Elevating it to a named stage makes the architecture clearer.
        //
        // NOTE: TRUNCATE TABLE trades before running with new data.
        // finalize_parallel_load() uses ADD PRIMARY KEY which requires
        // no duplicate trade_ids. Existing rows from prior runs will
        // cause a conflict. Run this in pgAdmin first:
        //   TRUNCATE TABLE trades;
        //   TRUNCATE TABLE technical_indicators;
        std::cout << "[STAGE 5] PARALLEL LOAD (4 threads)\n";
        {
            MarketStream::Benchmarker bm("Parallel Load", valid_trades.size(), bench_results);
            MarketStream::ParallelLoader::run(
                db_conn,
                valid_trades,
                indicators,
                bench_results,
                4 // num_threads — try 2, 4, 8 to find optimal for your hardware
            );
        }

        // ── PERFORMANCE REPORT ────────────────────────────────────────────
        MarketStream::print_benchmark_report(bench_results);

        std::cout << "[SUCCESS] ETL Pipeline Finished.\n";
        std::cout << "===================================================\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[CRITICAL ERROR] Pipeline crashed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}