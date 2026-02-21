#include <iostream>
#include <vector>
#include <filesystem>
#include "parser/CsvParser.hpp"
#include "database/DatabaseLoader.hpp"
#include "validator/TradeValidator.hpp"
#include "benchmark/Benchmarker.hpp"
#include "indicators/TechnicalIndicators.hpp"
#include "threading/PipelineExecutor.hpp" // NEW

int main()
{
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n";

    std::filesystem::path csv_file = "sample_data.csv";
    std::string db_conn = "user=postgres password=Nikhil@10 host=localhost port=5432 dbname=etl_pipeline_db";

    std::vector<MarketStream::BenchmarkResult> bench_results;

    try
    {
        // STAGE 1 — EXTRACT
        std::cout << "\n[STAGE 1] EXTRACT\n";
        std::vector<MarketStream::Trade> raw_trades;
        {
            MarketStream::Benchmarker bm("Parse", 0, bench_results);
            raw_trades = MarketStream::CsvParser().parse(csv_file);
        }
        bench_results.back().item_count = raw_trades.size();
        std::cout << "[SUCCESS] Parsed " << raw_trades.size() << " raw trades.\n";

        // STAGE 2 — VALIDATE
        std::cout << "\n[STAGE 2] VALIDATE\n";
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

        // STAGE 3 — COMPUTE INDICATORS
        std::cout << "\n[STAGE 3] COMPUTE INDICATORS\n";
        std::vector<MarketStream::IndicatorResult> indicators;
        {
            MarketStream::Benchmarker bm("Indicators", valid_trades.size(), bench_results);
            indicators = MarketStream::TechnicalIndicators::compute_all(valid_trades, 5);
        }
        MarketStream::TechnicalIndicators::print_results(indicators);

        // STAGE 4 — SCHEMA INIT (must be sequential — tables must exist before load)
        std::cout << "[STAGE 4] INIT SCHEMA + PARALLEL LOAD\n";
        MarketStream::DatabaseLoader loader(db_conn);
        loader.init_schema();

        // ==================================================================
        // STAGE 4b — PARALLEL: Load trades + Save indicators simultaneously
        // ==================================================================
        // WHY IS init_schema() SEQUENTIAL BUT load IS PARALLEL?
        // init_schema() must complete FIRST — it creates the tables.
        // If bulk_load and save_indicators ran before init_schema() finished,
        // they'd try to write to tables that don't exist yet → crash.
        // This is a DEPENDENCY: schema creation → data loading.
        // Dependencies must be sequential. Independent work can be parallel.
        // Identifying what is and isn't a dependency is the core skill of
        // concurrent system design.
        // ==================================================================
        MarketStream::PipelineExecutor::run_parallel_load(
            db_conn,
            valid_trades,
            indicators,
            bench_results);

        // PERFORMANCE REPORT
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