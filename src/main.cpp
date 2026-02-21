#include <iostream>
#include <vector>
#include <filesystem>
#include "parser/CsvParser.hpp"
#include "database/DatabaseLoader.hpp"
#include "validator/TradeValidator.hpp"
#include "benchmark/Benchmarker.hpp" // NEW

int main()
{
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n";

    std::filesystem::path csv_file = "sample_data.csv";
    std::string db_conn = "user=postgres password=Nikhil@10 host=localhost port=5432 dbname=etl_pipeline_db";

    // All benchmark results collected here.
    // Passed by reference into each Benchmarker so they can push results.
    std::vector<MarketStream::BenchmarkResult> bench_results;

    try
    {
        // =====================================================================
        // STAGE 1 — EXTRACT (timed)
        // =====================================================================
        std::cout << "\n[STAGE 1] EXTRACT\n";
        std::cout << "[INFO] Reading " << csv_file << " ...\n";

        std::vector<MarketStream::Trade> raw_trades;
        {
            // Opening brace starts the scope.
            // Benchmarker constructor fires here → start time recorded.
            // We pass 0 for item_count because we don't know it yet.
            // We'll update it after parsing via a separate result entry.
            MarketStream::Benchmarker bm("Parse", 0, bench_results);

            MarketStream::CsvParser parser;
            raw_trades = parser.parse(csv_file);

            // Closing brace → Benchmarker destructor fires → time recorded.
        }

        // Update the parse result's item_count now that we know it
        // bench_results.back() = the result just pushed by the Benchmarker above
        bench_results.back().item_count = raw_trades.size();

        std::cout << "[SUCCESS] Parsed " << raw_trades.size() << " raw trades.\n";

        // =====================================================================
        // STAGE 2 — VALIDATE (timed)
        // =====================================================================
        std::cout << "\n[STAGE 2] TRANSFORM / VALIDATE\n";

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

        // =====================================================================
        // STAGE 3 — LOAD (timed)
        // =====================================================================
        std::cout << "\n[STAGE 3] LOAD\n";

        MarketStream::DatabaseLoader loader(db_conn);
        loader.init_schema();

        {
            MarketStream::Benchmarker bm("DB Load", valid_trades.size(), bench_results);
            loader.bulk_load(valid_trades);
        }

        // =====================================================================
        // PRINT PERFORMANCE REPORT
        // =====================================================================
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