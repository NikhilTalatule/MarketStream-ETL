#include <iostream>
#include <vector>
#include <filesystem>
#include <cstdlib>          // std::getenv — reads environment variables
#include "parser/CsvParser.hpp"
#include "database/DatabaseLoader.hpp"
#include "validator/TradeValidator.hpp"
#include "benchmark/Benchmarker.hpp"
#include "indicators/TechnicalIndicators.hpp"
#include "threading/ParallelLoader.hpp"
#include "output/ParquetWriter.hpp"

int main()
{
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n\n";

    std::filesystem::path csv_file = "large_data.csv";

    // -------------------------------------------------------------------------
    // IMPROVEMENT 1: Read DB connection string from environment variable.
    //
    // WHY NOT HARDCODE THE PASSWORD?
    //   A hardcoded password in source code is a security vulnerability.
    //   Anyone who sees this file (on GitHub, in a code review, over your
    //   shoulder) instantly has your database credentials.
    //
    // HOW TO SET THE ENVIRONMENT VARIABLE (run this ONCE in your terminal):
    //   On MSYS2 / Linux / Mac, add this line to your ~/.bashrc file:
    //
    //     export ETL_DB_CONN="user=postgres password=YOUR_PASSWORD host=localhost port=5432 dbname=etl_pipeline_db"
    //
    //   Then reload it:
    //     source ~/.bashrc
    //
    //   On Windows CMD (not MSYS2):
    //     setx ETL_DB_CONN "user=postgres password=YOUR_PASSWORD host=localhost port=5432 dbname=etl_pipeline_db"
    //
    // HOW std::getenv WORKS:
    //   std::getenv("ETL_DB_CONN") asks the OS:
    //     "Do you have a variable called ETL_DB_CONN?"
    //   If YES  → returns a pointer to the value string
    //   If NO   → returns nullptr (null pointer — nothing)
    //
    // WHY CHECK FOR nullptr?
    //   If someone forgets to set the variable and we use nullptr as a string,
    //   the program crashes with an unhelpful segfault.
    //   We check explicitly and print a clear error message instead.
    // -------------------------------------------------------------------------
    const char* env_conn = std::getenv("ETL_DB_CONN");
    if (!env_conn)
    {
        std::cerr << "[ERROR] ETL_DB_CONN environment variable is not set.\n";
        std::cerr << "  Please set it before running the pipeline.\n";
        std::cerr << "  Example (add to ~/.bashrc):\n";
        std::cerr << "    export ETL_DB_CONN=\"user=postgres password=YOUR_PASSWORD"
                     " host=localhost port=5432 dbname=etl_pipeline_db\"\n";
        return 1;
    }
    std::string db_conn = env_conn;

    std::vector<MarketStream::BenchmarkResult> bench_results;

    try
    {
        // STAGE 1: EXTRACT
        std::cout << "[STAGE 1] EXTRACT\n";
        std::vector<MarketStream::Trade> raw_trades;
        {
            MarketStream::Benchmarker bm("Parse", 0, bench_results);
            raw_trades = MarketStream::CsvParser().parse(csv_file);
        }
        bench_results.back().item_count = raw_trades.size();
        std::cout << "[SUCCESS] Parsed " << raw_trades.size() << " raw trades.\n\n";

        // STAGE 2: VALIDATE
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

        // STAGE 3: COMPUTE INDICATORS
        std::cout << "[STAGE 3] COMPUTE INDICATORS\n";
        std::vector<MarketStream::IndicatorResult> indicators;
        {
            MarketStream::Benchmarker bm("Indicators", valid_trades.size(), bench_results);
            indicators = MarketStream::TechnicalIndicators::compute_all(valid_trades, 5);
        }
        MarketStream::TechnicalIndicators::print_results(indicators);

        // STAGE 4: INIT SCHEMA
        std::cout << "[STAGE 4] INIT SCHEMA\n";
        {
            MarketStream::DatabaseLoader schema_loader(db_conn);
            schema_loader.init_schema();
        }
        std::cout << "\n";

        // STAGE 5: PARALLEL DB LOAD (4 threads)
        // REMINDER: TRUNCATE TABLE trades; TRUNCATE TABLE technical_indicators;
        std::cout << "[STAGE 5] PARALLEL LOAD (4 threads)\n";
        {
            MarketStream::Benchmarker bm("Parallel Load", valid_trades.size(), bench_results);
            MarketStream::ParallelLoader::run(
                db_conn, valid_trades, indicators, bench_results, 4);
        }
        std::cout << "\n";

        // STAGE 6: PARQUET OUTPUT
        // PostgreSQL  = operational DB (OLTP) — point queries, inserts
        // Parquet     = analytics format (OLAP) — aggregations, ML, S3, Athena
        // Both from ONE pipeline run.
        std::cout << "[STAGE 6] PARQUET OUTPUT\n";
        {
            auto parquet_path = MarketStream::ParquetWriter::make_output_path(".");
            MarketStream::Benchmarker bm("Parquet Write", valid_trades.size(), bench_results);
            MarketStream::ParquetWriter::write(valid_trades, parquet_path);
        }
        std::cout << "\n";

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
