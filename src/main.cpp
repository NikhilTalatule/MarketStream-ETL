#include <iostream>
#include <vector>
#include <filesystem>
#include "parser/CsvParser.hpp"
#include "database/DatabaseLoader.hpp"

int main()
{
    // Optimization: Fast I/O
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n";

    // Configuration
    std::filesystem::path csv_file = "sample_data.csv";
    // NOTE: Replace 'password' with your actual password!
    std::string db_conn = "user=postgres password=Nikhil@10 host=localhost port=5432 dbname=etl_pipeline_db";

    try
    {
        // 1. Parse
        std::cout << "[INFO] Reading " << csv_file << " ...\n";
        MarketStream::CsvParser parser;
        auto trades = parser.parse(csv_file);
        std::cout << "[SUCCESS] Parsed " << trades.size() << " trades.\n";

        // 2. Database Init
        std::cout << "[INFO] Connecting to Database...\n";
        MarketStream::DatabaseLoader loader(db_conn);
        loader.init_schema();

        // 3. Load Data
        std::cout << "[INFO] Starting Bulk Load...\n";
        loader.bulk_load(trades);

        std::cout << "[SUCCESS] ETL Pipeline Finished.\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[CRITICAL ERROR] Pipeline crashed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}