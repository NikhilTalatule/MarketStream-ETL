#include <iostream>
#include "../include/csv_parser.hpp"
#include "../include/database_loader.hpp"

int main()
{
    std::cout << "═══════════════════════════════════════════════════\n";
    std::cout << "     ETL Pipeline v1.0 - CSV to PostgreSQL\n";
    std::cout << "═══════════════════════════════════════════════════\n\n";

    // Configuration
    const std::string csv_file = "sample_data.csv";
    const std::string db_host = "localhost";
    const std::string db_name = "etl_pipeline_db";
    const std::string db_user = "postgres";
    const std::string db_password = "Nikhil@10"; 

    // Step 1: Parse CSV
    std::cout << "📂 Step 1: Parsing CSV file...\n";
    CSVParser parser;
    if (!parser.parse(csv_file))
    {
        return 1;
    }

    std::cout << "   Headers: ";
    for (const auto &h : parser.get_headers())
    {
        std::cout << h << " | ";
    }
    std::cout << "\n\n";

    // Step 2: Connect to Database
    std::cout << "🗄️  Step 2: Connecting to PostgreSQL...\n";
    DatabaseLoader loader(db_host, db_name, db_user, db_password);

    // Step 3: Create Table
    std::cout << "\n📋 Step 3: Creating table...\n";
    if (!loader.create_table())
    {
        return 1;
    }

    // Step 4: Load Data
    std::cout << "\n⬆️  Step 4: Loading data to database...\n";
    if (!loader.load_data(parser.get_data()))
    {
        return 1;
    }

    // Step 5: Verify
    std::cout << "\n✅ Step 5: Verification\n";
    loader.display_data();

    std::cout << "\n═══════════════════════════════════════════════════\n";
    std::cout << "  ✨ ETL Pipeline Completed Successfully! ✨\n";
    std::cout << "═══════════════════════════════════════════════════\n";

    return 0;
}
