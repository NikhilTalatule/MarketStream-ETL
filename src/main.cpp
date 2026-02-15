#include <iostream>
#include <vector>
#include <filesystem>
#include "parser/CsvParser.hpp" // Include our new high-speed parser

// Note: We will add the DatabaseLoader later. For now, we verify the Parser works.

int main()
{
    // 1. Setup the High-Performance Environment
    // turning off sync with C-style I/O improves std::cout speed
    std::ios_base::sync_with_stdio(false);

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL | High-Frequency Trading Engine\n";
    std::cout << "===================================================\n";

    // 2. Define the Input File
    // Ensure you have created this file in your 'build' folder!
    std::filesystem::path csv_file = "sample_data.csv";

    // 3. Initialize the Engine components
    MarketStream::CsvParser parser;

    // 4. Execute the Parse
    std::cout << "[INFO] Reading " << csv_file << " ...\n";

    try
    {
        auto trades = parser.parse(csv_file);

        // 5. Report Results (The "Dashboard")
        std::cout << "[SUCCESS] Parsed " << trades.size() << " trades successfully.\n";

        if (!trades.empty())
        {
            std::cout << "[DEBUG] Sample Trade (First Row):\n";
            std::cout << "   Symbol: " << trades[0].symbol << "\n";
            std::cout << "   Price:  " << trades[0].price << "\n";
            std::cout << "   Volume: " << trades[0].volume << "\n";
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "[CRITICAL ERROR] Engine crash: " << e.what() << "\n";
        return 1;
    }

    return 0;
}