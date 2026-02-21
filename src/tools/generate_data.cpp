#include <iostream>
#include <filesystem>
#include "DataGenerator.hpp"

// ============================================================================
// generate_data.exe — Standalone data generation tool
//
// WHY A SEPARATE EXECUTABLE?
// Data generation is a one-time setup task, not part of the pipeline loop.
// Mixing it into main.cpp would slow every pipeline run.
// Separate tool = generate once, run pipeline many times.
//
// Usage:
//   .\generate_data.exe              → generates 1,000,000 rows
//   .\generate_data.exe 500000       → generates 500,000 rows
// ============================================================================
int main(int argc, char *argv[])
{
    // argc = argument count (how many words on the command line)
    // argv = argument values (array of strings)
    // argv[0] = the program name itself
    // argv[1] = first user argument (optional row count)

    size_t num_trades = 1'000'000; // Default: 1 million

    if (argc > 1)
    {
        // stoul = string to unsigned long
        // The user can override: .\generate_data.exe 500000
        num_trades = std::stoul(argv[1]);
    }

    std::cout << "===================================================\n";
    std::cout << "   MarketStream ETL — Synthetic Data Generator\n";
    std::cout << "===================================================\n\n";

    try
    {
        // Generate into the build directory (same place as the exe)
        std::filesystem::path output = "large_data.csv";
        MarketStream::DataGenerator::generate(output, num_trades);

        std::cout << "\nRun the pipeline with:\n";
        std::cout << "  Edit main.cpp: change csv_file to \"large_data.csv\"\n";
        std::cout << "  Then: ninja && .\\etl_pipeline.exe\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}