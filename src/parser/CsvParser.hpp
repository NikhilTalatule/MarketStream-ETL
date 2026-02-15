#pragma once

#include <string_view>
#include <vector>
#include <filesystem>
#include <span > // C++20: A lightweight view over an array
#include "../model/Trade.hpp"

namespace MarketStream {

    class CsvParser {
    public:
        // Constructor
        CsvParser() = default;

        /**
         * @brief Parses a CSV file into a vector of Trade objects.
         * Uses "Zero-Copy" logic by reading large chunks into a buffer 
         * and using string_views to process them.
         * * @param file_path Path to the CSV file
         * @return std::vector<Trade> List of parsed trades
         */
        [[nodiscard]]
        std::vector<Trade> parse(const std::filesystem::path& file_path);

    private:
        /**
         * @brief internal helper to parse a single line.
         * Takes a raw view of the line (no string copy) and fills a Trade struct.
         */
        [[nodiscard]]
        Trade parse_line(std::string_view line);
    };

} // namespace MarketStream