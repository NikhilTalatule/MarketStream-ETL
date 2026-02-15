#include "CsvParser.hpp"
#include <fstream>
#include <iostream>
#include <charconv> // C++17: The fastest way to parse numbers (faster than atoi/stod)

namespace MarketStream
{

    std::vector<Trade> CsvParser::parse(const std::filesystem::path &file_path)
    {
        std::vector<Trade> trades;
        std::ifstream file(file_path, std::ios::binary | std::ios::ate); // Open at End (ate) to get size

        if (!file.is_open())
        {
            std::cerr << "Error: Could not open file " << file_path << "\n";
            return trades;
        }

        // 1. Get File Size & Allocate Buffer (The "One Big Read" Strategy)
        // Instead of reading line-by-line (millions of small reads), we read the whole block.
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::vector<char> buffer(size);
        if (!file.read(buffer.data(), size))
        {
            std::cerr << "Error: Could not read file content\n";
            return trades;
        }

        // 2. Create a View over the buffer
        // std::string_view is just a pointer + length. It costs nothing.
        std::string_view content(buffer.data(), size);

        // 3. Parse Line by Line
        size_t start = 0;
        size_t end = content.find('\n');

        while (end != std::string_view::npos)
        {
            // Create a view for just this line (Zero Copy!)
            std::string_view line = content.substr(start, end - start);

            // Skip empty lines
            if (!line.empty())
            {
                trades.push_back(parse_line(line));
            }

            // Move to next line
            start = end + 1;
            end = content.find('\n', start);
        }

        // Handle the last line (if no newline at end)
        if (start < size)
        {
            std::string_view line = content.substr(start, size - start);
            if (!line.empty())
                trades.push_back(parse_line(line));
        }

        return trades;
    }

    Trade CsvParser::parse_line(std::string_view line)
    {
        Trade trade;
        size_t start = 0;
        size_t end = line.find(',');

        // 1. Parse Symbol (String)
        // We HAVE to copy here because our Trade struct owns the std::string.
        // This is the only allocation in the whole function.
        trade.symbol = (end == std::string_view::npos) ? "" : std::string(line.substr(start, end - start));

        // Move to Price
        start = end + 1;
        end = line.find(',', start);
        std::string_view price_str = line.substr(start, end - start);

        // 2. Parse Price (Double) using std::from_chars (High Performance)
        // std::from_chars is 5-10x faster than std::stod because it doesn't handle locales.
        std::from_chars(price_str.data(), price_str.data() + price_str.size(), trade.price);

        // Move to Volume
        start = end + 1;
        end = line.find(',', start);
        std::string_view vol_str = line.substr(start, end - start);

        // 3. Parse Volume (Int)
        std::from_chars(vol_str.data(), vol_str.data() + vol_str.size(), trade.volume);

        // 4. Fake Timestamp (for now, just use system time or a counter)
        // Real parsing of "2023-10-25 10:30:00" is complex; we'll add that later.
        trade.timestamp = 0;

        return trade;
    }

} // namespace MarketStream