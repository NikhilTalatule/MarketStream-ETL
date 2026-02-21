#include "CsvParser.hpp"
#include <fstream>
#include <iostream>
#include <charconv> // C++17: from_chars — the fastest number parser in C++

namespace MarketStream
{

    // =========================================================================
    // HELPER: extract_field
    // =========================================================================
    // WHY A HELPER FUNCTION?
    // We need to do the same thing 9 times (once per column): find the next
    // comma, cut out that piece of text, and advance our position.
    // Repeating that logic 9 times makes code messy and bug-prone.
    // One helper = one place to fix if there's a bug.
    //
    // WHY string_view& (reference)?
    // string_view is just a pointer + a length. We pass it by reference so
    // this function can MODIFY 'remaining' — chopping off the field we just
    // read. After the call, 'remaining' points to the rest of the line.
    // This is zero-copy: no new string is allocated anywhere.
    // =========================================================================
    static std::string_view extract_field(std::string_view &remaining)
    {
        // Find the next comma in the remaining text
        size_t comma_pos = remaining.find(',');

        std::string_view field;

        if (comma_pos == std::string_view::npos)
        {
            // No comma found = this is the LAST field on the line
            field = remaining;
            remaining = {}; // Make remaining empty
        }
        else
        {
            // Extract everything BEFORE the comma
            field = remaining.substr(0, comma_pos);
            // Advance remaining to AFTER the comma
            remaining = remaining.substr(comma_pos + 1);
        }

        // Strip carriage return \r if the file was saved on Windows
        // Windows line endings are \r\n. Linux is just \n.
        // getline strips \n but NOT \r, so we do it manually.
        if (!field.empty() && field.back() == '\r')
            field.remove_suffix(1);

        return field;
    }

    // =========================================================================
    // MAIN PARSE FUNCTION
    // =========================================================================
    std::vector<Trade> CsvParser::parse(const std::filesystem::path &file_path)
    {
        std::vector<Trade> trades;

        // Open file in BINARY mode
        // WHY BINARY? In text mode on Windows, the OS converts \r\n → \n
        // automatically, which adds overhead. In binary mode, we get the raw
        // bytes and handle it ourselves — faster and predictable.
        // 'ate' = "At The End" — seek to end of file immediately so we can
        // ask "how big is this file?" with tellg().
        std::ifstream file(file_path, std::ios::binary | std::ios::ate);

        if (!file.is_open())
        {
            std::cerr << "[PARSER ERROR] Cannot open: " << file_path << "\n";
            return trades;
        }

        // -------------------------------------------------------
        // THE "ONE BIG READ" STRATEGY
        // -------------------------------------------------------
        // WHY NOT READ LINE BY LINE?
        // Every call to getline() is a system call — a request to the OS.
        // System calls are expensive (microseconds each). For 1 million rows,
        // that's millions of system calls = seconds of overhead.
        //
        // Instead: ONE system call reads the ENTIRE file into RAM.
        // Then we process purely in memory — no OS involvement.
        // This is the same technique used by high-performance JSON parsers
        // like simdjson and RapidCSV.
        // -------------------------------------------------------
        std::streamsize file_size = file.tellg(); // We're at end, so tellg() = file size
        file.seekg(0, std::ios::beg);             // Seek back to beginning

        // Allocate one contiguous block of memory for entire file
        // std::vector<char> manages the memory for us (RAII)
        std::vector<char> buffer(file_size);

        if (!file.read(buffer.data(), file_size))
        {
            std::cerr << "[PARSER ERROR] File read failed: " << file_path << "\n";
            return trades;
        }

        // -------------------------------------------------------
        // CREATE A STRING_VIEW OVER THE BUFFER
        // -------------------------------------------------------
        // WHY string_view AND NOT string?
        // std::string would COPY all that data into a new allocation = double RAM.
        // std::string_view is JUST a pointer to buffer.data() + a length.
        // It costs exactly 16 bytes on a 64-bit machine (8 byte pointer + 8 byte size).
        // We are "viewing" the buffer, not owning a copy. Zero copy = zero overhead.
        // -------------------------------------------------------
        std::string_view content(buffer.data(), file_size);

        // -------------------------------------------------------
        // PARSE LINE BY LINE
        // -------------------------------------------------------
        size_t start = 0;
        size_t end = content.find('\n');
        bool first_line = true; // FIX for Bug #1: tracks the header row

        while (end != std::string_view::npos)
        {
            std::string_view line = content.substr(start, end - start);

            // Strip \r for Windows compatibility
            if (!line.empty() && line.back() == '\r')
                line.remove_suffix(1);

            if (!line.empty())
            {
                if (first_line)
                {
                    // SKIP THE HEADER ROW
                    // "trade_id,order_id,timestamp,..." is text, not data.
                    // Trying to parse "trade_id" as a uint64 would silently
                    // fail and give us trade_id = 0. We skip it entirely.
                    first_line = false;
                }
                else
                {
                    // Parse this data row into a Trade object
                    Trade t = parse_line(line);
                    trades.push_back(std::move(t));
                    // WHY std::move?
                    // Trade contains std::string (symbol). Without move,
                    // push_back would COPY that string. With move, it
                    // TRANSFERS ownership = zero copy. This matters at scale.
                }
            }

            start = end + 1;
            end = content.find('\n', start);
        }

        // Handle last line if file doesn't end with newline
        if (start < static_cast<size_t>(file_size))
        {
            std::string_view line = content.substr(start);
            if (!line.empty() && line != "\r")
                trades.push_back(parse_line(line));
        }

        return trades;
    }

    // =========================================================================
    // parse_line — Converts one CSV line into one Trade struct
    // =========================================================================
    // Column order: trade_id,order_id,timestamp,symbol,price,volume,side,type,is_pro
    // =========================================================================
    Trade CsvParser::parse_line(std::string_view line)
    {
        Trade trade{}; // {} = zero-initialize ALL fields. Prevents garbage data.
                       // Without this, unread fields would have random RAM values.

        // -------------------------------------------------------
        // PARSING PATTERN FOR EACH FIELD:
        // 1. Call extract_field(line) — this MODIFIES 'line',
        //    removing the field we just read from the front.
        //    After the call, 'line' starts at the NEXT field.
        // 2. Use from_chars to convert text → number (for numeric fields)
        //    OR std::string constructor for string fields.
        // -------------------------------------------------------

        // Field 1: trade_id (uint64_t)
        {
            auto field = extract_field(line);
            std::from_chars(field.data(), field.data() + field.size(), trade.trade_id);
        }

        // Field 2: order_id (uint64_t)
        {
            auto field = extract_field(line);
            std::from_chars(field.data(), field.data() + field.size(), trade.order_id);
        }

        // Field 3: timestamp (long long = nanoseconds)
        {
            auto field = extract_field(line);
            std::from_chars(field.data(), field.data() + field.size(), trade.timestamp);
        }

        // Field 4: symbol (std::string)
        // WHY is this the ONLY allocation in the entire function?
        // std::string needs to own its memory (heap allocation).
        // All other fields are primitive types (int, double, char)
        // that sit directly inside the Trade struct — no heap needed.
        {
            auto field = extract_field(line);
            trade.symbol = std::string(field); // ONE heap allocation per trade
        }

        // Field 5: price (double)
        // from_chars for float/double requires C++17 or later.
        // It does NOT handle locale (no "1,234.56" European format).
        // Pure ASCII digits only — exactly what exchange feeds send.
        {
            auto field = extract_field(line);
            std::from_chars(field.data(), field.data() + field.size(), trade.price);
        }

        // Field 6: volume (uint32_t)
        {
            auto field = extract_field(line);
            std::from_chars(field.data(), field.data() + field.size(), trade.volume);
        }

        // Field 7: side ('B' or 'S')
        // It's a single character field. Just grab the first char.
        // No parsing needed — char IS the value.
        {
            auto field = extract_field(line);
            trade.side = field.empty() ? 'N' : field[0]; // 'N' = Unknown if missing
        }

        // Field 8: type ('M', 'L', or 'I')
        {
            auto field = extract_field(line);
            trade.type = field.empty() ? 'M' : field[0];
        }

        // Field 9: is_pro (bool, stored as 0 or 1 in CSV)
        {
            auto field = extract_field(line);
            int val = 0;
            std::from_chars(field.data(), field.data() + field.size(), val);
            trade.is_pro = (val == 1); // 1 → true (institutional), 0 → false (retail)
        }

        return trade;
    }

} // namespace MarketStream