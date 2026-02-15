#pragma once

#include <string>
#include <concepts>
#include <cstdint> // Fixed width integers (uint64_t) are mandatory in finance

namespace MarketStream
{

    /**
     * @brief Represents a comprehensive trade execution.
     * * MEMORY LAYOUT OPTIMIZATION:
     * We order fields largest-to-smallest to minimize "Structure Padding".
     * This makes the struct smaller in memory (Cache Friendly).
     */
    struct Trade
    {
        // --- 8 Byte Fields ---
        uint64_t trade_id;   // Unique ID from the Exchange (e.g., 1000234)
        uint64_t order_id;   // The Order ID that triggered this trade
        long long timestamp; // Nanoseconds since Epoch (UTC)
        double price;        // Execution Price

        // --- 4 Byte Fields ---
        uint32_t volume; // Quantity traded

        // --- Complex Fields ---
        std::string symbol;   // Ticker (e.g., "RELIANCE")
        std::string exchange; // Exchange Code (e.g., "NSE", "BSE", "NASDAQ")

        // --- 1 Byte Fields ---
        char side;   // 'B' = Buy, 'S' = Sell, 'N' = Unknown
        char type;   // 'M' = Market, 'L' = Limit, 'I' = IOC
        bool is_pro; // true = Institutional Trade, false = Retail

        // C++20 Spaceship Operator: Sorts primarily by Timestamp, then by Trade ID
        auto operator<=>(const Trade &) const = default;
    };

    // --- C++20 Concept for Validation ---
    // This ensures any generic function processing a 'Trade' can access these fields.
    template <typename T>
    concept Tradeable = requires(T t) {
        { t.trade_id } -> std::convertible_to<uint64_t>;
        { t.price } -> std::convertible_to<double>;
        { t.volume } -> std::convertible_to<uint32_t>;
        { t.timestamp } -> std::convertible_to<long long>;
        { t.side } -> std::convertible_to<char>;
    };

} // namespace MarketStream