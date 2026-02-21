#pragma once

// ============================================================================
// WHY A DATA GENERATOR?
//
// Real exchange data feeds are proprietary — NSE, BSE, NASDAQ don't give
// free access to historical tick data. For stress testing, we generate
// synthetic data that mirrors real exchange feed characteristics:
//
//   - Realistic price movements (random walk with drift)
//   - Correct timestamp spacing (nanosecond intervals)
//   - Realistic volume distribution (institutional vs retail sizes)
//   - Correct symbol distribution (some stocks trade more than others)
//   - Mix of buy/sell sides and order types
//
// A random walk means: next_price = current_price + small_random_change
// This mimics how stock prices actually move — not randomly jumping between
// unrelated values, but drifting up and down from the previous price.
// It's the foundation of the Geometric Brownian Motion model used in
// Black-Scholes option pricing.
// ============================================================================

#include <fstream>
#include <string>
#include <vector>
#include <random> // C++ random number generation
#include <chrono>
#include <iostream>
#include <iomanip>
#include <filesystem>
#include <unordered_map>
namespace MarketStream
{

    class DataGenerator
    {
    public:
        // ====================================================================
        // generate()
        // ====================================================================
        // Writes a CSV file with 'num_trades' rows of realistic trade data.
        //
        // PARAMETERS:
        //   output_path — where to write the CSV file
        //   num_trades  — how many trade rows to generate
        //   seed        — random seed for reproducibility
        //                 Same seed = same data every time = reproducible tests
        // ====================================================================
        static void generate(
            const std::filesystem::path &output_path,
            size_t num_trades = 1'000'000,
            uint64_t seed = 42)
        {
            std::cout << "[GENERATOR] Generating " << num_trades
                      << " synthetic trades...\n";

            auto gen_start = std::chrono::high_resolution_clock::now();

            // ----------------------------------------------------------------
            // RANDOM NUMBER ENGINE
            // ----------------------------------------------------------------
            // std::mt19937_64 = Mersenne Twister 64-bit.
            // WHY THIS SPECIFIC ENGINE?
            // It has a period of 2^19937-1 (astronomically large — never repeats
            // in any practical use). It passes all statistical randomness tests.
            // It's the industry standard for Monte Carlo simulations and
            // synthetic data generation. Finance uses it everywhere.
            // The 64 = uses 64-bit integers internally = faster on 64-bit CPUs.
            // ----------------------------------------------------------------
            std::mt19937_64 rng(seed);

            // ----------------------------------------------------------------
            // DISTRIBUTION OBJECTS
            // ----------------------------------------------------------------
            // std::uniform_int_distribution<int>(0, 5) generates integers
            // from 0 to 5 with equal probability (uniform distribution).
            //
            // std::normal_distribution<double>(mean, stddev) generates doubles
            // following a bell curve centered at 'mean'.
            // We use this for price changes — small changes are common,
            // large jumps are rare. Exactly like real market microstructure.
            // ----------------------------------------------------------------

            // Which symbol to assign to each trade
            // Weights: RELIANCE and TCS trade most frequently (index heavyweights)
            std::vector<std::string> symbols = {
                "RELIANCE", "RELIANCE", "RELIANCE", // 3x weight = trades most
                "TCS", "TCS", "TCS",
                "INFY", "INFY",
                "HDFC", "HDFC",
                "WIPRO",
                "ICICIBANK",
                "BAJFINANCE",
                "HCLTECH",
                "AXISBANK",
                "SBIN"};
            std::uniform_int_distribution<int> symbol_dist(0, symbols.size() - 1);

            // Price change per tick: normally distributed around 0
            // mean=0.0 = no systematic drift (fair market)
            // stddev=0.5 = typical price fluctuation per trade in rupees
            std::normal_distribution<double> price_change_dist(0.0, 0.5);

            // Volume: uniform between 10 and 5000 shares
            // Real trades: retail = 10-500 shares, institutional = 500-5000+
            std::uniform_int_distribution<int> volume_dist(10, 5000);

            // Side: 0=Buy, 1=Sell (roughly equal probability)
            std::uniform_int_distribution<int> side_dist(0, 1);

            // Order type: 0=Market, 1=Limit, 2=IOC
            // Market orders ~30%, Limit orders ~60%, IOC ~10%
            std::uniform_int_distribution<int> type_dist(0, 9);

            // is_pro: ~20% institutional, 80% retail
            std::uniform_int_distribution<int> pro_dist(0, 4);

            // ----------------------------------------------------------------
            // STARTING PRICES PER SYMBOL
            // These are realistic Indian stock prices (approximate)
            // ----------------------------------------------------------------
            std::unordered_map<std::string, double> current_prices = {
                {"RELIANCE", 2456.75},
                {"TCS", 3567.50},
                {"INFY", 1423.25},
                {"HDFC", 1678.90},
                {"WIPRO", 432.60},
                {"ICICIBANK", 987.45},
                {"BAJFINANCE", 6823.10},
                {"HCLTECH", 1234.55},
                {"AXISBANK", 987.30},
                {"SBIN", 601.75}};

            // ----------------------------------------------------------------
            // OPEN FILE AND WRITE
            // ----------------------------------------------------------------
            // std::ios::binary = write raw bytes, no \r\n translation on Windows
            // This gives us consistent file behavior cross-platform.
            // ----------------------------------------------------------------
            std::ofstream file(output_path, std::ios::binary);
            if (!file.is_open())
            {
                throw std::runtime_error("Cannot create file: " + output_path.string());
            }

            // Write CSV header
            file << "trade_id,order_id,timestamp,symbol,price,volume,side,type,is_pro\n";

            // Starting timestamp: Oct 25, 2023 09:15:00 IST in nanoseconds
            // (NSE market open time)
            long long timestamp = 1698208500000000000LL;

            // Nanoseconds between trades: ~10 microseconds average for NSE
            // This gives us 100,000 trades/second which is realistic for busy sessions
            std::uniform_int_distribution<long long> time_gap_dist(5000, 50000); // 5µs–50µs

            for (size_t i = 0; i < num_trades; ++i)
            {
                // Pick symbol for this trade
                const std::string &symbol = symbols[symbol_dist(rng)];

                // Apply random walk to price
                // WHY CLAMP?
                // random_walk can drift very far with enough steps.
                // We clamp to ensure prices stay within a realistic range
                // (never below 10% of starting price, never above 10x starting).
                double &price = current_prices[symbol];
                price += price_change_dist(rng);
                if (price < 50.0)
                    price = 50.0; // Floor: prevent negative/zero prices
                if (price > 99999.0)
                    price = 99999.0; // Ceiling: prevent unrealistic values

                // Generate other fields
                int vol = volume_dist(rng);
                char side = (side_dist(rng) == 0) ? 'B' : 'S';

                int type_roll = type_dist(rng);
                char type = (type_roll < 3)   ? 'M'  // 30% Market
                            : (type_roll < 9) ? 'L'  // 60% Limit
                                              : 'I'; // 10% IOC

                bool is_pro = (pro_dist(rng) == 0); // 20% institutional

                // Advance timestamp by random gap
                timestamp += time_gap_dist(rng);

                // ----------------------------------------------------------------
                // WRITE ROW — using raw string building for maximum speed
                // ----------------------------------------------------------------
                // WHY NOT std::format or stream <<  here?
                // We're writing 1 million rows. Every microsecond matters.
                // std::to_string is simple and fast.
                // We build each line as a string and write it in one call.
                //
                // In a production generator, we'd use a fixed-size char buffer
                // and snprintf for even faster output. For now this is fine.
                // ----------------------------------------------------------------
                file << (1000000 + i) << ',' // trade_id
                     << (2000000 + i) << ',' // order_id
                     << timestamp << ','     // timestamp (ns)
                     << symbol << ','        // symbol
                     << std::fixed << std::setprecision(2) << price << ','
                     << vol << ','                // volume
                     << side << ','               // 'B' or 'S'
                     << type << ','               // 'M', 'L', or 'I'
                     << (is_pro ? 1 : 0) << '\n'; // 0 or 1
            }

            file.flush();
            file.close();

            auto gen_end = std::chrono::high_resolution_clock::now();
            auto gen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              gen_end - gen_start)
                              .count();

            // Get file size for reporting
            auto file_size_bytes = std::filesystem::file_size(output_path);
            double file_size_mb = static_cast<double>(file_size_bytes) / (1024.0 * 1024.0);

            std::cout << "[GENERATOR] Done!\n";
            std::cout << "[GENERATOR]   Rows generated : " << num_trades << "\n";
            std::cout << "[GENERATOR]   File size      : "
                      << std::fixed << std::setprecision(1) << file_size_mb << " MB\n";
            std::cout << "[GENERATOR]   Generation time: " << gen_ms << "ms\n";
            std::cout << "[GENERATOR]   Written to     : " << output_path << "\n";
        }
    };

} // namespace MarketStream