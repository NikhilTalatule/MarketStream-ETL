#pragma once

// ============================================================================
// TECHNICAL INDICATORS — WHY THIS MATTERS
//
// Raw trade data (price, volume, timestamp) is the INPUT.
// Technical indicators are the DERIVED SIGNALS that trading algorithms use.
//
// SMA (Simple Moving Average):
//   The average closing price over the last N periods.
//   SMA(5) for RELIANCE = average of last 5 RELIANCE trade prices.
//   WHY: Smooths out noise. A single price spike looks alarming.
//        The 5-period SMA shows the underlying trend.
//   Used by: momentum strategies, trend-following systems, support/resistance.
//
// RSI (Relative Strength Index):
//   A 0–100 oscillator measuring speed and magnitude of price changes.
//   RSI > 70 = overbought (price moved up too fast, likely to reverse down).
//   RSI < 30 = oversold  (price moved down too fast, likely to reverse up).
//   WHY: Every quant fund, every trading terminal, every Bloomberg screen
//        shows RSI. It's a universal signal.
//   Formula:
//     RSI = 100 - (100 / (1 + RS))
//     RS  = Average Gain / Average Loss over the last N periods
//
// VWAP (Volume Weighted Average Price):
//   The average price weighted by how much was traded at each price.
//   VWAP = Sum(price * volume) / Sum(volume)
//   WHY: Institutional traders benchmark their execution against VWAP.
//        "Did we buy below VWAP?" = good execution.
//        "Did we buy above VWAP?" = we paid too much.
// ============================================================================

#include <vector>
#include <string>
#include <unordered_map> // Hash map: O(1) average lookup by symbol
#include <numeric>       // std::accumulate — sum a range of numbers
#include <stdexcept>     // std::invalid_argument
#include <iostream>
#include <iomanip>
#include "../model/Trade.hpp"

namespace MarketStream
{

    // ============================================================================
    // IndicatorResult — One computed indicator value for one symbol
    // ============================================================================
    struct IndicatorResult
    {
        std::string symbol; // Which ticker (e.g., "RELIANCE")
        double sma;         // Simple Moving Average
        double rsi;         // Relative Strength Index (0-100)
        double vwap;        // Volume Weighted Average Price
        int period;         // How many trades were used in calculation
    };

    // ============================================================================
    // TechnicalIndicators — Computes indicators from a vector of Trade objects
    // ============================================================================
    class TechnicalIndicators
    {
    public:
        // ========================================================================
        // compute_all()
        // ========================================================================
        // Takes the full trades vector, groups by symbol, computes all indicators
        // for each symbol, returns one IndicatorResult per symbol.
        //
        // WHY std::unordered_map<string, vector<Trade>>?
        // We need to separate RELIANCE trades from TCS trades from INFY trades.
        // unordered_map gives O(1) average-case lookup and insertion by symbol key.
        // The value is a vector of all trades for that symbol.
        //
        // WHY NOT sort + iterate?
        // Sorting is O(n log n). Hash map grouping is O(n). Faster at scale.
        // ========================================================================
        [[nodiscard]]
        static std::vector<IndicatorResult> compute_all(
            const std::vector<Trade> &trades,
            int period = 5) // Default: use last 5 trades per symbol
        {
            if (trades.empty())
                return {};

            // ------------------------------------------------------------------
            // STEP 1: Group trades by symbol using a hash map
            // ------------------------------------------------------------------
            // unordered_map<KEY, VALUE>
            //   KEY   = std::string (the symbol: "RELIANCE", "TCS", etc.)
            //   VALUE = std::vector<double> (all prices for that symbol, in order)
            //
            // WHY ONLY PRICES AND VOLUMES, NOT FULL TRADES?
            // We only need price and volume for indicators.
            // Storing full Trade structs would waste memory.
            // Separate price and volume vectors = cache-friendly access patterns.
            // When computing SMA, the CPU loads only price doubles, not
            // adjacent symbol strings — stays in L1 cache longer.
            // ------------------------------------------------------------------
            std::unordered_map<std::string, std::vector<double>> prices_by_symbol;
            std::unordered_map<std::string, std::vector<uint32_t>> volumes_by_symbol;

            for (const auto &t : trades)
            {
                prices_by_symbol[t.symbol].push_back(t.price);
                volumes_by_symbol[t.symbol].push_back(t.volume);
            }

            // ------------------------------------------------------------------
            // STEP 2: Compute indicators for each symbol
            // ------------------------------------------------------------------
            std::vector<IndicatorResult> results;
            results.reserve(prices_by_symbol.size());

            for (const auto &[symbol, prices] : prices_by_symbol)
            {
                // Structured binding: [symbol, prices] unpacks the map entry.
                // 'symbol' = the string key, 'prices' = the vector of doubles.
                // This is C++17 syntax — cleaner than .first/.second on pairs.

                const auto &volumes = volumes_by_symbol.at(symbol);

                // Use min(period, available_data) — can't compute SMA(5)
                // if you only have 3 trades for that symbol.
                int effective_period = std::min(period, static_cast<int>(prices.size()));

                IndicatorResult result;
                result.symbol = symbol;
                result.period = effective_period;
                result.sma = compute_sma(prices, effective_period);
                result.rsi = compute_rsi(prices, effective_period);
                result.vwap = compute_vwap(prices, volumes);

                results.push_back(result);
            }

            return results;
        }

        // ========================================================================
        // print_results() — Formatted console output
        // ========================================================================
        static void print_results(const std::vector<IndicatorResult> &results)
        {
            std::cout << "\n";
            std::cout << "╔═══════════════════════════════════════════════════════════╗\n";
            std::cout << "║         MarketStream ETL — Technical Indicators           ║\n";
            std::cout << "╠════════════╦════════════╦════════════╦════════════╦═══════╣\n";
            std::cout << "║ Symbol     ║    SMA     ║    RSI     ║   VWAP     ║ Bars  ║\n";
            std::cout << "╠════════════╬════════════╬════════════╬════════════╬═══════╣\n";

            for (const auto &r : results)
            {
                // RSI interpretation: add a visual signal
                // WHY IN THE OUTPUT? In interviews, showing you understand
                // what the numbers MEAN is more impressive than just printing them.
                std::string rsi_signal;
                if (r.rsi >= 70.0)
                    rsi_signal = " OVERBOUGHT";
                else if (r.rsi <= 30.0)
                    rsi_signal = " OVERSOLD  ";
                else
                    rsi_signal = " NEUTRAL   ";

                std::cout << "║ "
                          << std::left << std::setw(10) << r.symbol
                          << " ║ "
                          << std::right << std::fixed << std::setprecision(2)
                          << std::setw(10) << r.sma
                          << " ║ "
                          << std::right << std::fixed << std::setprecision(1)
                          << std::setw(6) << r.rsi << rsi_signal
                          << " ║ "
                          << std::right << std::fixed << std::setprecision(2)
                          << std::setw(10) << r.vwap
                          << " ║ "
                          << std::right << std::setw(4) << r.period
                          << " ║\n";
            }

            std::cout << "╚════════════╩════════════╩════════════╩════════════╩══════╝\n\n";
        }

    private:
        // ========================================================================
        // compute_sma() — Simple Moving Average
        // ========================================================================
        // Takes the LAST 'period' prices and averages them.
        //
        // WHY LAST N AND NOT ALL?
        // "Moving" average = it moves forward through time.
        // You always look at the most recent N datapoints.
        // The "window" slides forward as new trades arrive.
        //
        // ALGORITHM:
        //   prices = [2456.75, 2457.00, 2458.00]  (sorted oldest to newest)
        //   period = 2
        //   We want the average of the last 2: [2457.00, 2458.00]
        //   SMA = (2457.00 + 2458.00) / 2 = 2457.50
        //
        // COMPLEXITY: O(period) — we only sum the last N elements.
        // ========================================================================
        [[nodiscard]]
        static double compute_sma(const std::vector<double> &prices, int period)
        {
            if (prices.empty() || period <= 0)
                return 0.0;

            // prices.end()         = iterator pointing PAST the last element
            // prices.end() - period = iterator pointing to the START of our window
            // std::accumulate sums from start → end, beginning with 0.0
            //
            // WHY 0.0 AND NOT 0?
            // std::accumulate uses the type of the initial value.
            // Starting with integer 0 would do integer arithmetic — truncating decimals.
            // Starting with 0.0 (double) keeps full floating point precision.
            double sum = std::accumulate(
                prices.end() - period, // Start of window
                prices.end(),          // End of window (past last element)
                0.0                    // Initial value for sum (double!)
            );

            return sum / static_cast<double>(period);
        }

        // ========================================================================
        // compute_rsi() — Relative Strength Index
        // ========================================================================
        // RSI measures whether price moved up more than down (or vice versa)
        // over the last N periods.
        //
        // STEP BY STEP ALGORITHM:
        // 1. Calculate price changes: change[i] = price[i] - price[i-1]
        // 2. Separate into gains (positive changes) and losses (negative changes)
        // 3. Average gain = sum of gains / period
        //    Average loss = sum of |losses| / period  (absolute value)
        // 4. RS = average_gain / average_loss
        // 5. RSI = 100 - (100 / (1 + RS))
        //
        // EXAMPLE:
        //   Prices: 100, 102, 101, 103, 105
        //   Changes: +2, -1, +2, +2
        //   Gains: 2, 0, 2, 2  → avg = 6/4 = 1.5
        //   Losses: 0, 1, 0, 0 → avg = 1/4 = 0.25
        //   RS = 1.5 / 0.25 = 6.0
        //   RSI = 100 - (100 / 7.0) = 85.7  → OVERBOUGHT
        // ========================================================================
        [[nodiscard]]
        static double compute_rsi(const std::vector<double> &prices, int period)
        {
            // Need at least 2 prices to compute one change
            if (prices.size() < 2 || period <= 1)
                return 50.0; // Return neutral RSI

            // Work on the last (period+1) prices to get 'period' changes
            int start_idx = static_cast<int>(prices.size()) - period - 1;
            if (start_idx < 0)
                start_idx = 0;

            double avg_gain = 0.0;
            double avg_loss = 0.0;
            int count = 0;

            for (int i = start_idx + 1; i < static_cast<int>(prices.size()); ++i)
            {
                double change = prices[i] - prices[i - 1];

                if (change > 0.0)
                    avg_gain += change; // Positive change = gain
                else
                    avg_loss += (-change); // Negative change = loss (store as positive)

                count++;
            }

            if (count == 0)
                return 50.0;

            avg_gain /= static_cast<double>(count);
            avg_loss /= static_cast<double>(count);

            // Edge case: if average loss is 0, all moves were upward = RSI = 100
            if (avg_loss == 0.0)
                return 100.0;

            double rs = avg_gain / avg_loss;
            double rsi = 100.0 - (100.0 / (1.0 + rs));

            return rsi;
        }

        // ========================================================================
        // compute_vwap() — Volume Weighted Average Price
        // ========================================================================
        // VWAP = Sum(price_i * volume_i) / Sum(volume_i)
        //
        // WHY WEIGHT BY VOLUME?
        // A trade of 1000 shares at ₹2456 should influence the average MORE
        // than a trade of 10 shares at ₹2460.
        // Simple average treats both equally — wrong.
        // VWAP weights each price by how much was traded at that price — correct.
        //
        // REAL WORLD USE:
        // "We bought 50,000 shares of RELIANCE today at an average of ₹2457.
        //  The VWAP was ₹2456.80. We paid ₹0.20 above VWAP."
        // That ₹0.20 slippage across 50,000 shares = ₹10,000 in execution cost.
        // That's why institutions care deeply about VWAP.
        // ========================================================================
        [[nodiscard]]
        static double compute_vwap(
            const std::vector<double> &prices,
            const std::vector<uint32_t> &volumes)
        {
            if (prices.empty())
                return 0.0;

            double total_value = 0.0;  // Sum of (price * volume)
            double total_volume = 0.0; // Sum of volume

            for (size_t i = 0; i < prices.size(); ++i)
            {
                total_value += prices[i] * static_cast<double>(volumes[i]);
                total_volume += static_cast<double>(volumes[i]);
            }

            if (total_volume == 0.0)
                return 0.0;

            return total_value / total_volume;
        }
    };

} // namespace MarketStream