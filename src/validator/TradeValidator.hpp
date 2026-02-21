#pragma once

// ============================================================================
// WHAT IS CTRE?
// CTRE = Compile-Time Regular Expressions (by Hana Dusíková)
// A regular expression (regex) is a pattern that describes valid text.
// Example: "only uppercase letters, 1 to 10 characters" = regex [A-Z]{1,10}
//
// Normal std::regex (built into C++) compiles the pattern at RUNTIME —
// every time your program runs, it builds a state machine from the pattern string.
// This takes microseconds per check — unacceptable in HFT.
//
// CTRE compiles the pattern at COMPILE TIME — the compiler itself builds
// the state machine once when you compile your code. At runtime, checking
// a symbol takes nanoseconds. 50x+ faster than std::regex.
//
// WHY THIS MATTERS FOR HFT:
// We receive 1 million+ trade events per second from exchanges.
// At 1M events/sec, 1 microsecond per validation = 1 second of pure validation
// overhead. CTRE brings that to ~20ms. That gap is the difference between
// keeping up with the feed and falling behind.
// ============================================================================
#include <ctre.hpp>    // The CTRE library (already in third_party/ctre/)
#include <string_view> // Zero-copy string viewing
#include <string>      // std::string for error messages
#include "../model/Trade.hpp"

namespace MarketStream
{

    // ============================================================================
    // ValidationResult — What a validation check returns
    // ============================================================================
    // WHY A STRUCT INSTEAD OF JUST RETURNING bool?
    // If validation fails, you need to know WHY.
    // Just returning false tells you nothing. Did price fail? Symbol fail?
    // A struct carries BOTH: did it pass, AND if not, what failed.
    // This is how production systems emit structured error logs.
    // ============================================================================
    struct ValidationResult
    {
        bool valid;         // true = trade is clean, false = rejected
        std::string reason; // empty if valid, description of failure if not

        // -----------------------------------------------------------------------
        // Static factory methods — clean way to create pass/fail results
        // -----------------------------------------------------------------------
        // WHY STATIC METHODS INSTEAD OF CONSTRUCTOR?
        // ValidationResult::ok()         reads like English: "return ok result"
        // ValidationResult::fail("msg")  reads like English: "return fail result"
        // Compare to: ValidationResult(true, "") — less clear what true means here.
        // This pattern is called "Named Constructor Idiom" — common in modern C++.
        // -----------------------------------------------------------------------
        static ValidationResult ok()
        {
            return {true, ""};
        }

        static ValidationResult fail(std::string reason)
        {
            // std::move here transfers ownership of the string into the struct
            // instead of copying it. Small optimization but correct habit.
            return {false, std::move(reason)};
        }
    };

    // ============================================================================
    // TradeValidator — Validates a Trade struct before DB insertion
    // ============================================================================
    class TradeValidator
    {
    public:
        // ========================================================================
        // CTRE PATTERN DEFINITIONS
        // ========================================================================
        // These are defined as static constexpr — meaning they exist exactly once
        // (static) and are computed at compile time (constexpr).
        //
        // WHAT IS constexpr?
        // Normal variables are computed when your program RUNS.
        // constexpr variables are computed when your code COMPILES.
        // The compiled .exe already has the answer baked in — zero runtime cost.
        //
        // PATTERN BREAKDOWN:
        // ctre::match<"PATTERN">(text)
        //   Returns a match object (truthy if matched, falsy if not)
        //   The pattern is checked character by character against 'text'.
        //
        // REGEX SYNTAX USED HERE (don't memorize, just understand the shape):
        //   [A-Z]    = any single uppercase letter (A, B, C ... Z)
        //   {1,10}   = between 1 and 10 of the previous thing
        //   +        = one or more of the previous thing
        //   [A-Z]{1,10}  = 1 to 10 uppercase letters — valid ticker symbol
        // ========================================================================

        // Validates a single Trade. Returns ok() or fail("reason").
        [[nodiscard]]
        static ValidationResult validate(const Trade &trade)
        {
            // ------------------------------------------------------------------
            // CHECK 1: Symbol format
            // ------------------------------------------------------------------
            // Valid symbols: "RELIANCE", "TCS", "ICICIBANK"
            // Invalid: "reliance" (lowercase), "RE LIANCE" (space), "" (empty)
            //
            // ctre::match<"[A-Z]{1,10}"> returns a match object.
            // The 'if (!match)' checks if the match FAILED (pattern didn't fit).
            // ------------------------------------------------------------------
            if (!ctre::match<"[A-Z]{1,10}">(trade.symbol))
            {
                // std::format is C++20's type-safe string formatting.
                // Like printf but the compiler checks your types.
                // {} = placeholder, fills in the value of trade.symbol.
                return ValidationResult::fail(
                    "Invalid symbol: '" + trade.symbol + "' — must be 1-10 uppercase letters");
            }

            // ------------------------------------------------------------------
            // CHECK 2: Price sanity
            // ------------------------------------------------------------------
            // price <= 0: clearly corrupted data (a stock can't cost zero or negative)
            // price >= 1,000,000: no Indian or US stock costs 10 lakh per share.
            //   If we see this, the feed sent garbage. Reject it before it corrupts
            //   your average price calculations or triggers false alerts.
            // ------------------------------------------------------------------
            if (trade.price <= 0.0 || trade.price >= 1'000'000.0)
            {
                // C++14+ digit separator: 1'000'000 = 1000000, just more readable.
                // The apostrophe is ignored by the compiler — purely visual.
                return ValidationResult::fail(
                    "Invalid price: " + std::to_string(trade.price) +
                    " — must be between 0 and 1,000,000");
            }

            // ------------------------------------------------------------------
            // CHECK 3: Volume must be positive
            // ------------------------------------------------------------------
            // A trade of 0 shares makes no sense. Reject immediately.
            // This also catches the case where from_chars failed to parse
            // the volume field and left it at 0 (zero-initialized default).
            // ------------------------------------------------------------------
            if (trade.volume == 0)
            {
                return ValidationResult::fail("Invalid volume: 0 — must be > 0");
            }

            // ------------------------------------------------------------------
            // CHECK 4: Side must be B, S, or N
            // ------------------------------------------------------------------
            // Any other character means the feed sent corrupt data.
            // 'N' = unknown (we allow it but log it as a warning in main).
            // ------------------------------------------------------------------
            if (trade.side != 'B' && trade.side != 'S' && trade.side != 'N')
            {
                return ValidationResult::fail(
                    std::string("Invalid side: '") + trade.side + "' — must be B, S, or N");
            }

            // ------------------------------------------------------------------
            // CHECK 5: Order type must be M, L, or I
            // ------------------------------------------------------------------
            if (trade.type != 'M' && trade.type != 'L' && trade.type != 'I')
            {
                return ValidationResult::fail(
                    std::string("Invalid type: '") + trade.type + "' — must be M, L, or I");
            }

            // ------------------------------------------------------------------
            // CHECK 6: Timestamp must be positive
            // ------------------------------------------------------------------
            // A timestamp of 0 means parsing failed (zero-init default).
            // Real exchange timestamps are nanoseconds since 1970 — always huge positive.
            if (trade.timestamp <= 0)
            {
                return ValidationResult::fail(
                    "Invalid timestamp: " + std::to_string(trade.timestamp) +
                    " — must be positive nanoseconds since epoch");
            }

            // All checks passed
            return ValidationResult::ok();
        }

        // ========================================================================
        // validate_batch() — Validates an entire vector of trades
        // ========================================================================
        // Returns only the VALID trades. Rejects are logged and discarded.
        //
        // WHY RETURN A NEW VECTOR INSTEAD OF MODIFYING IN-PLACE?
        // Modifying a vector while iterating it is dangerous — easy to skip elements.
        // Returning a new vector is the "functional" approach: input is unchanged,
        // output is clean data. This is safer and easier to reason about.
        //
        // In HFT production systems, rejected trades go to a "dead letter queue"
        // for human review. We'll add that as a Phase 3 enhancement.
        // ========================================================================
        [[nodiscard]]
        static std::vector<Trade> validate_batch(const std::vector<Trade> &trades)
        {
            std::vector<Trade> valid_trades;

            // Reserve space upfront — if most trades are valid (typical case),
            // we avoid repeated memory reallocations as the vector grows.
            // WHY RESERVE?
            // std::vector doubles its capacity when full. Without reserve(),
            // for 1000 trades: ~10 reallocations + copies. With reserve(1000):
            // exactly 1 allocation. For 1M trades, this difference is significant.
            valid_trades.reserve(trades.size());

            int rejected_count = 0;

            for (const auto &trade : trades)
            {
                auto result = validate(trade);

                if (result.valid)
                {
                    // std::move transfers the Trade into valid_trades
                    // without copying the symbol string.
                    valid_trades.push_back(trade);
                }
                else
                {
                    rejected_count++;
                    // In production this would go to a structured logger.
                    // For now, stderr is correct — rejections are warnings, not crashes.
                    std::cerr << "[VALIDATOR] REJECTED trade_id=" << trade.trade_id
                              << " | Reason: " << result.reason << "\n";
                }
            }

            std::cout << "[VALIDATOR] Batch complete: "
                      << valid_trades.size() << " valid, "
                      << rejected_count << " rejected.\n";

            return valid_trades;
        }
    };

} // namespace MarketStream