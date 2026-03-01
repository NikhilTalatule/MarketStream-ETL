#pragma once

// ============================================================================
// TickMessage.hpp — The JSON wire format for the WebSocket tick feed
// ============================================================================
//
// WHY DEFINE A SEPARATE "WIRE FORMAT" STRUCT?
//
// Your Trade struct (Trade.hpp) is the INTERNAL representation.
// It's optimized for C++ performance: fixed-width integers, char fields,
// cache-friendly layout. It's NOT designed for network transmission.
//
// TickMessage is the EXTERNAL representation — what actually travels over
// the WebSocket as JSON text. It maps to/from Trade via serialize/deserialize.
//
// This separation is the DTO pattern (Data Transfer Object):
//   Trade       = domain model   (your code works with this)
//   TickMessage = transport model (the network sees this)
//
// Why not just send raw Trade bytes over the socket?
//   1. Endianness: your server and client might have different byte order.
//      JSON text is endian-neutral — always the same on any machine.
//   2. Versioning: if Trade adds a field, old clients still parse JSON fine.
//      Raw binary: old clients read the wrong bytes if layout changes.
//   3. Debuggability: you can read JSON in Wireshark / netcat.
//      Raw binary: you see garbage hex.
//
// In production HFT: you'd use FlatBuffers or SBE (Simple Binary Encoding)
// for zero-copy binary serialization instead of JSON. But JSON is correct
// for learning the architecture — same pattern, different encoding.
// ============================================================================

#include <string>
#include <nlohmann/json.hpp> // nlohmann JSON — installed via MSYS2
#include "../model/Trade.hpp"

namespace MarketStream
{

    // ============================================================================
    // TickMessage — One tick event serialized as JSON
    // ============================================================================
    // JSON wire format (what travels over the WebSocket):
    // {
    //   "trade_id":  1000042,
    //   "order_id":  2000042,
    //   "timestamp": 1698230401234567890,
    //   "symbol":    "RELIANCE",
    //   "price":     2457.25,
    //   "volume":    500,
    //   "side":      "B",
    //   "type":      "L",
    //   "is_pro":    false
    // }
    //
    // WHY nlohmann::json?
    // It's the most widely-used C++ JSON library. Header-only, no build step.
    // You installed it via MSYS2. It has a clean, expressive API:
    //   json["field"] = value;   // serialize
    //   value = json["field"];   // deserialize
    // ============================================================================
    struct TickMessage
    {
        uint64_t trade_id;
        uint64_t order_id;
        long long timestamp;
        std::string symbol;
        double price;
        uint32_t volume;
        char side; // 'B' or 'S'
        char type; // 'M', 'L', or 'I'
        bool is_pro;

        // ========================================================================
        // to_json() — Serialize TickMessage → JSON string
        // ========================================================================
        // WHY RETURN std::string AND NOT nlohmann::json?
        // We ultimately need BYTES to send over the WebSocket.
        // .dump() converts the json object to a UTF-8 string.
        // Boost.Beast sends strings. So we serialize all the way to string here.
        //
        // nlohmann::json j;        ← creates empty JSON object {}
        // j["trade_id"] = value;   ← adds key-value pair
        // j.dump()                 ← converts to string: {"trade_id":1000042,...}
        // ========================================================================
        [[nodiscard]]
        std::string to_json() const
        {
            nlohmann::json j;
            j["trade_id"] = trade_id;
            j["order_id"] = order_id;
            j["timestamp"] = timestamp;
            j["symbol"] = symbol;
            j["price"] = price;
            j["volume"] = volume;
            j["side"] = std::string(1, side); // char → 1-char string
            j["type"] = std::string(1, type);
            j["is_pro"] = is_pro;
            return j.dump();
        }

        // ========================================================================
        // from_json() — Deserialize JSON string → TickMessage
        // ========================================================================
        // Static factory: TickMessage msg = TickMessage::from_json(text);
        //
        // nlohmann::json::parse(text) throws if text is malformed JSON.
        // We let that exception propagate — the caller handles it.
        // In production: wrap in try/catch and route to a rejection log.
        //
        // j.at("field")    — throws std::out_of_range if field is missing
        // j["field"]       — returns null if field is missing (silent corruption risk)
        // .at() is safer for deserialization: fail loudly, not silently.
        //
        // .get<std::string>()  — extracts as std::string
        // .get<uint64_t>()     — extracts as uint64_t
        // etc. Type checked at runtime.
        // ========================================================================
        [[nodiscard]]
        static TickMessage from_json(const std::string &text)
        {
            auto j = nlohmann::json::parse(text);

            TickMessage msg;
            msg.trade_id = j.at("trade_id").get<uint64_t>();
            msg.order_id = j.at("order_id").get<uint64_t>();
            msg.timestamp = j.at("timestamp").get<long long>();
            msg.symbol = j.at("symbol").get<std::string>();
            msg.price = j.at("price").get<double>();
            msg.volume = j.at("volume").get<uint32_t>();

            // JSON stores side/type as 1-char strings.
            // We extract the first character.
            // .at(0) = first char of string.
            auto side_str = j.at("side").get<std::string>();
            auto type_str = j.at("type").get<std::string>();
            msg.side = side_str.empty() ? 'N' : side_str[0];
            msg.type = type_str.empty() ? 'M' : type_str[0];

            msg.is_pro = j.at("is_pro").get<bool>();
            return msg;
        }

        // ========================================================================
        // to_trade() — Convert TickMessage → Trade (your domain model)
        // ========================================================================
        // Called by the consumer after deserialization.
        // Maps every field from the wire format to the internal Trade struct.
        // 'exchange' defaults to "WSS" (WebSocket Stream) — not in wire format.
        // ========================================================================
        [[nodiscard]]
        Trade to_trade() const
        {
            Trade t{};
            t.trade_id = trade_id;
            t.order_id = order_id;
            t.timestamp = timestamp;
            t.symbol = symbol;
            t.price = price;
            t.volume = volume;
            t.side = side;
            t.type = type;
            t.is_pro = is_pro;
            t.exchange = "WSS"; // WebSocket Stream source identifier
            return t;
        }

        // ========================================================================
        // from_trade() — Convert Trade → TickMessage (for the server to send)
        // ========================================================================
        [[nodiscard]]
        static TickMessage from_trade(const Trade &t)
        {
            TickMessage msg;
            msg.trade_id = t.trade_id;
            msg.order_id = t.order_id;
            msg.timestamp = t.timestamp;
            msg.symbol = t.symbol;
            msg.price = t.price;
            msg.volume = t.volume;
            msg.side = t.side;
            msg.type = t.type;
            msg.is_pro = t.is_pro;
            return msg;
        }
    };

} // namespace MarketStream