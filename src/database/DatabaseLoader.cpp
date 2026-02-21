#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple> // Required for std::make_tuple

namespace MarketStream
{

    DatabaseLoader::DatabaseLoader(const std::string &connection_string)
        : conn_str(connection_string) {}

    void DatabaseLoader::init_schema()
    {
        try
        {
            pqxx::connection C(conn_str);
            pqxx::work W(C);

            W.exec(R"(
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id BIGINT,
                    order_id BIGINT,
                    timestamp BIGINT,
                    symbol VARCHAR(10),
                    price DOUBLE PRECISION,
                    volume INTEGER,
                    side CHAR(1),
                    type CHAR(1),
                    is_pro BOOLEAN
                );
            )");

            W.commit();
            std::cout << "[DB] Schema initialized (Table 'trades' ready).\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Init Schema failed: " << e.what() << "\n";
            throw;
        }
    }

    void DatabaseLoader::bulk_load(const std::vector<Trade> &trades)
    {
        if (trades.empty())
            return;

        try
        {
            pqxx::connection C(conn_str);
            pqxx::work W(C);

            // --- FIXED SECTION START ---
            // Old Way (Error): pqxx::stream_to stream = pqxx::stream_to::table(...)
            // New Way (Correct): Use the constructor directly
            // In bulk_load, update the stream_to constructor AND the tuple:

            pqxx::stream_to stream(W,
                                   "trades",
                                   std::vector<std::string>{
                                       "trade_id", "order_id", "timestamp",
                                       "symbol", "price", "volume",
                                       "side", "type", "is_pro"});

            for (const auto &t : trades)
            {
                stream << std::make_tuple(
                    t.trade_id,
                    t.order_id,
                    t.timestamp,
                    t.symbol,
                    t.price,
                    (int)t.volume, // Cast uint32_t → int for pqxx compatibility
                    std::string(1, t.side),
                    std::string(1, t.type),
                    t.is_pro);
            }

            stream.complete();
            W.commit();
            std::cout << "[DB] Successfully loaded " << trades.size() << " trades.\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Bulk load failed: " << e.what() << "\n";
            throw;
        }
    }

} // namespace MarketStream