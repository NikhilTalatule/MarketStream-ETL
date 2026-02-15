#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple>
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

            // SQL: Create Table matching our C++ Struct
            // We use 'bigint' for timestamps and IDs to match uint64_t
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
            throw; // Re-throw so main() knows we failed
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

            // High-Performance Stream (COPY command)
            // We specify exactly which columns we are writing to.
            pqxx::stream_to stream = pqxx::stream_to::table(W,
                                                            "trades",
                                                            {"symbol", "price", "volume", "timestamp", "side", "trade_id"});

            // Iterate and shove data into the pipe
            for (const auto &t : trades)
            {
                // We send data in the EXACT order of the columns list above
                stream << std::make_tuple(
                    t.symbol,
                    t.price,
                    t.volume,
                    t.timestamp,
                    std::string(1, t.side), // Convert char to string for DB
                    t.trade_id);
            }

            // Close the stream (flushes data to DB)
            stream.complete();

            W.commit(); // Make it permanent
            std::cout << "[DB] Successfully loaded " << trades.size() << " trades.\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[DB ERROR] Bulk load failed: " << e.what() << "\n";
            throw;
        }
    }

} // namespace MarketStream