#pragma once

#include <string>
#include <vector>
#include <pqxx/pqxx> // The PostgreSQL Driver
#include "../model/Trade.hpp"

namespace MarketStream
{

    class DatabaseLoader
    {
    public:
        // Constructor: Establishes the connection immediately
        DatabaseLoader(const std::string &connection_string);

        // Destructor: Closes connection automatically (RAII)
        ~DatabaseLoader() = default;

        /**
         * @brief Creates the 'trades' table if it doesn't exist.
         * Defines the schema with correct data types (BigInt, Double, Varchar).
         */
        void init_schema();

        /**
         * @brief Uses High-Performance 'COPY' protocol to stream data.
         * * PERFORMANCE NOTE:
         * Instead of 1000 INSERT statements, we open one stream and push
         * data directly. This is the fastest way to load Postgres.
         */
        void bulk_load(const std::vector<Trade> &trades);

    private:
        std::string conn_str;
    };

} // namespace MarketStream