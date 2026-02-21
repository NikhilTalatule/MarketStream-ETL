#pragma once

#include <string>
#include <vector>
#include <pqxx/pqxx>
#include "../model/Trade.hpp"
#include "../indicators/TechnicalIndicators.hpp"  // NEW

namespace MarketStream
{

    class DatabaseLoader
    {
    public:
        DatabaseLoader(const std::string& connection_string);
        ~DatabaseLoader() = default;

        // Creates both tables: 'trades' and 'technical_indicators'
        void init_schema();

        // Streams trades into DB via COPY protocol + staging table
        void bulk_load(const std::vector<Trade>& trades);

        // NEW: Saves computed indicators into technical_indicators table
        void save_indicators(const std::vector<IndicatorResult>& indicators);

    private:
        std::string conn_str;
    };

} // namespace MarketStream