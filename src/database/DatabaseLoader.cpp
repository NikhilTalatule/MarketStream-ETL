#include "DatabaseLoader.hpp"
#include <iostream>
#include <tuple> // std::make_tuple — packages multiple values into one object
                 // Think of it like putting multiple items into a single box
                 // so pqxx can receive all column values for one row at once.

namespace MarketStream
{

    // =========================================================================
    // CONSTRUCTOR
    // =========================================================================
    // WHY ": conn_str(connection_string)" SYNTAX?
    // This is called a "Member Initializer List". It initializes the private
    // member 'conn_str' BEFORE the constructor body runs.
    // It is faster than writing "conn_str = connection_string;" inside {}.
    // Assignment inside {} = construct empty string, THEN copy into it (2 steps).
    // Initializer list = construct string directly with the value (1 step).
    // Always prefer initializer lists — this is standard C++ practice.
    // =========================================================================
    DatabaseLoader::DatabaseLoader(const std::string &connection_string)
        : conn_str(connection_string) {}

    // =========================================================================
    // init_schema() — Creates the 'trades' table in PostgreSQL
    // =========================================================================
    void DatabaseLoader::init_schema()
    {
        try
        {
            // pqxx::connection opens a TCP connection to the PostgreSQL server.
            // When this object goes out of scope (end of function), the
            // destructor automatically closes the connection. This is RAII:
            // Resource Acquisition Is Initialization. No manual cleanup needed.
            pqxx::connection C(conn_str);

            // pqxx::work is a "transaction guard".
            // WHY TRANSACTIONS? In a database, multiple operations are grouped.
            // If your program crashes halfway through, a transaction ensures
            // the database ROLLS BACK to the safe state before you started.
            // Think of it as "draft mode" — nothing is permanent until .commit().
            pqxx::work W(C);

            // R"(...)" is a Raw String Literal in C++.
            // Normal strings need backslashes: "line1\nline2"
            // Raw strings let you write multi-line SQL directly as-is.
            // Perfect for embedding SQL — it reads exactly like SQL would
            // in pgAdmin.
            W.exec(R"(
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id  BIGINT,
                    order_id  BIGINT,
                    timestamp BIGINT,
                    symbol    VARCHAR(10),
                    price     DOUBLE PRECISION,
                    volume    INTEGER,
                    side      CHAR(1),
                    type      CHAR(1),
                    is_pro    BOOLEAN
                );
            )");

            // .commit() = "make it permanent". Nothing is written to disk
            // until this line. If an exception was thrown above, commit()
            // never runs, and PostgreSQL automatically undoes everything.
            W.commit();
            std::cout << "[DB] Schema initialized (Table 'trades' ready).\n";
        }
        catch (const std::exception &e)
        {
            // We catch by const reference to avoid copying the exception object.
            // 'throw;' re-throws the SAME exception up to the caller (main.cpp).
            // This lets main.cpp decide what to do (print error and exit).
            std::cerr << "[DB ERROR] Init Schema failed: " << e.what() << "\n";
            throw;
        }
    }

    // =========================================================================
    // bulk_load() — Streams ALL trades into PostgreSQL in one COPY operation
    // =========================================================================
    // WHY "COPY" INSTEAD OF "INSERT"?
    //
    // INSERT approach (slow):
    //   INSERT INTO trades VALUES (1, 2, ...);   ← round trip 1
    //   INSERT INTO trades VALUES (3, 4, ...);   ← round trip 2
    //   ... repeated 10,000 times ...
    // Each INSERT = parse SQL + plan query + execute + write WAL log.
    // For 10,000 rows: ~10,000 round trips to the server.
    //
    // COPY approach (fast — what we use):
    //   Opens ONE binary stream to PostgreSQL.
    //   Pumps all rows in as raw data.
    //   PostgreSQL receives them like reading a file — no SQL parsing per row.
    // For 10,000 rows: 1 stream open + 10,000 data writes + 1 stream close.
    // Speed difference: COPY is typically 10x–50x faster than individual INSERTs.
    // This is the SAME protocol PostgreSQL uses internally for pg_dump/pg_restore.
    // =========================================================================
    void DatabaseLoader::bulk_load(const std::vector<Trade> &trades)
    {
        // Guard clause: if someone calls bulk_load with no data, exit immediately.
        // WHY? Sending an empty COPY stream to PostgreSQL can cause errors.
        if (trades.empty())
        {
            std::cout << "[DB] No trades to load.\n";
            return;
        }

        try
        {
            // Open a fresh connection for the bulk load operation.
            pqxx::connection C(conn_str);
            pqxx::work W(C);

            // =================================================================
            // THE KEY FIX: pqxx::stream_to::table() factory method
            // =================================================================
            // WHY DID THE OLD CODE FAIL?
            // The old code used: pqxx::stream_to stream(W, "trades", vector{...})
            // This 3-argument constructor was DEPRECATED then REMOVED in libpqxx 7+.
            // The library now enforces a factory method pattern for safety.
            //
            // WHAT IS A FACTORY METHOD?
            // Instead of calling "new Car()" directly, you call "Car::create()".
            // The factory can do validation/setup before creating the object.
            // pqxx::stream_to::table() is that factory — it validates the table
            // path and column names before opening the COPY stream.
            //
            // SYNTAX BREAKDOWN:
            //   pqxx::stream_to::table(
            //       W,             ← The active transaction (required)
            //       {"trades"},    ← Table path as an initializer list
            //                        {"schema", "table"} for schema-qualified tables
            //                        {"trades"} for default public schema
            //       {"col1",...}   ← Column names — ORDER MUST MATCH tuple below!
            //   )
            //
            // WHY 'auto' FOR THE TYPE?
            // The return type of stream_to::table() is complex internally.
            // Writing it out fully would be: pqxx::stream_to stream = ...
            // Using 'auto' tells the compiler "figure out the type for me".
            // This is idiomatic modern C++. Always use auto for complex types.
            // =================================================================
            auto stream = pqxx::stream_to::table(
                W,
                {"trades"}, // Table name in { } = pqxx::table_path initializer
                {           // Column names — must be in the SAME ORDER as the tuple
                 "trade_id",
                 "order_id",
                 "timestamp",
                 "symbol",
                 "price",
                 "volume",
                 "side",
                 "type",
                 "is_pro"});

            // =================================================================
            // STREAMING LOOP
            // =================================================================
            // 'const auto&' = read-only reference. We do NOT copy each Trade.
            // '&' means reference (an alias to the original object in the vector).
            // 'const' means we promise not to modify it.
            // Without '&': each iteration would COPY the Trade struct = slow.
            // With 'const auto&': zero copies, maximum speed.
            // =================================================================
            for (const auto &t : trades)
            {
                // std::make_tuple packages multiple values into a single object.
                // WHY TUPLE AND NOT JUST PASSING VALUES?
                // pqxx's stream_to operator<< is designed to accept a tuple.
                // A tuple is a fixed-size collection of different types.
                // Here it's: (uint64, uint64, long long, string, double, int, string, string, bool)
                // pqxx knows how to serialize each type to the PostgreSQL wire format.
                //
                // TYPE CAST NOTES:
                // (int)t.volume — volume is uint32_t in our struct, but PostgreSQL
                //   INTEGER is signed 32-bit. The cast tells pqxx to send it as int.
                //   Alternatively we could use static_cast<int>(t.volume) which is
                //   the safer C++ style (we'll use that — explained below).
                //
                // std::string(1, t.side) — t.side is a char ('B' or 'S').
                //   pqxx serializes std::string to CHAR/VARCHAR cleanly.
                //   A single char might confuse pqxx's type system, so we
                //   wrap it in a 1-character string. std::string(1, 'B') = "B".
                stream << std::make_tuple(
                    t.trade_id,
                    t.order_id,
                    t.timestamp,
                    t.symbol,
                    t.price,
                    static_cast<int>(t.volume), // uint32_t → int (safe, values fit)
                    std::string(1, t.side),     // char → string for pqxx
                    std::string(1, t.type),     // char → string for pqxx
                    t.is_pro                    // bool → PostgreSQL BOOLEAN
                );
            }

            // .complete() MUST be called before .commit()
            // WHY? The COPY stream is still "open" after the loop.
            // complete() sends the EOF signal to PostgreSQL, telling it
            // "no more rows are coming, finalize the COPY operation".
            // If you call commit() without complete(), PostgreSQL will
            // reject the transaction because the stream is still open.
            stream.complete();

            // NOW we commit — everything gets written to disk permanently.
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