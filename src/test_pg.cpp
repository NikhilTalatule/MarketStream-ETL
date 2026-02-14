#include <iostream>
#include <pqxx/pqxx>

int main()
{
    try
    {
        // Update with your pgAdmin credentials
        pqxx::connection conn(
            "host=localhost "
            "port=5432 "
            "dbname=etl_pipeline_db "
            "user=postgres "
            "password=Nikhil@10 " // ← Change this!
        );

        if (conn.is_open())
        {
            std::cout << "✅ Connected to PostgreSQL!\n";
            std::cout << "Database: " << conn.dbname() << "\n";

            // Test query
            pqxx::work txn(conn);
            pqxx::result res = txn.exec("SELECT version();");
            std::cout << "PostgreSQL version:\n"
                      << res[0][0].c_str() << "\n";

            txn.commit();
        }
        else
        {
            std::cout << "❌ Failed to connect\n";
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
