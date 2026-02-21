cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Main ETL pipeline
add_executable(etl_pipeline
    src/main.cpp
    src/parser/CsvParser.cpp
    src/database/DatabaseLoader.cpp
)
target_link_libraries(etl_pipeline PRIVATE pqxx pq)

add_custom_command(
    TARGET etl_pipeline POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv
        ${CMAKE_CURRENT_BINARY_DIR}/sample_data.csv
    COMMENT "Copying sample_data.csv to build directory..."
)

# Database test tool
add_executable(test_db src/test_pg.cpp)
target_link_libraries(test_db PRIVATE pqxx pq)

# -----------------------------------------------------------------------
# NEW: Synthetic data generator tool
# No PostgreSQL needed — pure file I/O
# -----------------------------------------------------------------------
add_executable(generate_data
    src/tools/generate_data.cpp
)
# No pqxx linking needed — generator only writes CSV files