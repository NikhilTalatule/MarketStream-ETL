cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

# Find PostgreSQL
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Main executable
add_executable(etl_pipeline 
    src/main.cpp 
    src/parser/CsvParser.cpp
)

# Link libraries
target_link_libraries(etl_pipeline PRIVATE pqxx pq)

# --- Add the Database Test Tool ---
# 1. Create a new executable named "test_db" from the file "src/test_pg.cpp"
add_executable(test_db src/test_pg.cpp)

# 2. Link it to the PostgreSQL libraries (pqxx and pq)
target_link_libraries(test_db PRIVATE pqxx pq)

