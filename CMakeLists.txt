cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories - FIX: Point to src/ where headers actually are
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

# Find PostgreSQL
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Main executable
add_executable(etl_pipeline 
    src/main.cpp 
    src/parser/CsvParser.cpp
    src/database/DatabaseLoader.cpp
)

# Link libraries
target_link_libraries(etl_pipeline PRIVATE pqxx pq)

# Database Test Tool
add_executable(test_db src/test_pg.cpp)
target_link_libraries(test_db PRIVATE pqxx pq)