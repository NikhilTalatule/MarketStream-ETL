cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include CTRE
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

# Find PostgreSQL
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Main executable
add_executable(etl_pipeline src/main.cpp)

# Link libraries
target_link_libraries(etl_pipeline PRIVATE pqxx pq)
