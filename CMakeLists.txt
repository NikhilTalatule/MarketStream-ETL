cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ─── Include Paths ────────────────────────────────────────────────────────────
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

# ─── PostgreSQL ───────────────────────────────────────────────────────────────
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# ─── Apache Arrow + Parquet ───────────────────────────────────────────────────
# WHY find_package(Arrow)?
# Arrow ships with CMake config files (ArrowConfig.cmake).
# find_package reads these files and sets:
#   Arrow_FOUND        — whether Arrow was found
#   Arrow::arrow_shared — the imported target (handles include + link automatically)
#
# WHY find_package(Parquet)?
# Parquet is a SEPARATE library that depends on Arrow.
# It ships as parquet (the C++ reader/writer) and
# parquet_arrow (the bridge between Arrow tables and Parquet files).
# We need BOTH.
#
# REQUIRED = CMake stops with an error if not found.
# Better than a cryptic linker error 20 lines later.
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# ─── Main ETL Pipeline Executable ────────────────────────────────────────────
add_executable(etl_pipeline
    src/main.cpp
    src/parser/CsvParser.cpp
    src/database/DatabaseLoader.cpp
    src/output/ParquetWriter.cpp      # Phase 10: Parquet output
)

# target_link_libraries: links ALL required libraries
#
# pqxx pq           — PostgreSQL C++ driver + C library (same as before)
# Arrow::arrow_shared   — Apache Arrow runtime (columnar memory + arrays)
# Parquet::parquet_shared — Parquet file reader/writer
#
# WHY "PRIVATE"?
# PRIVATE = these libraries are needed to build etl_pipeline,
# but NOT needed by anyone who links against etl_pipeline.
# (etl_pipeline is an exe, not a lib, so PRIVATE is always correct here.)
target_link_libraries(etl_pipeline PRIVATE
    pqxx
    pq
    Arrow::arrow_shared
    Parquet::parquet_shared
)

# ─── Post-build: copy sample CSV to build dir ─────────────────────────────────
add_custom_command(
    TARGET etl_pipeline POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv
        ${CMAKE_CURRENT_BINARY_DIR}/sample_data.csv
    COMMENT "Copying sample_data.csv to build directory..."
)

# ─── Database test tool ───────────────────────────────────────────────────────
add_executable(test_db src/test_pg.cpp)
target_link_libraries(test_db PRIVATE pqxx pq)

# ─── Synthetic data generator ─────────────────────────────────────────────────
# Pure file I/O — no PostgreSQL, no Arrow needed
add_executable(generate_data
    src/tools/generate_data.cpp
)
# No extra libraries: generates only a CSV file