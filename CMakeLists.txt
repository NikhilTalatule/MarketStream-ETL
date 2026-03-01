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

# ─── ADD THIS BLOCK to your existing CMakeLists.txt ─────────────────────────
# Place it AFTER the generate_data executable block, at the bottom of the file.

# ─── Phase 11: SPSC Ring Buffer Benchmark ─────────────────────────────────────
# Pure C++20 standard library — no external dependencies needed.
# SPSCQueue.hpp lives in src/threading/ alongside ThreadPool.hpp
add_executable(spsc_benchmark
    src/tools/spsc_benchmark.cpp
)
# No target_link_libraries needed: pure C++20 standard library only.
# std::atomic, std::thread, std::optional — all built into GCC's stdlib.

# ─── Phase 12: False Sharing Benchmark ───────────────────────────────────────
add_executable(false_sharing_benchmark
    src/tools/false_sharing_benchmark.cpp
)
# ─── Phase 14: WebSocket Real-Time Feed Demo ──────────────────────────────────
#
# Boost.Beast is HEADER-ONLY — no libboost_beast to link against.
# But it DEPENDS ON Boost.Asio (also header-only) which DEPENDS ON Boost.System.
# Boost.System IS a compiled library (.dll/.a) that we must link.
#
# WHY find_package(Boost REQUIRED COMPONENTS system)?
#   COMPONENTS = we only need the 'system' compiled library.
#   Boost::system sets include paths and link flags automatically.
#
# WHY nlohmann_json::nlohmann_json?
#   This is an INTERFACE target — header-only, no .dll to link.
#   Just adds the include path so #include <nlohmann/json.hpp> resolves.
#
# WHY ws2_32 and wsock32?
#   Windows does NOT include socket support by default in the CRT.
#   ws2_32.lib = Winsock 2 (TCP/IP, accept, connect, send, recv).
#   wsock32.lib = older Winsock 1 compatibility (Boost.Asio needs this on some MSYS2 builds).
#   Linux: these are built into libc — no explicit linking needed.
#
# WHY _WIN32_WINNT=0x0601?
#   Boost.Asio uses Windows Vista+ APIs.
#   _WIN32_WINNT tells the Windows headers which API version to expose.
#   0x0601 = Windows 7 (covers all modern Windows including your Win11).
#   Without this: MSYS2 may default to an older value causing compile errors.

find_package(Boost REQUIRED COMPONENTS system)
find_package(nlohmann_json REQUIRED)

add_executable(websocket_demo
    src/tools/websocket_demo.cpp
)

target_compile_definitions(websocket_demo PRIVATE
    _WIN32_WINNT=0x0601   # Windows 7+ APIs (required by Boost.Asio on Windows)
)

target_link_libraries(websocket_demo PRIVATE
    Boost::system           # Boost.System (compiled) — required by Boost.Asio
    nlohmann_json::nlohmann_json  # JSON parsing (header-only interface target)
    ws2_32                  # Winsock 2 — TCP/IP on Windows
    wsock32                 # Winsock 1 compat — some Boost.Asio internals need this
)