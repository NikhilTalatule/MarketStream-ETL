cmake_minimum_required(VERSION 3.20)
project(ETLPipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -----------------------------------------------------------------------
# Include Directories
# WHY include_directories?
# The compiler needs to know WHERE to look for header files (.hpp).
# Without this, #include "parser/CsvParser.hpp" would fail because
# the compiler wouldn't know to look inside the src/ folder.
# -----------------------------------------------------------------------
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/third_party/ctre/include)

# Find PostgreSQL system installation and grab its include path
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# -----------------------------------------------------------------------
# Main Executable
# Lists ALL .cpp files that make up our program.
# The compiler compiles each one separately into a .obj file,
# then the LINKER combines all .obj files into one .exe
# -----------------------------------------------------------------------
add_executable(etl_pipeline
    src/main.cpp
    src/parser/CsvParser.cpp
    src/database/DatabaseLoader.cpp
)

# Link against pqxx (C++ PostgreSQL driver) and pq (the C driver it wraps)
# WHY both? pqxx is built ON TOP of pq. Like a car body on a chassis —
# you need both to drive.
target_link_libraries(etl_pipeline PRIVATE pqxx pq)

# -----------------------------------------------------------------------
# THE FIX: Auto-copy sample_data.csv into the build directory
# -----------------------------------------------------------------------
# WHY THIS WORKS:
# add_custom_command with POST_BUILD runs a shell command AFTER the
# executable is compiled successfully.
# CMAKE_CURRENT_SOURCE_DIR = D:/ETL-Pipeline  (where CMakeLists.txt is)
# CMAKE_CURRENT_BINARY_DIR = D:/ETL-Pipeline/build  (where .exe lands)
#
# ${CMAKE_COMMAND} -E copy_if_different:
#   This is a portable cross-platform file copy built into CMake itself.
#   "copy_if_different" = only copy if source is newer than destination.
#   This avoids unnecessary work on every build if the file hasn't changed.
#   We use ${CMAKE_COMMAND} instead of 'copy' or 'cp' because this works
#   identically on Windows, Linux, and Mac without changing the CMakeLists.
# -----------------------------------------------------------------------
add_custom_command(
    TARGET etl_pipeline POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv
        ${CMAKE_CURRENT_BINARY_DIR}/sample_data.csv
    COMMENT "Copying sample_data.csv to build directory..."
)

# -----------------------------------------------------------------------
# Database Test Tool (separate executable for connection testing)
# -----------------------------------------------------------------------
add_executable(test_db src/test_pg.cpp)
target_link_libraries(test_db PRIVATE pqxx pq)