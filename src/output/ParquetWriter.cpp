#include "ParquetWriter.hpp"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <chrono>
#include <ctime>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

// ── Error handling macro ──────────────────────────────────────────────────────
// Arrow operations return arrow::Status.
// This macro checks the status and throws std::runtime_error if it failed.
// We define it in the .cpp (not .hpp) so it doesn't pollute other translation units.
//
// do { ... } while(0) — the standard way to write a multi-statement macro.
// Without it: THROW_IF_NOT_OK(x); if(y) would bind the if to the inner if,
// not the outer call site. The do-while wrapper makes it behave like a function call.
#define THROW_IF_NOT_OK(expr)                             \
    do                                                    \
    {                                                     \
        ::arrow::Status _s = (expr);                      \
        if (!_s.ok())                                     \
        {                                                 \
            throw std::runtime_error(                     \
                std::string("[PARQUET ERROR] ") + #expr + \
                " -> " + _s.ToString());                  \
        }                                                 \
    } while (0)

namespace MarketStream
{

    // =========================================================================
    // make_output_path()
    // =========================================================================
    // Generates: trades_YYYYMMDD_HHMMSS.parquet in the given directory.
    //
    // WHY TIMESTAMP IN THE FILENAME?
    // Each pipeline run produces a NEW Parquet file.
    // Timestamped filenames = natural partitioning:
    //   s3://my-bucket/trades/trades_20241025_091500.parquet
    //   s3://my-bucket/trades/trades_20241025_093000.parquet
    // Athena can filter by date prefix without scanning all files.
    // This is the Hive partitioning pattern used universally in data lakes.
    // =========================================================================
    std::filesystem::path ParquetWriter::make_output_path(
        const std::filesystem::path &directory)
    {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);

        // std::tm = broken-down time (year, month, day, hour, min, sec)
        // localtime_s (Windows) / localtime_r (Linux) are the thread-safe versions.
        // std::localtime() is NOT thread-safe — it returns a pointer to a static
        // internal buffer. Two threads calling it simultaneously would corrupt
        // each other's result. The _s and _r variants write to a caller-supplied buffer.
        std::tm tm_now{};
#ifdef _WIN32
        localtime_s(&tm_now, &time_t_now);
#else
        localtime_r(&time_t_now, &tm_now);
#endif

        std::ostringstream oss;
        oss << "trades_"
            << std::put_time(&tm_now, "%Y%m%d_%H%M%S")
            << ".parquet";

        return directory / oss.str();
    }

    // =========================================================================
    // write()
    // =========================================================================
    // THE CORE TRANSFORMATION: vector<Trade> (row layout) → Parquet (columnar)
    //
    // PIPELINE (5 steps):
    //   1. Define Arrow Schema     — column names + types
    //   2. Create Array Builders   — one mutable staging area per column
    //   3. Fill Builders           — iterate trades ONCE, push each field to its column
    //   4. Finalize Arrays         — builders → immutable Arrow Arrays
    //   5. Write Parquet file      — Arrow Table → compressed Parquet on disk
    // =========================================================================
    long long ParquetWriter::write(
        const std::vector<Trade> &trades,
        const std::filesystem::path &output_path)
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        const size_t n = trades.size();

        std::cout << "[PARQUET] Converting " << n
                  << " trades to columnar format...\n";

        // ─────────────────────────────────────────────────────────────────────
        // STEP 1: DEFINE ARROW SCHEMA
        // ─────────────────────────────────────────────────────────────────────
        // Schema = list of (column_name, Arrow_type) pairs.
        // This gets embedded in the Parquet file footer so any reader
        // (Spark, Pandas, DuckDB) knows the exact type of each column.
        //
        // KEY DECISION — dictionary(int32, utf8) for symbol/side/type:
        //
        //   Plain utf8 stores:
        //     ["RELIANCE", "TCS", "RELIANCE", "INFY", "RELIANCE", ...]
        //     = 1,000,000 string copies × avg 7 bytes = ~7 MB
        //
        //   dictionary(int32, utf8) stores:
        //     Dictionary:  {0:"RELIANCE", 1:"TCS", 2:"INFY", ...}  ← 10 strings only
        //     Indices:     [0, 1, 0, 2, 0, 1, ...]  ← 1M × int32 = 4 MB
        //     But Parquet ALSO applies RLE encoding on top of dictionary:
        //     [RELIANCE×100000, TCS×90000, ...] → run-length pairs → ~0.1 MB
        //
        //   Result: symbol column goes from ~7 MB → ~0.1 MB in Parquet file.
        //   Same applies to side (only 'B','S') and type (only 'M','L','I').
        //
        // WHY int32 (not int8)?
        //   Arrow 23 renamed StringDictionary8Builder and the API for int8
        //   dictionary is more verbose. int32 handles up to 2 billion unique
        //   values — overkill for 10 symbols, but the compressed file size
        //   difference is negligible (Parquet RLE compresses both to ~0.1 MB).
        //   Using int32 gives us the simpler, stable Arrow 23 API.
        // ─────────────────────────────────────────────────────────────────────
        auto schema = arrow::schema({arrow::field("trade_id", arrow::uint64()),
                                     arrow::field("order_id", arrow::uint64()),
                                     arrow::field("timestamp", arrow::int64()), // nanoseconds since epoch
                                     arrow::field("symbol", arrow::dictionary(arrow::int8(), arrow::utf8())),
                                     arrow::field("price", arrow::float64()),
                                     arrow::field("volume", arrow::uint32()),
                                     arrow::field("side", arrow::dictionary(arrow::int8(), arrow::utf8())),
                                     arrow::field("type", arrow::dictionary(arrow::int8(), arrow::utf8())),
                                     arrow::field("is_pro", arrow::boolean())});

        // ─────────────────────────────────────────────────────────────────────
        // STEP 2: CREATE ARRAY BUILDERS
        // ─────────────────────────────────────────────────────────────────────
        // Builder pattern: mutable staging → immutable Array.
        // Think of each Builder as a growable column buffer.
        //
        // FIXED-WIDTH BUILDERS (UInt64Builder, DoubleBuilder, etc.):
        //   Appending 1M uint64 values = 1M × 8 bytes written sequentially.
        //   CPU's hardware prefetcher handles this perfectly — max throughput.
        //
        // DICTIONARY BUILDER (StringDictionaryBuilder):
        //   FIX for Arrow 23: the correct class name is StringDictionaryBuilder
        //   (not StringDictionary8Builder which was a shorthand in older Arrow).
        //
        //   Internally it maintains:
        //     - A hash map: string → int32 index (for de-duplication)
        //     - A values builder: stores each unique string ONCE
        //     - An indices builder: stores int32 index per row
        //
        //   For 1M rows with 10 unique symbols:
        //     - Hash map: 10 entries (tiny, always in L1 cache)
        //     - Values:   10 strings × avg 7 bytes = 70 bytes
        //     - Indices:  1M × 4 bytes = 4 MB
        //   vs plain StringBuilder: 1M × avg 7 bytes = 7 MB
        //   Dict builder is faster AND smaller. Win-win.
        //
        // arrow::default_memory_pool(): use the process default allocator.
        // Arrow supports NUMA-aware and GPU memory pools for advanced use cases.
        // ─────────────────────────────────────────────────────────────────────
        arrow::UInt64Builder trade_id_builder(arrow::default_memory_pool());
        arrow::UInt64Builder order_id_builder(arrow::default_memory_pool());
        arrow::Int64Builder timestamp_builder(arrow::default_memory_pool());
        arrow::StringDictionaryBuilder symbol_builder(arrow::default_memory_pool());
        arrow::DoubleBuilder price_builder(arrow::default_memory_pool());
        arrow::UInt32Builder volume_builder(arrow::default_memory_pool());
        arrow::StringDictionaryBuilder side_builder(arrow::default_memory_pool());
        arrow::StringDictionaryBuilder type_builder(arrow::default_memory_pool());
        arrow::BooleanBuilder is_pro_builder(arrow::default_memory_pool());

        // Pre-allocate exact capacity for fixed-width builders.
        // WHY RESERVE?
        // Without reserve: builder doubles its buffer on overflow = ~20 reallocations
        // for 1M rows, each copying all previous data. With reserve(n): ONE allocation.
        // For variable-length (string) builders: Arrow's dictionary builder
        // manages its own growth internally — no Reserve needed.
        THROW_IF_NOT_OK(trade_id_builder.Reserve(n));
        THROW_IF_NOT_OK(order_id_builder.Reserve(n));
        THROW_IF_NOT_OK(timestamp_builder.Reserve(n));
        THROW_IF_NOT_OK(price_builder.Reserve(n));
        THROW_IF_NOT_OK(volume_builder.Reserve(n));
        THROW_IF_NOT_OK(is_pro_builder.Reserve(n));

        // ─────────────────────────────────────────────────────────────────────
        // STEP 3: FILL BUILDERS — THE COLUMNAR TRANSFORMATION
        // ─────────────────────────────────────────────────────────────────────
        // We iterate trades ONCE (row by row) and distribute each field to its
        // respective column builder. After this loop, all 9 builders are full.
        //
        // UnsafeAppend vs Append:
        //   Append       = checks capacity, reallocates if needed, then writes
        //   UnsafeAppend = writes directly (no check) — safe because we Reserved
        //   UnsafeAppend is ~30% faster (eliminates one branch per element)
        //   Use it ONLY after Reserve(n) with exactly n appends.
        //
        // FIX for arrow::util::string_view:
        //   Arrow 23 removed their own arrow::util::string_view alias.
        //   The compiler correctly says "did you mean std::string_view?"
        //   We use std::string_view directly — it's the C++17 standard type
        //   and Arrow 23 accepts it in all string Append() overloads.
        //
        // std::string_view(&t.side, 1):
        //   Creates a view of exactly 1 character starting at t.side.
        //   NO heap allocation. Just a pointer + length = 16 bytes on stack.
        //   This is the correct zero-allocation way to pass a single char
        //   as a string to Arrow — no temporary std::string needed.
        // ─────────────────────────────────────────────────────────────────────
        for (const auto &t : trades)
        {
            // Fixed-width: UnsafeAppend (pre-Reserved, skip bounds check)
            trade_id_builder.UnsafeAppend(t.trade_id);
            order_id_builder.UnsafeAppend(t.order_id);
            timestamp_builder.UnsafeAppend(static_cast<int64_t>(t.timestamp));
            price_builder.UnsafeAppend(t.price);
            volume_builder.UnsafeAppend(t.volume);
            is_pro_builder.UnsafeAppend(t.is_pro);

            // Dictionary string fields: use std::string_view (Arrow 23 fix)
            // string_view(ptr, size) = zero-copy view, no heap allocation
            THROW_IF_NOT_OK(symbol_builder.Append(
                std::string_view(t.symbol.data(), t.symbol.size())));

            // char → 1-char string_view: pointer to the char + length 1
            // No std::string temporary, no heap allocation, no copy
            THROW_IF_NOT_OK(side_builder.Append(std::string_view(&t.side, 1)));
            THROW_IF_NOT_OK(type_builder.Append(std::string_view(&t.type, 1)));
        }

        // ─────────────────────────────────────────────────────────────────────
        // STEP 4: FINALIZE — Builders → immutable Arrow Arrays
        // ─────────────────────────────────────────────────────────────────────
        // Finish() seals the builder and transfers its memory to the Array.
        // The Array is immutable — safe to share across threads without locks.
        // shared_ptr<Array>: Arrow uses shared ownership so one Array can be
        // referenced by multiple RecordBatches and Tables simultaneously.
        // ─────────────────────────────────────────────────────────────────────
        std::shared_ptr<arrow::Array> trade_id_arr, order_id_arr, timestamp_arr;
        std::shared_ptr<arrow::Array> symbol_arr, price_arr, volume_arr;
        std::shared_ptr<arrow::Array> side_arr, type_arr, is_pro_arr;

        THROW_IF_NOT_OK(trade_id_builder.Finish(&trade_id_arr));
        THROW_IF_NOT_OK(order_id_builder.Finish(&order_id_arr));
        THROW_IF_NOT_OK(timestamp_builder.Finish(&timestamp_arr));
        THROW_IF_NOT_OK(symbol_builder.Finish(&symbol_arr));
        THROW_IF_NOT_OK(price_builder.Finish(&price_arr));
        THROW_IF_NOT_OK(volume_builder.Finish(&volume_arr));
        THROW_IF_NOT_OK(side_builder.Finish(&side_arr));
        THROW_IF_NOT_OK(type_builder.Finish(&type_arr));
        THROW_IF_NOT_OK(is_pro_builder.Finish(&is_pro_arr));

        // ─────────────────────────────────────────────────────────────────────
        // ASSEMBLE ARROW TABLE
        // ─────────────────────────────────────────────────────────────────────
        // Table = schema + columnar arrays. This is what Pandas, Spark,
        // DuckDB, Polars all natively understand. ZERO copy here —
        // Table holds shared_ptrs to the same Arrays we just finalized.
        // ─────────────────────────────────────────────────────────────────────
        auto table = arrow::Table::Make(
            schema,
            {trade_id_arr, order_id_arr, timestamp_arr,
             symbol_arr, price_arr, volume_arr,
             side_arr, type_arr, is_pro_arr});

        std::cout << "[PARQUET] Arrow table built. "
                  << table->num_rows() << " rows x "
                  << table->num_columns() << " columns. Writing...\n";

        // ─────────────────────────────────────────────────────────────────────
        // STEP 5: WRITE PARQUET FILE
        // ─────────────────────────────────────────────────────────────────────
        auto outfile_result = arrow::io::FileOutputStream::Open(output_path.string());
        if (!outfile_result.ok())
        {
            throw std::runtime_error(
                "[PARQUET ERROR] Cannot create output file: " +
                output_path.string() + " -> " + outfile_result.status().ToString());
        }
        auto outfile = outfile_result.ValueOrDie();

        // ── Writer Properties ─────────────────────────────────────────────────
        // COMPRESSION: Snappy
        //   Fastest decompress speed of all Parquet codecs (~500 MB/s).
        //   Spark and Athena default to Snappy.
        //   Trade-off: ~2x compression ratio vs GZIP's ~4x.
        //   For hot/warm data (queried frequently): Snappy wins.
        //   For cold/archive data (queried rarely): use ZSTD or GZIP.
        //
        // ROW GROUP SIZE: entire dataset in one group.
        //   Row groups are the unit of parallel reading in Spark.
        //   One group for 1M rows = one Spark task reads the entire file.
        //   For 100M+ row files, use groups of 5-10M rows for parallelism.
        //
        // store_schema(): embed Arrow schema in Parquet metadata footer.
        //   Enables perfect round-trip: pd.read_parquet() gets exact dtypes.
        //   Without this: Parquet readers may infer slightly different types.
        // ─────────────────────────────────────────────────────────────────────
        auto writer_props = parquet::WriterProperties::Builder()
                                .compression(arrow::Compression::SNAPPY)
                                ->max_row_group_length(static_cast<int64_t>(n))
                                ->build();

        auto arrow_props = parquet::ArrowWriterProperties::Builder()
                               .store_schema()
                               ->build();

        // WriteTable does all encoding + compression + file structure.
        // Parquet file structure written:
        //   [magic bytes: PAR1]
        //   [row group 0: column chunks, each compressed with Snappy]
        //   [file footer: schema, row group offsets, column statistics]
        //   [magic bytes: PAR1]
        //
        // The footer column statistics (min/max per column per row group)
        // enable predicate pushdown: "WHERE price > 5000" lets readers
        // skip entire row groups without decompressing them.
        auto write_status = parquet::arrow::WriteTable(
            *table,
            arrow::default_memory_pool(),
            outfile,
            static_cast<int64_t>(n),
            writer_props,
            arrow_props);

        if (!write_status.ok())
            throw std::runtime_error("[PARQUET ERROR] WriteTable: " + write_status.ToString());

        // Close() flushes all buffers and writes the file footer.
        // Without Close(): the footer is never written = corrupt file.
        auto close_status = outfile->Close();
        if (!close_status.ok())
            throw std::runtime_error("[PARQUET ERROR] Close: " + close_status.ToString());

        // ─────────────────────────────────────────────────────────────────────
        // REPORT
        // ─────────────────────────────────────────────────────────────────────
        auto t1 = std::chrono::high_resolution_clock::now();
        long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

        double file_mb = static_cast<double>(std::filesystem::file_size(output_path)) / (1024.0 * 1024.0);
        double csv_est_mb = static_cast<double>(n) * 65.0 / 1'000'000.0;

        std::cout << "[PARQUET] Complete!\n";
        std::cout << "[PARQUET]   Output file    : " << output_path.filename() << "\n";
        std::cout << "[PARQUET]   Rows written   : " << n << "\n";
        std::cout << "[PARQUET]   Parquet size   : "
                  << std::fixed << std::setprecision(1) << file_mb << " MB\n";
        std::cout << "[PARQUET]   vs CSV (~65MB) : "
                  << std::fixed << std::setprecision(1)
                  << (csv_est_mb / file_mb) << "x compression\n";
        std::cout << "[PARQUET]   Duration       : " << ns / 1'000'000 << "ms\n";
        std::cout << "[PARQUET]   Throughput     : "
                  << static_cast<long long>(n * 1.0e9 / ns) << " rows/sec\n";

        return ns;
    }

} // namespace MarketStream