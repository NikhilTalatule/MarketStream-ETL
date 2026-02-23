# MarketStream-ETL

**A High-Frequency Trading Data Pipeline in C++20**

Zero-copy CSV parsing → CTRE validation → Technical indicators → PostgreSQL bulk load — end-to-end at **1.1 million trades/second**.

---

## Benchmark Results — 1,000,000 Trades

Measured on Windows 11, GCC 15.2.0, PostgreSQL 16.2 (localhost), Ryzen/Intel consumer hardware.

| Stage              | Duration | Throughput               | ns / trade |
| ------------------ | -------- | ------------------------ | ---------- |
| **Parse**          | 904ms    | **1,105,595 trades/sec** | 904 ns     |
| **Validate**       | 308ms    | **3,245,556 trades/sec** | 308 ns     |
| **Indicators**     | 150ms    | **6,636,039 trades/sec** | 150 ns     |
| **DB Load (COPY)** | 4,242ms  | **235,728 trades/sec**   | 4,242 ns   |
| **Total Pipeline** | ~10 sec  | —                        | —          |

---

## Architecture

```
  CSV File (65MB)
       │
       ▼
┌─────────────────┐
│   CsvParser     │  Zero-copy: one big read → string_view → from_chars
│  (Stage 1)      │  No heap allocations during parse (except symbol string)
└────────┬────────┘
         │ vector<Trade>
         ▼
┌─────────────────┐
│ TradeValidator  │  CTRE compile-time regex — 50x faster than std::regex
│  (Stage 2)      │  6 validation rules: symbol, price, volume, side, type, timestamp
└────────┬────────┘
         │ vector<Trade> (clean only)
         ▼
┌─────────────────────┐
│ TechnicalIndicators │  O(n) hash map grouping by symbol
│    (Stage 3)        │  SMA, RSI, VWAP computed per symbol
└────────┬────────────┘
         │ vector<IndicatorResult>
         ▼
┌─────────────────────────────────────────────┐
│             PipelineExecutor                │
│  std::async — two threads simultaneously:  │
│   Thread A: bulk_load(trades)              │  ─── trades table
│   Thread B: save_indicators(indicators)    │  ─── technical_indicators table
└─────────────────────────────────────────────┘
```

---

## Key Engineering Decisions

### Zero-Copy CSV Parser

The naive approach reads a CSV file line by line with `getline()`. Each call is a system call — a request to the OS. For 1 million rows, that is 1 million system calls.

MarketStream-ETL uses a single `file.read()` to load the entire file into a `std::vector<char>` buffer. A `std::string_view` is then created over that buffer — 16 bytes (pointer + length) regardless of file size. Every field extraction is a pointer arithmetic operation against the same buffer. No copies. No allocations (except one `std::string` per trade for the symbol field).

Number parsing uses `std::from_chars` — C++17's locale-free, allocation-free number parser. It is the fastest standard number parser available.

### Compile-Time Regex Validation (CTRE)

Symbol validation uses CTRE (Compile-Time Regular Expressions). The pattern `[A-Z]{1,10}` is compiled by the C++ compiler into a state machine baked into the executable. At runtime, checking a symbol costs nanoseconds. `std::regex` compiles the same pattern at program startup, costing microseconds per check.

At 1 million events per second, this difference is 1 second of overhead (std::regex) vs. 20ms (CTRE). That gap is the difference between keeping up with a live feed and falling behind.

### Staging Table → Index Rebuild Pattern

The first implementation used the classic staging pattern:

```sql
COPY into staging table (no constraints)
INSERT INTO trades SELECT * FROM staging ON CONFLICT DO NOTHING
```

At 1 million rows, the `ON CONFLICT` clause forces PostgreSQL to perform 1 million B-tree index lookups — one per row — as data is being inserted. This took **44 seconds**.

The production pattern drops the index before COPY and rebuilds it after:

```sql
ALTER TABLE trades DROP CONSTRAINT trades_pkey
COPY 1,000,000 rows directly (no index overhead)  ← full disk speed
ALTER TABLE trades ADD PRIMARY KEY (trade_id)      ← one O(N log N) sort
CREATE INDEX ...                                   ← one pass over sorted data
```

Result: **4.2 seconds**. A 10.6x improvement. This is how `pg_restore`, `pgloader`, and all serious bulk load tools operate.

### Parallel Database I/O (std::async)

Trades and indicators are written to separate tables with no shared state between them. Both operations are pure I/O — independently parallelizable.

Two `std::async(std::launch::async, ...)` tasks run simultaneously. Each task constructs its own `DatabaseLoader`, which opens its own `pqxx::connection`. No shared mutable state. No mutexes. No data races. Exception propagation is automatic via `future.get()` — if either thread throws, the exception re-surfaces in the main thread.

When both tasks are roughly equal in cost, this delivers ~1.87x speedup. Amdahl's Law limits the gain when one task dominates by cost.

---

## Technical Indicators

Computed per symbol from the full 1M trade dataset:

**SMA (Simple Moving Average)** — Average of the last N prices. Smooths noise, reveals trend direction. Used by momentum strategies and trend-following systems.

**RSI (Relative Strength Index)** — 0 to 100 oscillator. RSI > 70 = overbought (likely reversal down). RSI < 30 = oversold (likely reversal up). Formula: `RSI = 100 - (100 / (1 + avg_gain / avg_loss))`.

**VWAP (Volume Weighted Average Price)** — `Σ(price × volume) / Σ(volume)`. Institutional benchmark. Buying below VWAP = good execution. Used by every algo trading desk globally.

Indicators are persisted to a separate `technical_indicators` table with a `computed_at` nanosecond timestamp on each row. Every pipeline run appends new rows — never overwrites. This is the append-only immutable log pattern. It answers: _"What was RELIANCE RSI at 10:30am yesterday?"_

---

## Database Schema

```sql
CREATE TABLE trades (
    trade_id  BIGINT           PRIMARY KEY,
    order_id  BIGINT           NOT NULL,
    timestamp BIGINT           NOT NULL,          -- nanoseconds since epoch
    symbol    VARCHAR(10)      NOT NULL,
    price     DOUBLE PRECISION NOT NULL CHECK (price > 0),
    volume    INTEGER          NOT NULL CHECK (volume > 0),
    side      CHAR(1)          NOT NULL CHECK (side IN ('B','S','N')),
    type      CHAR(1)          NOT NULL CHECK (type IN ('M','L','I')),
    is_pro    BOOLEAN          NOT NULL
);
CREATE INDEX idx_trades_symbol_time ON trades (symbol, timestamp);

CREATE TABLE technical_indicators (
    id          BIGSERIAL        PRIMARY KEY,
    symbol      VARCHAR(10)      NOT NULL,
    computed_at BIGINT           NOT NULL,         -- nanoseconds since epoch
    sma         DOUBLE PRECISION NOT NULL,
    rsi         DOUBLE PRECISION NOT NULL CHECK (rsi >= 0 AND rsi <= 100),
    vwap        DOUBLE PRECISION NOT NULL CHECK (vwap > 0),
    period      INTEGER          NOT NULL CHECK (period > 0)
);
CREATE INDEX idx_indicators_symbol_time ON technical_indicators (symbol, computed_at);
```

---

## Project Structure

```
D:/ETL-Pipeline/
├── src/
│   ├── main.cpp
│   ├── model/
│   │   └── Trade.hpp                  # Trade struct, C++20 spaceship operator, Tradeable concept
│   ├── parser/
│   │   ├── CsvParser.hpp
│   │   └── CsvParser.cpp              # Zero-copy parser: one big read + string_view
│   ├── validator/
│   │   └── TradeValidator.hpp         # CTRE compile-time regex validation
│   ├── indicators/
│   │   └── TechnicalIndicators.hpp    # SMA, RSI, VWAP per symbol
│   ├── database/
│   │   ├── DatabaseLoader.hpp
│   │   └── DatabaseLoader.cpp         # COPY protocol + index-rebuild bulk load
│   ├── threading/
│   │   └── PipelineExecutor.hpp       # std::async parallel DB write
│   ├── benchmark/
│   │   └── Benchmarker.hpp            # RAII scoped timer, ns-precision
│   └── tools/
│       ├── DataGenerator.hpp          # 1M row synthetic trade generator
│       └── generate_data.cpp
├── third_party/
│   └── ctre/                          # Compile-Time Regular Expressions
├── sample_data.csv                    # 13 rows: 10 valid + 3 deliberately malformed
├── CMakeLists.txt
└── README.md
```

---

## Build Instructions

**Prerequisites:** GCC 13+ (C++20), CMake 3.20+, Ninja, PostgreSQL 16, libpqxx 7+, CTRE (header-only, included in `third_party/`).

**MSYS2 UCRT64 (Windows):**

```bash
pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake
pacman -S mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-libpqxx
```

**Build:**

```powershell
mkdir build && cd build
cmake .. -G "Ninja"
ninja
```

Three executables are built: `etl_pipeline.exe`, `generate_data.exe`, `test_db.exe`.

**Generate 1M row test dataset:**

```powershell
.\generate_data.exe              # 1,000,000 rows (default)
.\generate_data.exe 500000       # custom row count
```

**Configure database connection in `src/main.cpp`:**

```cpp
std::string db_conn = "user=postgres password=YOUR_PW host=localhost port=5432 dbname=etl_pipeline_db";
```

**Run:**

```powershell
.\etl_pipeline.exe
```

---

## Useful Queries

```sql
-- Verify trade count
SELECT COUNT(*) FROM trades;

-- Per-symbol trade volume
SELECT symbol, COUNT(*) as trades, SUM(volume) as total_volume
FROM trades
GROUP BY symbol
ORDER BY trades DESC;

-- Latest RSI signals
SELECT symbol, rsi,
       CASE WHEN rsi >= 70 THEN 'OVERBOUGHT'
            WHEN rsi <= 30 THEN 'OVERSOLD'
            ELSE 'NEUTRAL' END AS signal,
       to_timestamp(computed_at / 1e9) AS computed_at
FROM technical_indicators
ORDER BY computed_at DESC
LIMIT 20;

-- RSI history for one symbol (append-only log)
SELECT symbol, rsi, sma, vwap, to_timestamp(computed_at / 1e9) AS run_time
FROM technical_indicators
WHERE symbol = 'RELIANCE'
ORDER BY computed_at;
```

---

## Tech Stack

| Component       | Technology                | Why                                                   |
| --------------- | ------------------------- | ----------------------------------------------------- |
| Language        | C++20                     | Zero-overhead abstractions, deterministic performance |
| Compiler        | GCC 15.2.0 (MSYS2 UCRT64) | Full C++20 support on Windows                         |
| Build           | CMake 4.2 + Ninja         | Faster than Make, portable                            |
| Regex           | CTRE (header-only)        | Compile-time patterns, 50x faster than std::regex     |
| Database driver | libpqxx 7+                | Modern C++ PostgreSQL client                          |
| Database        | PostgreSQL 16             | Industry standard, COPY protocol for bulk load        |
| Concurrency     | std::async / std::future  | Task parallelism with automatic exception propagation |
| Number parsing  | std::from_chars           | No locale, no allocation, fastest in standard library |

---

## Developer

**Nikhil** — Building toward Data Engineering / Business Intelligence roles.  

---
