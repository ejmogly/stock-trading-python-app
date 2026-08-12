# Automated Stock Market Data Platform: Polygon API to Snowflake

[![Daily Stock Tickers & Prices Ingestion](https://github.com/ejmogly/stock-trading-python-app/actions/workflows/daily_stock_job.yml/badge.svg)](https://github.com/ejmogly/stock-trading-python-app/actions/workflows/daily_stock_job.yml)
![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-00A1E9.svg)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automation-2088FF.svg)

A production-grade, automated data engineering platform built to extract daily stock reference metadata and grouped daily OHLCV trading prices from **Polygon.io** (Massive API), transform them with dynamic schema alignment and date partitioning (`ds`), and load them into **Snowflake** data warehouse (`STOCK_TICKERS` & `STOCK_PRICES`).

The pipeline is 100% automated using **GitHub Actions**, running daily in the cloud without requiring local server uptime or manual database administration.

---

## 🏗️ Architecture & Data Flow

```
+----------------------------------------------------------------------+
|                     Polygon.io (Massive API)                         |
|  - Reference Metadata: /v3/reference/tickers                         |
|  - Daily OHLCV Prices: /v2/aggs/grouped/locale/us/market/stocks      |
+----------------------------------------------------------------------+
                                  |
                                  v  (Rate-Limited Pagination & Grouped Endpoints)
+----------------------------------------------------------------------+
|                     Python ETL Data Pipeline                         |
|                                                                      |
|  1. script.py        --> Master Ticker Reference Metadata            |
|  2. fetch_prices.py  --> Daily Grouped Stock Prices (OHLCV)          |
|  3. backfill.py      --> Historical Ticker Metadata Backfilling      |
|                                                                      |
|  Features: Exponential Rate-Limit Backoff (12s), Datestamp Tagging   |
|            (ds), Dynamic Schema Mapping, Idempotent Overwrites       |
+----------------------------------------------------------------------+
                                  |
                                  v  (Snowflake Connector executemany)
+----------------------------------------------------------------------+
|                     Snowflake Data Warehouse                         |
|                                                                      |
|  - Dimension Table: EJAY_K.PUBLIC.STOCK_TICKERS (Metadata)           |
|  - Fact Table:      EJAY_K.PUBLIC.STOCK_PRICES  (OHLCV Prices)       |
+----------------------------------------------------------------------+
                                  ^
                                  |  (Daily Cloud Cron @ 01:00 UTC)
+----------------------------------------------------------------------+
|                   GitHub Actions Cloud Runner                        |
|                   [.github/workflows/daily_stock_job.yml]            |
+----------------------------------------------------------------------+
```

---

## 💾 Dual Table Data Model in Snowflake

| Table Name | Entity Type | Columns / Schema | Description |
| :--- | :--- | :--- | :--- |
| **`STOCK_TICKERS`** | Dimension Table | `TICKER`, `NAME`, `MARKET`, `LOCALE`, `PRIMARY_EXCHANGE`, `TYPE`, `ACTIVE`, `CURRENCY_NAME`, `CIK`, `COMPOSITE_FIGI`, `SHARE_CLASS_FIGI`, `LAST_UPDATED_UTC`, `DS` | Master directory of stock reference metadata (companies, exchanges, CIKs, security types). |
| **`STOCK_PRICES`** | Fact Table | `TICKER`, `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME`, `VWAP`, `TRANSACTIONS`, `DS` | Daily trading market price data (OHLCV) for point-in-time financial analysis & charting. |

---

## 🛠️ Key Technical Features & Solved Challenges

### 1. Ultra-Fast Daily Price Ingestion (1 Call per Day)
* **Design**: Uses Polygon's Grouped Daily Aggregates API (`/v2/aggs/grouped/locale/us/market/stocks/{date}`).
* **Performance**: Fetches all 12,000+ daily stock prices for an entire market day in **1 single API call (~3 seconds)**, making 1 month of price backfills finish in under 2 minutes.

### 2. API Rate Limiting Management (5 Requests / Minute)
* **Problem**: Polygon.io Basic tier limits calls to 5 per minute. Rapid requests trigger HTTP 429 rate limit errors.
* **Solution**: Custom `fetch_json_with_retry()` wrapper catches HTTP 429 errors and `"error"` JSON objects, automatically pausing 12 seconds (`time.sleep(12)`) before retrying.

### 3. Idempotency & Duplicate Prevention
* **Problem**: Re-running pipelines on the same date accumulated duplicate rows.
* **Solution**: Automatic partition cleanup prior to insertion:
  ```sql
  DELETE FROM "STOCK_TICKERS" WHERE "DS" = '2026-08-12';
  DELETE FROM "STOCK_PRICES" WHERE "DS" = '2026-08-12';
  ```
  Guarantees exactly 1 clean snapshot per date regardless of re-run frequency.

### 4. Typed DDL & Dynamic Schema Evolution
* **Solution**: Dynamically inspects existing tables using `DESCRIBE TABLE` and auto-generates typed columns:
  - `ACTIVE` $\rightarrow$ `BOOLEAN`
  - `LAST_UPDATED_UTC` $\rightarrow$ `TIMESTAMP_NTZ`
  - `DS` $\rightarrow$ `DATE`
  - `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VWAP` $\rightarrow$ `FLOAT`
  - `VOLUME`, `TRANSACTIONS` $\rightarrow$ `NUMBER`

---

## 📂 Project Repository Structure

```
stock-trading-python-app/
├── .github/
│   └── workflows/
│       └── daily_stock_job.yml   # Automated daily GitHub Actions workflow
├── .env                          # Local environment variables (ignored by Git)
├── .gitignore                    # Git exclusion rules
├── PIPELINE_DOCUMENTATION.md     # Detailed English technical documentation
├── PIPELINE_DOCUMENTATION_KR.md  # Detailed Korean technical documentation
├── README.md                     # GitHub repository front page
├── requirements.txt              # Python dependency requirements
├── backfill.py                   # Historical ticker reference backfill script
├── fetch_prices.py               # Daily & historical stock price ingestion script
├── scheduler.py                  # Local Python schedule daemon
└── script.py                     # Daily stock ticker reference ingestion script
```

---

## 💻 Usage & Execution Guide

### 1. Automated Daily Ingestion (GitHub Actions)
Runs automatically every day at 01:00 UTC. Can also be manually triggered anytime via **GitHub Repo $\rightarrow$ Actions $\rightarrow$ Daily Stock Tickers Ingestion $\rightarrow$ Run workflow**.

### 2. Manual Daily Run
```bash
# Ingest current stock ticker metadata
python3 script.py

# Ingest yesterday's stock trading prices (OHLCV)
python3 fetch_prices.py
```

### 3. Backfilling Historical Stock Prices
```bash
# Backfill July 2026 stock prices (Full Month in ~2 mins)
python3 fetch_prices.py --start 2026-07-01 --end 2026-07-31

# Backfill last 30 days of stock prices
python3 fetch_prices.py --days 30
```

### 4. Backfilling Historical Ticker Reference Data
```bash
# Backfill ticker metadata for a specific date range
python3 backfill.py --start 2026-08-01 --end 2026-08-11
```

---

## 📊 Verification Queries (Snowflake Worksheets)

```sql
-- 1. Check loaded row counts by date partition
SELECT DS, COUNT(*) FROM EJAY_K.PUBLIC.STOCK_TICKERS GROUP BY DS ORDER BY DS DESC;
SELECT DS, COUNT(*) FROM EJAY_K.PUBLIC.STOCK_PRICES GROUP BY DS ORDER BY DS DESC;

-- 2. Join price data with ticker metadata for analysis/visualization
SELECT p.DS, p.TICKER, t.NAME, t.PRIMARY_EXCHANGE, p.CLOSE, p.VOLUME, p.VWAP
FROM EJAY_K.PUBLIC.STOCK_PRICES p
JOIN EJAY_K.PUBLIC.STOCK_TICKERS t ON p.TICKER = t.TICKER
WHERE p.DS = CURRENT_DATE() - 1
  AND t.TYPE = 'CS'
ORDER BY p.VOLUME DESC
LIMIT 20;
```
