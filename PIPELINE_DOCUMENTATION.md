# Stock Data Platform: Polygon API to Snowflake (Automated via GitHub Actions)

## 📋 Executive Summary
This document provides full technical documentation for the production-grade automated stock data platform. The platform extracts company reference metadata and daily trading market data (OHLCV) from **Polygon.io** (Massive API), transforms the payloads with dynamic schema alignment and date partitioning (`ds`), and loads them into **Snowflake** (`EJAY_K.PUBLIC.STOCK_TICKERS` & `EJAY_K.PUBLIC.STOCK_PRICES`).

The pipeline is 100% automated using **GitHub Actions**, executing daily in the cloud without requiring local laptop uptime or manual database administration.

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

## 💾 Dual Table Data Model

### 1. `STOCK_TICKERS` (Dimension Table - Metadata)
Stores reference data for companies, exchanges, SEC CIK numbers, and active trading status.
* `TICKER` (`VARCHAR`): Stock ticker symbol (e.g. `AAPL`, `MSFT`)
* `NAME` (`VARCHAR`): Company full legal name
* `MARKET` (`VARCHAR`): Market category (`stocks`)
* `LOCALE` (`VARCHAR`): Country locale (`us`)
* `PRIMARY_EXCHANGE` (`VARCHAR`): Exchange code (`XNAS`, `XNYS`)
* `TYPE` (`VARCHAR`): Security type (`CS` = Common Stock, `ETF`)
* `ACTIVE` (`BOOLEAN`): Active trading status (`TRUE`/`FALSE`)
* `CURRENCY_NAME` (`VARCHAR`): Currency code (`usd`)
* `CIK` (`VARCHAR`): SEC Central Index Key
* `COMPOSITE_FIGI` (`VARCHAR`): Global FIGI identifier
* `SHARE_CLASS_FIGI` (`VARCHAR`): Share class FIGI identifier
* `LAST_UPDATED_UTC` (`TIMESTAMP_NTZ`): Last metadata update timestamp
* `DS` (`DATE`): Daily partition date (`YYYY-MM-DD`)

### 2. `STOCK_PRICES` (Fact Table - Market OHLCV Prices)
Stores daily trading prices and volume for financial analytics and visualization.
* `TICKER` (`VARCHAR`): Stock ticker symbol
* `OPEN` (`FLOAT`): Opening market price
* `HIGH` (`FLOAT`): Highest price of the day
* `LOW` (`FLOAT`): Lowest price of the day
* `CLOSE` (`FLOAT`): Closing market price
* `VOLUME` (`NUMBER`): Total shares traded
* `VWAP` (`FLOAT`): Volume-weighted average price
* `TRANSACTIONS` (`NUMBER`): Number of transactions
* `DS` (`DATE`): Trading date (`YYYY-MM-DD`)

---

## 🛠️ Key Technical Features & Solved Challenges

### 1. Ultra-Fast Grouped Price Ingestion (1 Call / Day)
* `fetch_prices.py` utilizes Polygon's Grouped Daily Aggregates API (`/v2/aggs/grouped/locale/us/market/stocks/{date}`).
* Ingests all 12,000+ stock prices for an entire market day in **1 single API call (~3 seconds)**.

### 2. Rate Limiting Backoff (5 Requests / Minute)
* `fetch_json_with_retry()` catches HTTP 429 errors and `"error"` responses on Polygon Basic plan, automatically pausing 12 seconds (`time.sleep(12)`) before retrying.

### 3. Idempotent Partition Overwrites
* Before inserting records for a partition date `ds`, both `script.py` and `fetch_prices.py` run:
  ```sql
  DELETE FROM "STOCK_TICKERS" WHERE "DS" = 'YYYY-MM-DD';
  DELETE FROM "STOCK_PRICES" WHERE "DS" = 'YYYY-MM-DD';
  ```
  Prevents row duplication regardless of how many times a job is triggered.

---

## 📂 Repository Code Files

* **`script.py`**: Daily stock reference metadata ingestion script.
* **`fetch_prices.py`**: Daily & historical stock price ingestion script.
* **`backfill.py`**: Historical ticker metadata backfill script.
* **`scheduler.py`**: Local daemon schedule runner.
* **`.github/workflows/daily_stock_job.yml`**: GitHub Actions daily cloud automation workflow.
