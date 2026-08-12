# Automated Stock Data Pipeline: Polygon API to Snowflake

[![Daily Stock Tickers Ingestion](https://github.com/ejmogly/stock-trading-python-app/actions/workflows/daily_stock_job.yml/badge.svg)](https://github.com/ejmogly/stock-trading-python-app/actions/workflows/daily_stock_job.yml)
![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-00A1E9.svg)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automation-2088FF.svg)

A production-grade, automated data engineering pipeline built to extract daily stock reference market data from **Polygon.io** (Massive API), transform it with dynamic schema alignment and date partitioning (`ds`), and batch load it into **Snowflake** (`EJAY_K.PUBLIC.STOCK_TICKERS`). 

The pipeline is fully automated using **GitHub Actions**, running daily in the cloud without requiring local server uptime or manual database administration.

---

## 🏗️ Architecture & Data Flow

```
+---------------------------------+
|  Polygon.io (Massive API)       |
|  /v3/reference/tickers          |
+---------------------------------+
                |
                v  (5 req/min rate-limited pagination)
+---------------------------------+
|  Python ETL Pipeline            |
|  [script.py]                    |
|  - Rate-limit Backoff (12s)     |
|  - Datestamp Tagging (ds)       |
|  - Dynamic Schema Mapping       |
|  - Idempotent Partition Overwrite|
+---------------------------------+
                |
                v  (Snowflake Connector executemany)
+---------------------------------+
|  Snowflake Data Warehouse       |
|  EJAY_K.PUBLIC.STOCK_TICKERS    |
+---------------------------------+
                ^
                |  (Daily Cloud Cron @ 01:00 UTC)
+---------------------------------+
|  GitHub Actions Cloud Runner    |
|  [.github/workflows/daily...]   |
+---------------------------------+
```

---

## 🛠️ Key Technical Features & Solved Challenges

### 1. API Rate Limiting (5 Requests / Minute)
* **Problem**: Polygon.io Free/Basic tier restricts requests to 5 calls per minute. Running rapid pagination requests causes HTTP 429 errors (`You've exceeded the maximum requests per minute...`), prematurely terminating ingestion at 5,000 tickers.
* **Solution**: `fetch_json_with_retry()` automatically catches HTTP 429 responses and `"error"` JSON objects, pausing execution for 12 seconds (`time.sleep(12)`) before retrying. This allows the script to gracefully fetch all ~13,000+ tickers across 14 pages.

### 2. Idempotency & Duplicate Prevention
* **Problem**: Re-running the pipeline multiple times on the same date resulted in duplicate rows.
* **Solution**: Added partition cleanup prior to insertion:
  ```sql
  DELETE FROM "STOCK_TICKERS" WHERE "DS" = '2026-08-12'
  ```
  Guarantees that re-running the job on the same day always produces **exactly 1 clean snapshot of 13,084 rows**.

### 3. Dynamic Schema & Typed DDL Creation
* **Problem**: Manual Snowflake DDL requires maintenance, while untyped staging tables treat dates and booleans as strings.
* **Solution**: Implemented typed DDL auto-generation in `script.py`:
  - `ACTIVE` $\rightarrow$ `BOOLEAN`
  - `LAST_UPDATED_UTC` $\rightarrow$ `TIMESTAMP_NTZ`
  - `DS` $\rightarrow$ `DATE`
  - All other fields $\rightarrow$ `VARCHAR`
  - Runs `DESCRIBE TABLE` and executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` if new API fields appear.

### 4. 24/7 Cloud Automation (GitHub Actions)
* **Problem**: Running scripts locally pauses when the computer is closed or asleep.
* **Solution**: Created `.github/workflows/daily_stock_job.yml` to trigger the Python job daily at 01:00 UTC on GitHub's cloud runners.

---

## 📂 Project Repository Structure

```
stock-trading-python-app/
├── .github/
│   └── workflows/
│       └── daily_stock_job.yml   # GitHub Actions workflow for daily automation
├── .env                          # Local environment secrets (ignored by Git)
├── .gitignore                    # Prevents secrets & cache files from being committed
├── README.md                     # Comprehensive repository documentation
├── requirements.txt              # Required Python packages
├── scheduler.py                  # Local Python schedule daemon
└── script.py                     # Main ETL ingestion script
```

---

## 💻 Core Code Implementations

### 1. Ingestion & Snowflake Load (`script.py`)

```python
from datetime import datetime
import os 
import time
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "EJAY_K")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "STOCK_TICKERS")

LIMIT = 1000

# Explicit column data types mapping for Snowflake DDL
COLUMN_TYPE_MAP = {
    'ACTIVE': 'BOOLEAN',
    'LAST_UPDATED_UTC': 'TIMESTAMP_NTZ',
    'DS': 'DATE'
}

def fetch_json_with_retry(url):
    """Fetch URL and handle Polygon API rate limits (5 req/min on Free Tier)."""
    full_url = url if "apiKey" in url else f"{url}&apiKey={POLYGON_API_KEY}"
    while True:
        response = requests.get(full_url)
        data = response.json()
        if response.status_code == 429 or "error" in data:
            print("Polygon API rate limit reached (5 req/min). Waiting 12 seconds...")
            time.sleep(12)
            continue
        return data

def run_stock_job():
    ds_date = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    
    tickers = []
    data = fetch_json_with_retry(url)
    if 'results' in data:
        for ticker in data['results']:
            ticker['ds'] = ds_date
            tickers.append(ticker)

    while 'next_url' in data and data['next_url']:
        print('requesting next page:', data['next_url'])
        data = fetch_json_with_retry(data['next_url'])
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = ds_date
                tickers.append(ticker)

    print(f"Total tickers fetched: {len(tickers)}")

    if tickers:
        # Dynamically extract all unique field names from tickers
        fieldnames = []
        for ticker in tickers:
            for key in ticker.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = conn.cursor()

        # Check if destination table exists and alter/create as needed with typed columns
        try:
            cur.execute(f'DESCRIBE TABLE "{SNOWFLAKE_TABLE}"')
            existing_cols = [r[0].upper() for r in cur.fetchall()]
            for col in fieldnames:
                if col.upper() not in existing_cols:
                    col_type = COLUMN_TYPE_MAP.get(col.upper(), 'VARCHAR')
                    cur.execute(f'ALTER TABLE "{SNOWFLAKE_TABLE}" ADD COLUMN IF NOT EXISTS "{col.upper()}" {col_type}')
        except Exception:
            cols_sql_parts = []
            for col in fieldnames:
                col_type = COLUMN_TYPE_MAP.get(col.upper(), 'VARCHAR')
                cols_sql_parts.append(f'"{col.upper()}" {col_type}')
            cols_sql = ", ".join(cols_sql_parts)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_TABLE}" ({cols_sql})')

        # Read exact column layout of destination table
        cur.execute(f'DESCRIBE TABLE "{SNOWFLAKE_TABLE}"')
        target_cols = [r[0].upper() for r in cur.fetchall()]

        # Delete existing data for today's partition date before inserting (Idempotent Load)
        print(f"Clearing any existing data for DS = '{ds_date}'...")
        cur.execute(f'DELETE FROM "{SNOWFLAKE_TABLE}" WHERE "DS" = \'{ds_date}\'')

        # Prepare batch insertion statement
        cols_list = ", ".join([f'"{col}"' for col in target_cols])
        placeholders = ", ".join(["%s"] * len(target_cols))
        insert_query = f'INSERT INTO "{SNOWFLAKE_TABLE}" ({cols_list}) VALUES ({placeholders})'

        rows_to_insert = [
            tuple(ticker.get(col.lower()) if ticker.get(col.lower()) is not None else None for col in target_cols)
            for ticker in tickers
        ]

        batch_size = 5000
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            cur.executemany(insert_query, batch)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Successfully dumped {len(tickers)} tickers to Snowflake table '{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}'")

if __name__ == '__main__':
    run_stock_job()
```

---

## 🔒 Configuration & Environment Secrets

### 1. Local `.env` file (Local Testing)
```ini
POLYGON_API_KEY = "your_polygon_key"
SNOWFLAKE_USER = "your_user"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_ACCOUNT = "your_account"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "EJAY_K"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_TABLE = "STOCK_TICKERS"
```

### 2. GitHub Repository Secrets (Cloud Deployment)
Added via **GitHub Repo $\rightarrow$ Settings $\rightarrow$ Secrets and variables $\rightarrow$ Actions**:
* `POLYGON_API_KEY`
* `SNOWFLAKE_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_ACCOUNT`
* `SNOWFLAKE_WAREHOUSE`
* `SNOWFLAKE_DATABASE`
* `SNOWFLAKE_SCHEMA`
* `SNOWFLAKE_TABLE`

---

## 📊 Verification Queries (Snowflake Worksheets)

Run these queries anytime in Snowflake to check status and partition counts:

```sql
-- 1. Check total count loaded by partition date (ds)
SELECT DS, COUNT(*) 
FROM EJAY_K.PUBLIC.STOCK_TICKERS 
GROUP BY DS 
ORDER BY DS DESC;

-- 2. Inspect latest tickers
SELECT TICKER, NAME, MARKET, TYPE, ACTIVE, LAST_UPDATED_UTC, DS
FROM EJAY_K.PUBLIC.STOCK_TICKERS 
WHERE DS = CURRENT_DATE()
ORDER BY TICKER 
LIMIT 20;

-- 3. Verify column data types
DESCRIBE TABLE EJAY_K.PUBLIC.STOCK_TICKERS;
```
