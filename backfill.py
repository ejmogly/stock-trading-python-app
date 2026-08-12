import argparse
from datetime import datetime, timedelta
import os
import sys
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

def backfill_single_date(ds_date, conn):
    """Fetch historical tickers for a specific date and load into Snowflake."""
    print(f"\n==========================================")
    print(f"Processing Backfill for Date: {ds_date}")
    print(f"==========================================")
    
    url = f'https://api.massive.com/v3/reference/tickers?date={ds_date}&market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    tickers = []
    data = fetch_json_with_retry(url)
    if 'results' in data:
        for ticker in data['results']:
            ticker['ds'] = ds_date
            tickers.append(ticker)

    while 'next_url' in data and data['next_url']:
        print(f"[{ds_date}] Requesting next page...")
        data = fetch_json_with_retry(data['next_url'])
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = ds_date
                tickers.append(ticker)

    print(f"[{ds_date}] Total tickers fetched: {len(tickers)}")

    if tickers:
        # Dynamically extract all field names for this date
        fieldnames = []
        for ticker in tickers:
            for key in ticker.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        cur = conn.cursor()

        # Ensure table and any missing columns exist
        try:
            cur.execute(f'DESCRIBE TABLE "{SNOWFLAKE_TABLE}"')
            existing_cols = [r[0].upper() for r in cur.fetchall()]
            for col in fieldnames:
                if col.upper() not in existing_cols:
                    col_type = COLUMN_TYPE_MAP.get(col.upper(), 'VARCHAR')
                    cur.execute(f'ALTER TABLE "{SNOWFLAKE_TABLE}" ADD COLUMN IF NOT EXISTS "{col.upper()}" {col_type}')
        except Exception:
            cols_sql_parts = [f'"{col.upper()}" {COLUMN_TYPE_MAP.get(col.upper(), "VARCHAR")}' for col in fieldnames]
            cols_sql = ", ".join(cols_sql_parts)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_TABLE}" ({cols_sql})')

        cur.execute(f'DESCRIBE TABLE "{SNOWFLAKE_TABLE}"')
        target_cols = [r[0].upper() for r in cur.fetchall()]

        # Clear existing data for ds_date to guarantee idempotency
        cur.execute(f'DELETE FROM "{SNOWFLAKE_TABLE}" WHERE "DS" = \'{ds_date}\'')

        # Prepare batch insertion
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
        print(f"[{ds_date}] Successfully backfilled {len(tickers)} rows into Snowflake!")

def main():
    parser = argparse.ArgumentParser(description="Backfill historical stock tickers into Snowflake.")
    parser.add_argument("--start", type=str, help="Start date in YYYY-MM-DD format (e.g. 2026-08-01)")
    parser.add_argument("--end", type=str, help="End date in YYYY-MM-DD format (e.g. 2026-08-11)")
    parser.add_argument("--days", type=int, help="Number of past days to backfill (e.g. 5)")

    args = parser.parse_args()

    if args.days:
        end_dt = datetime.now() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=args.days - 1)
    elif args.start and args.end:
        start_dt = datetime.strptime(args.start, '%Y-%m-%d')
        end_dt = datetime.strptime(args.end, '%Y-%m-%d')
    else:
        # Default: backfill past 2 days if no arguments passed
        end_dt = datetime.now() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=1)

    print(f"Initializing Backfill Execution: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    curr_dt = start_dt
    while curr_dt <= end_dt:
        ds_date = curr_dt.strftime('%Y-%m-%d')
        backfill_single_date(ds_date, conn)
        curr_dt += timedelta(days=1)

    conn.close()
    print("\n🎉 All historical backfill dates completed successfully!")

if __name__ == '__main__':
    main()
