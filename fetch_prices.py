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
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_PRICES_TABLE", "STOCK_PRICES")

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

def fetch_prices_for_date(ds_date, conn):
    """Fetch grouped daily OHLCV prices for all stocks on ds_date in 1 single API request."""
    print(f"\n==========================================")
    print(f"Fetching Grouped Stock Prices for Date: {ds_date}")
    print(f"==========================================")

    url = f'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{ds_date}?adjusted=true&apiKey={POLYGON_API_KEY}'
    data = fetch_json_with_retry(url)

    results = data.get('results', [])
    if not results:
        print(f"[{ds_date}] No trading price data returned (weekend or market holiday). Skipping.")
        return

    print(f"[{ds_date}] Successfully fetched price data for {len(results)} stocks in 1 API call!")

    cur = conn.cursor()

    # Ensure table STOCK_PRICES exists with typed columns
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_TABLE}" (
        "TICKER" VARCHAR,
        "OPEN" FLOAT,
        "HIGH" FLOAT,
        "LOW" FLOAT,
        "CLOSE" FLOAT,
        "VOLUME" NUMBER,
        "VWAP" FLOAT,
        "TRANSACTIONS" NUMBER,
        "DS" DATE
    )
    '''
    cur.execute(create_table_sql)

    # Delete existing price records for ds_date to guarantee idempotency
    cur.execute(f'DELETE FROM "{SNOWFLAKE_TABLE}" WHERE "DS" = \'{ds_date}\'')

    # Map API keys (T, o, h, l, c, v, vw, n) to Snowflake columns
    insert_query = f'''
    INSERT INTO "{SNOWFLAKE_TABLE}" 
    ("TICKER", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "VWAP", "TRANSACTIONS", "DS") 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    rows_to_insert = [
        (
            item.get('T'),
            item.get('o'),
            item.get('h'),
            item.get('l'),
            item.get('c'),
            item.get('v'),
            item.get('vw'),
            item.get('n'),
            ds_date
        )
        for item in results
    ]

    # Batch insert into Snowflake
    batch_size = 5000
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        cur.executemany(insert_query, batch)

    conn.commit()
    cur.close()
    print(f"[{ds_date}] Successfully dumped {len(rows_to_insert)} stock prices into '{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}'!")

def main():
    parser = argparse.ArgumentParser(description="Fetch grouped daily stock prices (OHLCV) into Snowflake.")
    parser.add_argument("--start", type=str, help="Start date in YYYY-MM-DD format (e.g. 2026-08-01)")
    parser.add_argument("--end", type=str, help="End date in YYYY-MM-DD format (e.g. 2026-08-11)")
    parser.add_argument("--days", type=int, help="Number of past days to fetch (e.g. 7)")

    args = parser.parse_args()

    if args.days:
        end_dt = datetime.now() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=args.days - 1)
    elif args.start and args.end:
        start_str = args.start.strip()
        end_str = args.end.strip()
        try:
            start_dt = datetime.strptime(start_str, '%Y-%m-%d')
            end_dt = datetime.strptime(end_str, '%Y-%m-%d')
        except ValueError as e:
            print(f"Error parsing dates. Please format as YYYY-MM-DD. Details: {e}")
            sys.exit(1)
    else:
        # Default: fetch yesterday's stock prices
        end_dt = datetime.now() - timedelta(days=1)
        start_dt = end_dt

    print(f"Initializing Price Fetch Execution: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

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
        fetch_prices_for_date(ds_date, conn)
        curr_dt += timedelta(days=1)

    conn.close()
    print("\n🎉 Price ingestion task completed successfully!")

if __name__ == '__main__':
    main()
