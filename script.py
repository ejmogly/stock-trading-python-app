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

