import schedule
import time
from datetime import datetime, timedelta
import snowflake.connector
import os
from dotenv import load_dotenv
from script import run_stock_job
from fetch_prices import fetch_prices_for_date

load_dotenv()

def run_daily_prices():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "EJAY_K"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    )
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    fetch_prices_for_date(yesterday, conn)
    conn.close()

def basic_job():
    print("Scheduler heartbeat at:", datetime.now())

# Log heartbeat every hour
schedule.every().hour.do(basic_job)

# Run stock price ingestion daily at 01:00 AM
schedule.every().day.at("01:00").do(run_daily_prices)

# Run ticker metadata ingestion weekly every Monday at 01:00 AM
schedule.every().monday.at("01:00").do(run_stock_job)

print("Scheduler started. Waiting for scheduled jobs (Daily Prices & Weekly Tickers)...")
while True:
    schedule.run_pending()
    time.sleep(1)


