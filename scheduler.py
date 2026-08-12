import schedule
import time
from datetime import datetime
from script import run_stock_job

def basic_job():
    print("Scheduler heartbeat at:", datetime.now())

# Log heartbeat every hour
schedule.every().hour.do(basic_job)

# Run stock ingestion job daily at 01:00 AM
schedule.every().day.at("01:00").do(run_stock_job)

print("Scheduler started. Waiting for scheduled jobs...")
while True:
    schedule.run_pending()
    time.sleep(1)

