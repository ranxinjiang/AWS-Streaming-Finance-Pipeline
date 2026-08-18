import json
import boto3
import time
import subprocess
import sys
import os

# Install requests library dynamically in Lambda
subprocess.check_call([
    sys.executable,
    "-m",
    "pip",
    "install",
    "requests",
    "--target",
    "/tmp"
])

sys.path.append("/tmp")

import requests

kinesis = boto3.client("kinesis", region_name="us-east-2")

STREAM_NAME = "ranxin-jiang-project02-kinesis-datastream"

API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")

companies = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA",
    "TSLA", "NFLX", "ADBE", "CRM", "ORCL", "IBM",
    "INTC", "AMD", "QCOM", "CSCO", "AVGO", "TXN",
    "JPM", "BAC", "C", "GS", "MS", "AXP",
    "V", "MA", "PYPL", "WMT", "TGT", "COST",
    "HD", "LOW", "MCD", "SBUX", "NKE", "DIS",
    "KO", "PEP", "PG", "JNJ", "PFE", "MRK",
    "ABBV", "TMO", "UNH", "CVS", "XOM", "CVX",
    "COP", "SLB", "BA", "CAT", "GE", "MMM",
    "F", "GM", "UBER", "ABNB", "SHOP", "XYZ",
    "PLTR", "SNOW", "ZM", "ROKU", "EA", "TTWO"
]

def get_daily_data(company):
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_DAILY"
        f"&symbol={company}"
        f"&apikey={API_KEY}"
    )

    for attempt in range(5):
        response = requests.get(url)
        data = response.json()

        if "Time Series (Daily)" in data:
            return data["Time Series (Daily)"]

        print(f"Attempt {attempt + 1} failed for {company}")
        print(data)
        time.sleep(15)

    return None

def lambda_handler(event, context):
    total_records = 0
    skipped_companies = []

    for company in companies:
        print(f"Collecting data for {company}")

        daily_data = get_daily_data(company)

        if daily_data is None:
            print(f"Skipping {company} after all retry attempts.")
            skipped_companies.append(company)
            continue

        company_record_count = 0

        for date, values in daily_data.items():
            open_stock = float(values["1. open"])
            close_stock = float(values["4. close"])

            difference = round(close_stock - open_stock, 2)

            record = {
                "name": company,
                "ts": date,
                "open_stock": open_stock,
                "close_stock": close_stock,
                "difference": difference
            }

            json_record = json.dumps(record) + "\n"

            print(json_record)

            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json_record,
                PartitionKey=company
            )

            total_records += 1
            company_record_count += 1

            time.sleep(0.05)

        print(f"Finished {company}: {company_record_count} records")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Lambda execution completed",
            "total_records_streamed": total_records,
            "skipped_companies": skipped_companies,
            "expected_records_if_all_successful": 6600
        })
    }
