"""
FMP API Key: vGrqwqW0SyuAyyKLzCOwSBdf0dCTPdOg

Endpoints:

Company Profile: https://financialmodelingprep.com/stable/profile?symbol=AAPL
Financial Ratios: https://financialmodelingprep.com/stable/ratios?symbol=AAPL
Income Statement: https://financialmodelingprep.com/stable/income-statement?symbol=AAPL
Latest Stock Price: https://financialmodelingprep.com/stable/quote?symbol=AAPL
Key Metrics: Endpoint: https://financialmodelingprep.com/stable/key-metrics?symbol=AAPL
"""

import requests
from pathlib import Path
import json
import sqlite3 as sql
import pandas as pd
import time

key_path = Path("/Users/addyhalos/Documents/secrets/keys.json") #where api key is saved locally(not uploaded for security)
companies = ["AAPL","MSFT","TSLA","NVDA","AMZN"]
data_dir = Path("data")
db_path = data_dir / "nyse_financials.db"
sleep = 0.5

endpoints = {
    "company_profile": "https://financialmodelingprep.com/stable/profile",
    "company_ratios": "https://financialmodelingprep.com/stable/ratios",
    "company_income": "https://financialmodelingprep.com/stable/income-statement",
    "company_quote": "https://financialmodelingprep.com/stable/quote",
    "company_key_metrics": "https://financialmodelingprep.com/stable/key-metrics"
}

# retrieve credentials from key.json (locally saved for security)
if not key_path.exists():
    raise FileNotFoundError(f"keys.json not found at {key_path}")
with key_path.open("r", encoding="utf-8") as fh:
    data = json.load(fh)
api_key = data.get("fmp_key")

data_dir.mkdir(exist_ok=True)
session = requests.Session()
conn = sql.connect(db_path)

try:
    for table_name, endpoint_url in endpoints.items():
        all_data = []  # collect all companies' data for this endpoint

    for symbol in companies:
        url = endpoint_url
        resp = session.get(url, params={"apikey": api_key},symbol=symbol, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    
    #for list
    if isinstance(data, list):
                df = pd.DataFrame(data)
    #for dict
    elif isinstance(data, dict):
                df = pd.DataFrame([data])

    df["symbol"] = symbol
    df["fetched_at"] = pd.Timestamp.utcnow()

    all_data.append(df)
    time.sleep(sleep)

     # Concatenate all tickers for this endpoint into one DataFrame
    if all_data:
        endpoint_df = pd.concat(all_data, ignore_index=True)

        # Save/append to SQLite (one table per endpoint)
        endpoint_df.to_sql(table_name, conn, if_exists="append", index=False)

        print(f"✅ Loaded {len(endpoint_df)} rows into table `{table_name}`")

finally:
    conn.commit()
    conn.close()