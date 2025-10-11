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
data_dir = Path("/Users/addyhalos/Documents/GitHub/datascience-portfolio/data_engineering/data")
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
        all_dfs = []
        print(f"\n--- endpoint: {table_name} -> {endpoint_url} ---")

        # ensure we iterate every symbol
        for symbol in companies:
            try:
                resp = session.get(endpoint_url, params={"symbol": symbol, "apikey": api_key}, timeout=20)
                status = resp.status_code
                text_len = len(resp.text or "")
                print(f"{table_name} | {symbol} -> status: {status} | len: {text_len}")

                if status == 200:
                    payload = resp.json()
                    if isinstance(payload, list):
                        df = pd.DataFrame(payload) if payload else pd.DataFrame([{"note":"empty_list"}])
                    elif isinstance(payload, dict):
                        df = pd.DataFrame([payload])
                    else:
                        df = pd.DataFrame([{"raw": str(payload)}])
                else:
                    # Non-200 -> keep error info in a row so table is created and you can inspect
                    df = pd.DataFrame([{"error_status": status, "error_text": resp.text[:500]}])

                # annotate
                df["symbol"] = symbol
                df["endpoint"] = table_name
                df["fetched_at"] = pd.Timestamp.utcnow()
                all_dfs.append(df)

            except Exception as e:
                print(f"EXCEPTION: {table_name} | {symbol} -> {e}")
                all_dfs.append(pd.DataFrame([{"symbol": symbol, "endpoint": table_name, "error": str(e), "fetched_at": pd.Timestamp.utcnow()}]))
            finally:
                time.sleep(sleep)

        # ALWAYS write a table (append). even if only placeholders exist
        if all_dfs:
            endpoint_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        else:
            # should not happen, but safe fallback
            endpoint_df = pd.DataFrame([{"note":"no_data_collected","endpoint":table_name, "fetched_at": pd.Timestamp.utcnow()}])

        endpoint_df.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"✅ WROTE {len(endpoint_df)} rows to `{table_name}`")

finally:
    conn.commit()
    # print summary
    tbls = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", conn)
    print("\nTables:", tbls['name'].tolist())
    for t in tbls['name']:
        cnt = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM `{t}`", conn).iloc[0]['cnt']
        print(f" - {t}: {cnt} rows")
    conn.close()