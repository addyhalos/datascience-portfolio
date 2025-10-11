"""
curl -L -o ~/Downloads/customer-churn-dataset.zip
  https://www.kaggle.com/api/v1/datasets/download/mubeenshehzadi/customer-churn-dataset
"""

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import sqlite3 as sql
import zipfile, io
from pathlib import Path
import json

path = Path(r"/Users/addyhalos/Documents/secrets/kaggle.json")
url = "https://www.kaggle.com/api/v1/datasets/download/mubeenshehzadi/customer-churn-dataset"

# retrieve credentials from kaggle.json
if not path.exists():
    raise FileNotFoundError(f"kaggle.json not found at {path}")
with path.open("r", encoding="utf-8") as fh:
    data = json.load(fh)
user = data.get("username")
key = data.get("key")
if not user or not key:
    raise ValueError("kaggle.json missing 'username' or 'key'")

response = requests.get(url, auth=HTTPBasicAuth(user, key))
OUT_CSV = Path("data/churn.csv")

if response.status_code == 200:
    # Unzip into memory
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall("data/")  # Extract to data/ folder
    print("Dataset downloaded and extracted.")
else:
    print("Failed:", response.status_code, response.text)

try:
    response.raise_for_status()
except requests.HTTPError as exc:
    print("HTTP error:", exc)
    print("Response status:", response.status_code)
    print("Response text (truncated):", response.text[:1000])
    raise


# 3) load CSV directly from the ZIP into a pandas DataFrame (in-memory)
zip_bytes = io.BytesIO(response.content)
with zipfile.ZipFile(zip_bytes) as z:
    csv_files = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csv_files:
        raise RuntimeError("No CSV files found inside dataset ZIP")
    chosen = csv_files[0]   # or pick by name if you want
    print("Loading CSV from:", chosen)
    with z.open(chosen) as csvfile:
        df = pd.read_csv(csvfile)

#print(df)

conn = sql.connect('data.db')

df.to_sql('customer_churn', conn,if_exists='replace',index=False)
# Run SQL and get result as DataFrame


sql_query = """
SELECT
churn
,gender
,SeniorCitizen
,Partner
,Dependents
,Contract
,PaperlessBilling
,PaymentMethod
,COUNT(DISTINCT CustomerID)

FROM customer_churn
GROUP BY 1,2,3,4,5,6,7,8
;
"""


results_df = pd.read_sql_query(sql_query, conn)

# Display results
print(results_df)

conn.close