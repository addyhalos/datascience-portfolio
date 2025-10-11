import sqlite3
import pandas as pd
from pathlib import Path

db = Path("/Users/addyhalos/Documents/GitHub/datascience-portfolio/data_engineering/data/nyse_financials.db")
conn = sqlite3.connect(db)

tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", conn)
print("Tables found:", tables['name'].tolist())

for t in tables['name']:
    cnt = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM `{t}`", conn).iloc[0]['cnt']
    print(f" - {t}: {cnt} rows")
    sample = pd.read_sql(f"SELECT * FROM `{t}` LIMIT 3", conn)
    print(sample.head(3))
    print("---")

conn.close()