# Databricks notebook source
import requests
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------
url = "https://api.statbank.dk/v1/data/FOLK1A/CSV"

payload = {
    "table": "FOLK1A",
    "format": "CSV",
    "variables": [
        {"code": "OMRÅDE", "values": ["101", "147", "265"]},
        {"code": "TID", "values": ["2023K1", "2023K2", "2023K3", "2023K4"]}
    ]
}

response = requests.post(url, json=payload)
response.raise_for_status()
csv_data = response.text

# COMMAND ----------

# COMMAND ----------

load_date = datetime.now().strftime("%Y-%m-%d")
filename = f"/Volumes/sandbox/ingest/folk1a_population/ingest_date={load_date}/data.csv"

with open("/tmp/folk1a.csv", "w", encoding="utf-8") as f:
    f.write(csv_data)

dbutils.fs.mv("file:/tmp/folk1a.csv", f"{filename}", True)

print(f" raw json saved to {filename}")
