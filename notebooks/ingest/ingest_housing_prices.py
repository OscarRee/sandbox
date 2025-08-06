# databricks notebook source
# ingest: ingest raw json response from statbank api

# COMMAND ----------
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------
url = "https://api.statbank.dk/v1/data/EJENDOMSSALG1/JSONSTAT"
payload = {
    "table": "EJENDOMSSALG1",
    "format": "JSONSTAT",
    "variables": [
        {"code": "Tid", "values": ["2022K1", "2022K2", "2022K3", "2022K4", "2023K1", "2023K2"]},
        {"code": "OMRÅDE", "values": ["ALL"]},
        {"code": "EJENDOMSKATEGORI", "values": ["Parcelhus"]},
        {"code": "ENHED", "values": ["Kr./m2"]}
    ]
}

# COMMAND ----------
response = requests.get(url, params=payload)
response.raise_for_status()
data = response.json()

# COMMAND ----------
spark = SparkSession.builder.GetOrCreate()
load_date = datetime.now().strftime("%Y-%m-%d")
filename = f"/Volumes/sandbox/ingest/statbank_housing/ingest_date={load_date}/response.json"

with open("/tmp/statbank_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f)

dbutils.fs.mv("file:/tmp/statbank_response.json", f"{filename}", True)

print(f" raw json saved to {filename}")