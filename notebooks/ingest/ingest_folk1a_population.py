# Databricks notebook source
import requests
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

url = "https://api.statbank.dk/v1/data/FOLK1A/CSV?OMRÅDE=101,147,265&KØN=0&ALDER=IALT&TID=2023K1,2023K2,2023K3,2023K4"
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

response = requests.post(url, json=payload)
print(response.status_code)
print(response.text[:500])

#response.raise_for_status()
#data = response.json()

# COMMAND ----------

# COMMAND ----------
import requests, pprint

meta = requests.get(
    "https://api.statbank.dk/v1/tableinfo/EJEN77?format=JSON&lang=en"
).json()

for var in meta:
    print(var["id"], "→ sample values:", var["values"][:5])


# COMMAND ----------

spark = SparkSession.builder.GetOrCreate()
load_date = datetime.now().strftime("%Y-%m-%d")
filename = f"/Volumes/sandbox/ingest/statbank_housing/ingest_date={load_date}/response.json"

with open("/tmp/statbank_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f)

dbutils.fs.mv("file:/tmp/statbank_response.json", f"{filename}", True)

print(f" raw json saved to {filename}")
