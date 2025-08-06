# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession

load_date = "2025-08-06"
csv_path = f"/Volumes/ingest/default/raw_fpæl1a/ingest_date={load_date}/data.csv"

df = spark.read.option("header", True).csv(csv_path)

df.printSchema()
df.show(5)







