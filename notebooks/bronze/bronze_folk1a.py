# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

load_date = "2025-08-06"
csv_path = f"/Volumes/ingest/default/raw_folk1a/ingest_date={load_date}/data.csv"

df = spark.read.option("header", True).option("delimiter", ";").csv(csv_path)

bronze_df = df.withColumn("bronze_date", lit(load_date)) \
              .withColumn("bronze_timestamp", current_timestamp()) \
              .withColumn("data_source", lit("statbank"))

bronze_table_name = "bronze.statbank.folk1a"

bronze_df.write \
    .mode("overwrite") \
    .saveAsTable(bronze_table_name)

print(f"Bronze table {bronze_table_name} created successfully")


