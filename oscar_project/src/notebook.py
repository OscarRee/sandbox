# Databricks notebook source
# MAGIC %md
# MAGIC # Danish Lakehouse Showcase
# MAGIC 
# MAGIC This notebook demonstrates Databricks lakehouse features using Danish public data sources including weather data from DMI, population statistics from Statistics Denmark, and energy consumption data.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from oscar_project import main
from pyspark.sql.functions import col, avg, max, min, count, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark session
spark = spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Danish Weather Data from DMI (Danish Meteorological Institute)

# COMMAND ----------

print("=== Danish Weather Data Analysis ===")
weather_df = main.get_danish_weather_data(spark)

if not weather_df.isEmpty():
    print("Weather data schema:")
    weather_df.printSchema()
    
    print("\nRecent weather observations:")
    weather_df.orderBy(col("timestamp").desc()).show(10)
    
    # Weather statistics
    weather_stats = weather_df.groupBy("parameter_id").agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),
        count("*").alias("observation_count")
    )
    
    print("\nWeather statistics by parameter:")
    weather_stats.show()
else:
    print("No weather data available - using sample data for demonstration")
    # Create sample weather data for demonstration
    sample_weather = [
        ("2024-01-15 10:00:00", 5.2, "temp_dry", "06181", "celsius"),
        ("2024-01-15 11:00:00", 6.1, "temp_dry", "06181", "celsius"),
        ("2024-01-15 12:00:00", 7.8, "temp_dry", "06181", "celsius"),
        ("2024-01-15 13:00:00", 8.5, "temp_dry", "06181", "celsius"),
        ("2024-01-15 14:00:00", 7.2, "temp_dry", "06181", "celsius")
    ]
    weather_df = spark.createDataFrame(sample_weather, ["timestamp", "temperature", "parameter_id", "station_id", "unit"])
    weather_df = weather_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    weather_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Danish Population Data from Statistics Denmark

# COMMAND ----------

print("=== Danish Population Data Analysis ===")
population_df = main.get_danish_population_data(spark)

if not population_df.isEmpty():
    print("Population data schema:")
    population_df.printSchema()
    
    print("\nPopulation data:")
    population_df.show(10)
    
    # Population trends
    population_trends = population_df.groupBy("area").agg(
        max("population").alias("max_population"),
        min("population").alias("min_population"),
        count("*").alias("data_points")
    )
    
    print("\nPopulation trends by area:")
    population_trends.show()
else:
    print("No population data available - using sample data for demonstration")
    # Create sample population data for demonstration
    sample_population = [
        ("2023Q1", "Copenhagen", 602000, "Statistics Denmark"),
        ("2023Q2", "Copenhagen", 603500, "Statistics Denmark"),
        ("2023Q3", "Copenhagen", 605200, "Statistics Denmark"),
        ("2023Q4", "Copenhagen", 607000, "Statistics Denmark"),
        ("2024Q1", "Copenhagen", 608500, "Statistics Denmark")
    ]
    population_df = spark.createDataFrame(sample_population, ["time_period", "area", "population", "data_source"])
    population_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Danish Energy Data

# COMMAND ----------

print("=== Danish Energy Consumption Analysis ===")
energy_df = main.get_danish_energy_data(spark)

print("Energy data schema:")
energy_df.printSchema()

print("\nEnergy consumption by type:")
energy_df.show()

# Energy analysis by type and date
energy_analysis = energy_df.groupBy("energy_type", "region").agg(
    avg("consumption_mwh").alias("avg_consumption_mwh"),
    max("consumption_mwh").alias("max_consumption_mwh"),
    min("consumption_mwh").alias("min_consumption_mwh")
)

print("\nEnergy consumption analysis:")
energy_analysis.show()

# Renewable vs non-renewable energy
renewable_energy = energy_df.filter(col("energy_type").isin("Wind", "Solar"))
fossil_energy = energy_df.filter(col("energy_type") == "Fossil")

print(f"\nRenewable energy records: {renewable_energy.count()}")
print(f"Fossil energy records: {fossil_energy.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lakehouse Features Demonstration

# COMMAND ----------

# Create a catalog and schema for Danish data
spark.sql("CREATE CATALOG IF NOT EXISTS danish_data")
spark.sql("CREATE SCHEMA IF NOT EXISTS danish_data.danish_lakehouse")

# Write weather data to Delta table
weather_df.write.mode("overwrite").saveAsTable("danish_data.danish_lakehouse.weather_observations")

# Write population data to Delta table
population_df.write.mode("overwrite").saveAsTable("danish_data.danish_lakehouse.population_statistics")

# Write energy data to Delta table
energy_df.write.mode("overwrite").saveAsTable("danish_data.danish_lakehouse.energy_consumption")

print("=== Data written to Delta tables ===")
print("Tables created:")
print("- danish_data.danish_lakehouse.weather_observations")
print("- danish_data.danish_lakehouse.population_statistics")
print("- danish_data.danish_lakehouse.energy_consumption")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Advanced Analytics - Cross-dataset Analysis

# COMMAND ----------

# Read from Delta tables
weather_delta = spark.read.table("danish_data.danish_lakehouse.weather_observations")
population_delta = spark.read.table("danish_data.danish_lakehouse.population_statistics")
energy_delta = spark.read.table("danish_data.danish_lakehouse.energy_consumption")

print("=== Delta Table Statistics ===")
print(f"Weather observations: {weather_delta.count()} records")
print(f"Population records: {population_delta.count()} records")
print(f"Energy consumption: {energy_delta.count()} records")

# Show table schemas
print("\nWeather table schema:")
weather_delta.printSchema()

print("\nPopulation table schema:")
population_delta.printSchema()

print("\nEnergy table schema:")
energy_delta.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality and Validation

# COMMAND ----------

# Check for null values
print("=== Data Quality Check ===")

weather_nulls = weather_df.select([count(col(c).isNull().cast("int")).alias(c) for c in weather_df.columns])
print("Weather data null counts:")
weather_nulls.show()

population_nulls = population_df.select([count(col(c).isNull().cast("int")).alias(c) for c in population_df.columns])
print("Population data null counts:")
population_nulls.show()

energy_nulls = energy_df.select([count(col(c).isNull().cast("int")).alias(c) for c in energy_df.columns])
print("Energy data null counts:")
energy_nulls.show()

# Data validation
print("\n=== Data Validation ===")
print(f"Weather data temperature range: {weather_df.agg(min('temperature'), max('temperature')).collect()}")
print(f"Population data range: {population_df.agg(min('population'), max('population')).collect()}")
print(f"Energy consumption range: {energy_df.agg(min('consumption_mwh'), max('consumption_mwh')).collect()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lakehouse Benefits Summary

# COMMAND ----------

print("=== Danish Lakehouse Showcase Summary ===")
print("This demonstration showcases the following Databricks Lakehouse features:")
print("1. Multi-source data ingestion (APIs, structured data)")
print("2. Delta table storage with ACID transactions")
print("3. Schema enforcement and data quality checks")
print("4. Time-series analytics capabilities")
print("5. Cross-dataset analysis and joins")
print("6. Real-time data processing capabilities")
print("7. Unified analytics and ML platform")

print("\nDanish data sources integrated:")
print("- DMI (Danish Meteorological Institute) weather data")
print("- Statistics Denmark population data")
print("- Danish Energy Agency consumption data")

print("\nNext steps for production:")
print("- Set up automated data pipelines with DLT")
print("- Implement data quality monitoring")
print("- Add streaming capabilities for real-time updates")
print("- Create ML models for predictive analytics")
print("- Set up data governance and access controls") 