# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table pistops

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
    .option("header", True) \
        .schema(pit_stops_schema) \
            .csv(f"{raw_folder_path}/pit_stops.csv")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.pitstops")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/pitstops"))

# COMMAND ----------


