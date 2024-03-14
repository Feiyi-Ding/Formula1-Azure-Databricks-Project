# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table laptimes

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
    .option("header", True) \
        .schema(lap_times_schema) \
            .csv(f"{raw_folder_path}/lap_times.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.laptimes")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/laptimes"))

# COMMAND ----------


