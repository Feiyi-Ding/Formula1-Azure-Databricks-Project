# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table qualifying

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
    .option("header", True) \
        .schema(qualifying_schema) \
            .csv(f"{raw_folder_path}/qualifying.csv")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
                .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/qualifying"))

# COMMAND ----------


