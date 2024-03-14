# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Drop 'url' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table circuits

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
        .schema(circuits_schema) \
            .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_dropped_df = circuits_df.drop('url')

# COMMAND ----------

circuits_final_df = circuits_dropped_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
        .withColumnRenamed("lat", "latitude") \
            .withColumnRenamed("lng", "longitude") \
                .withColumnRenamed("alt", "altitude") \
                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/circuits"))

# COMMAND ----------


