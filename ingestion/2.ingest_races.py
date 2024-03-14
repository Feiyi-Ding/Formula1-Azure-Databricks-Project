# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Add 'race_timestamp' and 'ingestion_date' column
# MAGIC - Drop 'date', 'time', 'url' column
# MAGIC - Rename columns
# MAGIC - Create table races partition by year

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
        .schema(races_schema) \
            .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_added_df = races_df \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

races_dropped_df = races_added_df.drop('date', 'time', 'url')

# COMMAND ----------

races_final_df = races_dropped_df \
    .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("year", "race_year") \
            .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_db.races")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/races"))

# COMMAND ----------


