# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.csv file
# MAGIC - Drop 'statusId' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table results partition by 'race_id'

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
    .option("header", True) \
        .schema(results_schema) \
            .csv(f"{raw_folder_path}/results.csv")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
                .withColumnRenamed("positionText", "position_text") \
                    .withColumnRenamed("positionOrder", "position_order") \
                        .withColumnRenamed("fastestLap", "fastest_lap") \
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_db.results")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/results"))

# COMMAND ----------


