# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/project2024formula1dl/raw-data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Add race_timestamp and ingestion column

# COMMAND ----------

races_new_df = races_df \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("ingestion_date", current_timestamp())
        
display(races_new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns

# COMMAND ----------

races_selected_df = races_new_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("race_timestamp"), col("ingestion_date"))
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Rename the columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
        .withColumnRenamed("circuitId", "circuit_id")

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet partition by year

# COMMAND ----------

races_renamed_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/project2024formula1dl/processed-data/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/project2024formula1dl/processed-data/races

# COMMAND ----------

df = spark.read.parquet("/mnt/project2024formula1dl/processed-data/races")
display(df)

# COMMAND ----------


