# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Drop 'url' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table constructors

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                     StructField("constructorRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read \
    .option("header", True) \
        .schema(constructors_schema) \
            .csv(f"{raw_folder_path}/constructors.csv")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructor_dropped_df = constructors_df.drop('url')

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
    .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("constructorRef", "constructor_ref") \
            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.constructors")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/constructors"))

# COMMAND ----------


