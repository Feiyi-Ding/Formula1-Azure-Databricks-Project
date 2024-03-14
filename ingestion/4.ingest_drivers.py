# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Create 'name' column
# MAGIC - Rename columns
# MAGIC - Drop 'url' column
# MAGIC - Add 'ingestion_date' column
# MAGIC - Create table drivers

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
    .option("header", True) \
        .schema(drivers_schema) \
            .csv(f"{raw_folder_path}/drivers.csv")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
        .withColumn("name", concat(col("forename"), lit(" "), col("surname"))) \
            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url", "forename", "surname")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_db.drivers")

# COMMAND ----------

display(spark.read.parquet(f"{db_folder_path}/drivers"))

# COMMAND ----------


