# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/project2024formula1dl/raw-data

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Ingest circuits.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Drop 'url' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/circuits.csv")

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

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/circuits"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Ingest races.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Add 'race_timestamp' and 'ingestion_date' column
# MAGIC - Drop 'date', 'time', 'url' column
# MAGIC - Rename columns
# MAGIC - Write data to ADLS as parquet file partition by year

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

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/project2024formula1dl/processed-data/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Ingest constructors.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Drop 'url' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/constructors.csv")

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

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/constructors"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Ingest drivers.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Create 'name' column
# MAGIC - Rename columns
# MAGIC - Drop 'url' column
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/drivers.csv")

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

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/drivers"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Ingest results.csv file
# MAGIC - Drop 'statusId' column
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file partition by 'race_id'

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/results.csv")

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

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/project2024formula1dl/processed-data/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Ingest pitstops.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/pit_stops.csv")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/pit_stops"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Ingest lap_times.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/lap_times.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/lap_times"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Ingest qualifyings.csv file
# MAGIC - Read csv file using spark dataframe reader
# MAGIC - Rename columns
# MAGIC - Add 'ingestion_date' column
# MAGIC - Write data to ADLS as parquet file

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
            .csv("dbfs:/mnt/project2024formula1dl/raw-data/qualifying.csv")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
                .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/project2024formula1dl/processed-data/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/project2024formula1dl/processed-data/qualifying"))

# COMMAND ----------


