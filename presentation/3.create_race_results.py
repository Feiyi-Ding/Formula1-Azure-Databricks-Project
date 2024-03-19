# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_df = spark.read.parquet(f"{db_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")
circuits_df = spark.read.parquet(f"{db_folder_path}/circuits").withColumnRenamed("location", "circuit_location")
drivers_df = spark.read.parquet(f"{db_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.parquet(f"{db_folder_path}/constructors").withColumnRenamed("name", "team")
results_df = spark.read.parquet(f"{db_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

race_results_df = races_df \
    .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
        .join(results_df, results_df.race_id == races_df.race_id ) \
            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
                    .select(races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, constructors_df.team, results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.position)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation_db.race_results")

# COMMAND ----------


