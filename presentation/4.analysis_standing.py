# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("race_year", "")
selected_race_year = dbutils.widgets.get("race_year")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
        .agg(sum("points").alias("total_points"),
             count(when(col("position") == 1, True)).alias("wins")
                   )

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_driver_standings_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

html = f"""<h1 style="color:Black;text-align:center;font-family:Ariel">{selected_race_year} Driver Standings</h1>"""
displayHTML(html)

# COMMAND ----------

display(final_driver_standings_df.filter(f"race_year = {selected_race_year}"))

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
        .agg(sum("points").alias("total_points"),
             count(when(col("position") == 1, True)).alias("wins")
        )

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_constructor_standings_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

html = f"""<h1 style="color:Black;text-align:center;font-family:Ariel">{selected_race_year} Constructor Standings</h1>"""
displayHTML(html)

# COMMAND ----------

display(final_constructor_standings_df.filter(f"race_year = {selected_race_year}"))

# COMMAND ----------


