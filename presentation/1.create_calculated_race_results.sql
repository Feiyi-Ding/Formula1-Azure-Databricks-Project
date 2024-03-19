-- Databricks notebook source
USE f1_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In different race years, drivers who achieve the same ranking will receive different points. For the purpose of data analysis, we assign 10 points to the first-place driver, 9 points to the second-place driver, and so on, decreasing in sequence. Meanwhile, only the top ten drivers in each race are considered.

-- COMMAND ----------

CREATE TABLE f1_presentation_db.calculated_race_results
USING parquet
AS
SELECT races.race_year,
        constructors.name AS team_name,    
        drivers.name AS driver_name,
        results.position,
        results.points,
        11 - results.position AS calculated_points
 FROM results
 JOIN drivers ON (results.driver_id = drivers.driver_id)
 JOIN constructors ON (results.constructor_id = constructors.constructor_id)
 JOIN races ON (results.race_id = races.race_id)
 WHERE results.position <= 10;

-- COMMAND ----------

SELECT * 
FROM f1_presentation_db.calculated_race_results
ORDER BY race_year DESC;

-- COMMAND ----------


