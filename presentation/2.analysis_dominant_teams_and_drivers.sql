-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Teams of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

WITH cte_dominant_team AS (
        SELECT team_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points,
        RANK() OVER (ORDER BY AVG(calculated_points) DESC) AS team_rank
        FROM f1_presentation_db.calculated_race_results
        GROUP BY team_name
        HAVING total_races > 100
        ORDER BY avg_points DESC
)
SELECT race_year,
        team_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
FROM f1_presentation_db.calculated_race_results
WHERE team_name IN (SELECT team_name FROM cte_dominant_team WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

WITH cte_dominant_driver AS (
        SELECT driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points,
        RANK() OVER (ORDER BY AVG(calculated_points) DESC) AS driver_rank
        FROM f1_presentation_db.calculated_race_results
        GROUP BY driver_name
        HAVING total_races > 50
        ORDER BY avg_points DESC
)
SELECT race_year,
        driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
FROM f1_presentation_db.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM cte_dominant_driver WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


