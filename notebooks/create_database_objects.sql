-- Databricks notebook source
-- DBTITLE 1,Create a new catalog in your metastore
CREATE CATALOG IF NOT EXISTS my_first_catalog;

-- COMMAND ----------

-- DBTITLE 1,Check for the newly created catalog
SHOW CATALOGS;

-- COMMAND ----------

-- DBTITLE 1,Add a new schema in the newly created catalog.
CREATE SCHEMA IF NOT EXISTS my_first_catalog.my_first_schema;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- DBTITLE 1,Create a new table manually using Instructions in slides


-- COMMAND ----------

USE CATALOG my_first_catalog;
USE SCHEMA my_first_schema;
SHOW TABLES;

-- COMMAND ----------

-- DBTITLE 1,Exploratory Analysis
SELECT * FROM csv.`/Volumes/my_first_catalog/my_first_schema/my_staging_data/fifa_worldcup.csv`

-- COMMAND ----------

-- DBTITLE 1,Captains that played in large soccer matches averaging at least 30000 audience
SELECT
  p.FullName,
  p.Club,
  p.Position
FROM
  my_first_catalog.my_first_schema.all_world_cup_players AS p
    JOIN my_first_catalog.my_first_schema.fifa_worldcup AS w
      ON p.Year = w.Year
WHERE
  p.IsCaptain = TRUE
  AND w.` Average_attendance ` > 30000

-- COMMAND ----------

-- DBTITLE 1,Create a view
CREATE VIEW IF NOT EXISTS my_first_catalog.my_first_schema.vw_captains_attendance AS
SELECT
  p.FullName,
  p.Club,
  p.Position
FROM
  my_first_catalog.my_first_schema.all_world_cup_players AS p
    JOIN my_first_catalog.my_first_schema.fifa_worldcup AS w
      ON p.Year = w.Year
WHERE
  p.IsCaptain = TRUE
  AND w.` Average_attendance ` > 30000

-- COMMAND ----------

USE CATALOG my_first_catalog;
USE SCHEMA my_first_schema;
SHOW VIEWS;

-- COMMAND ----------

-- DBTITLE 1,Query the view
SELECT * FROM my_first_catalog.my_first_schema.vw_captains_attendance;

-- COMMAND ----------

-- DBTITLE 1,Create a Dynamic view
CREATE VIEW IF NOT EXISTS my_first_catalog.my_first_schema.vw_secure_all_world_cup_players AS
SELECT
  CASE
    WHEN is_member('highclassification') THEN DateOfBirth
    ELSE '****-**-**'
  END as RedactedDateOfBirth,
  *
FROM
  my_first_catalog.my_first_schema.all_world_cup_players;

-- COMMAND ----------

-- DBTITLE 1,Query the dynamic view
SELECT * FROM  my_first_catalog.my_first_schema.vw_secure_all_world_cup_players;

-- COMMAND ----------

-- DBTITLE 1,Create a column masking function
CREATE OR REPLACE FUNCTION my_first_catalog.my_first_schema.format_club(Club STRING)
  RETURNS STRING
  RETURN regexp_replace(Club, '[^a-zA-Z0-9]', ' ')

-- COMMAND ----------

SELECT
  my_first_catalog.my_first_schema.format_club(Club)
FROM
  my_first_catalog.my_first_schema.all_world_cup_players;