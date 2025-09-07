# Databricks notebook source
wc_df = spark.sql("SELECT * FROM my_first_catalog.my_first_schema.all_world_cup_players")
wc_df.createOrReplaceTempView("wc")

# COMMAND ----------

# Read the JSON file into a DataFrame
df = spark.read.format("json").option("multiline","true").load("/Volumes/my_first_catalog/my_first_schema/my_staging_volume/all-world-cup-players - nulls.json")

df.createOrReplaceTempView("wc_cdc")
df.show(5)

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW wc_src AS 
# MAGIC SELECT DISTINCT * FROM wc_cdc;
# MAGIC
# MAGIC MERGE INTO my_first_catalog.my_first_schema.all_world_cup_players c
# MAGIC USING wc_src u
# MAGIC ON c.Year  = u.Year
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT count(*) FROM my_first_catalog.my_first_schema.all_world_cup_players
# MAGIC