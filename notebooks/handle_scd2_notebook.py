# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def employees():
  return spark.readStream.table("my_first_catalog.my_first_schema.tbl_scd2_employees_source")

dlt.create_streaming_table("my_first_catalog.my_first_schema.tbl_scd2_employees_target")

dlt.create_auto_cdc_flow(
  target = "my_first_catalog.my_first_schema.tbl_scd2_employees_target",
  source = "employees",
  keys = ["employee_id"],
  sequence_by = col("sequenceDate"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequenceDate"],
  stored_as_scd_type = "2"
)