# Databricks notebook source
import dlt

@dlt.expect_or_drop(
  "valid_orders",
  " order_mode IN ('NEW','RETURN', 'EDIT')"
)
@dlt.table
def dlt_bronze_valid_orders():
  return spark.readStream.table(
    "ctlg_electroniz.sch_bronze.tbl_bronze_orders"
  )