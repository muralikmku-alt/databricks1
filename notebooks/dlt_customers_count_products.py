# Databricks notebook source
import dlt

@dlt.table
def dlt_customers_count_products():
  facts = spark.readStream.table("my_first_catalog.my_first_schema.dlt_transactions")
  dims = spark.read.table("my_first_catalog.my_first_schema.dlt_customers")

  return (
    facts.join(dims, on="customer_id", how="inner").groupBy("customer_id").count()
  )