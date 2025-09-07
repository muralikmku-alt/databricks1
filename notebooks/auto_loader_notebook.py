# Databricks notebook source
file_path = "/Volumes/my_first_catalog/my_first_schema/my_staging_volume/staging_employees/"

stream_ingest = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true") 
    .option("cloudFiles.schemaLocation", "/Volumes/my_first_catalog/my_first_schema/my_staging_volume/staging_employees_checkpoint/")
    .option("mode", "DROPMALFORMED")
    .load(file_path)
    .writeStream
    .option("checkpointLocation", "/Volumes/my_first_catalog/my_first_schema/my_staging_volume/staging_employees_checkpoint/")
    .trigger(availableNow=True)
    .table("my_first_catalog.my_first_schema.tbl_streaming_employees")
    .awaitTermination()
    )
