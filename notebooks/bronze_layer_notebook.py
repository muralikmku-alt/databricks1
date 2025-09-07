# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    ArrayType, DateType, TimestampType, FloatType
)

# COMMAND ----------

df_customer_cdc = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/ctlg_electroniz/sch_bronze/ext_vol_cdc/store_transactions/customers/"
    )
)
display(df_customer_cdc)
df_customer_cdc.createOrReplaceTempView("df_customer_cdc")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers
# MAGIC USING
# MAGIC   df_customer_cdc
# MAGIC ON
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.customer_id = df_customer_cdc.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.customer_name = df_customer_cdc.customer_name,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.address = df_customer_cdc.address,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.city = df_customer_cdc.city,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.postalcode = df_customer_cdc.postalcode,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.country = df_customer_cdc.country,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.phone = df_customer_cdc.phone,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.email = df_customer_cdc.email,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.credit_card = df_customer_cdc.credit_card,
# MAGIC   ctlg_electroniz.sch_bronze.tbl_bronze_customers.updated_at = df_customer_cdc.updated_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.customer_id,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.customer_name,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.address,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.city,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.postalcode,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.country,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.phone,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.email,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.credit_card,
# MAGIC     ctlg_electroniz.sch_bronze.tbl_bronze_customers.updated_at
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     df_customer_cdc.customer_id,
# MAGIC     df_customer_cdc.customer_name,
# MAGIC     df_customer_cdc.address,
# MAGIC     df_customer_cdc.city,
# MAGIC     df_customer_cdc.postalcode,
# MAGIC     df_customer_cdc.country,
# MAGIC     df_customer_cdc.phone,
# MAGIC     df_customer_cdc.email,
# MAGIC     df_customer_cdc.credit_card,
# MAGIC     df_customer_cdc.updated_at
# MAGIC   );

# COMMAND ----------

from delta.tables import *
deltaTableOrders = DeltaTable.forName(spark, "ctlg_electroniz.sch_bronze.tbl_bronze_orders")

# COMMAND ----------

df_orders_cdc = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .csv(
        "/Volumes/ctlg_electroniz/sch_bronze/ext_vol_cdc/store_transactions/orders/"
    )
)
display(df_orders_cdc)
df_orders_cdc.createOrReplaceTempView("df_orders_cdc")
df_orders_cdc.printSchema()

# COMMAND ----------

deltaTableOrders.alias('orders') \
  .merge(
    df_orders_cdc.alias('df_orders_cdc'),
    'orders.order_number = df_orders_cdc.order_number'
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

df_ecommerce_cdc=spark.read.option("multiline", "true").json("/Volumes/ctlg_electroniz/sch_bronze/ext_vol_cdc/ecommerce_transactions/2025/07/15/")
display(df_ecommerce_cdc)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_bronze.tbl_bronze_ecommerce (
# MAGIC   customer_name STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   currency STRING,
# MAGIC   email STRING,
# MAGIC   order_date STRING,
# MAGIC   order_mode STRING,
# MAGIC   order_number STRING,
# MAGIC   phone STRING,
# MAGIC   postalcode STRING,
# MAGIC   product_name STRING,
# MAGIC   sale_price STRING
# MAGIC )

# COMMAND ----------

ECOMMERCE_SCHEMA =[
    ('customer_name', StringType()),
    ('address', StringType()),
    ('city', StringType()),
    ('country', StringType()),
    ('currency', StringType()),
    ('email', StringType()),
    ('order_date', StringType()),
    ('order_mode', StringType()),
    ('order_number', StringType()),
    ('phone', StringType()),
    ('postalcode', StringType()),
    ('product_name', StringType()),
    ('sale_price', StringType())
]

ecommerce_fields = [StructField(*field) for field in ECOMMERCE_SCHEMA]
schema_ecomm = StructType(ecommerce_fields)

df_ecommerce_cdc = spark.read.json(path="/Volumes/ctlg_electroniz/sch_bronze/ext_vol_cdc/ecommerce_transactions/2025/07/15/",schema=schema_ecomm,multiLine=True) 
#df_ecommerce_cdc.printSchema()
#display(df_ecommerce_cdc)
df_ecommerce_cdc.writeTo("ctlg_electroniz.sch_bronze.tbl_bronze_ecommerce").createOrReplace()

deltaTableEcommerce = DeltaTable.forName(spark, "ctlg_electroniz.sch_bronze.tbl_bronze_ecommerce")
deltaTableEcommerce.alias('ecommerce') \
  .merge(
    df_ecommerce_cdc.alias('df_ecommerce_cdc'),
    'ecommerce.order_number = df_ecommerce_cdc.order_number'
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

ILOCATION_SCHEMA =[
    ('ip1', IntegerType()),
    ('ip2', IntegerType()),
    ('country_code', StringType()),
    ('country_name', StringType())
]

fields = [StructField(*field) for field in ILOCATION_SCHEMA]
schema_iplocation = StructType(fields)

df_iplocation_cdc = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .schema(schema_iplocation)
    .csv(
        "/Volumes/ctlg_electroniz/sch_bronze/ext_vol_cdc/iplocation/"
    )
)
display(df_iplocation_cdc)
df_iplocation_cdc.createOrReplaceTempView("df_iplocation_cdc")
df_iplocation_cdc.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_bronze.tbl_bronze_iplocation (
# MAGIC   ip1 INTEGER,
# MAGIC   ip2 INTEGER,
# MAGIC   country_code STRING,
# MAGIC   country_name STRING
# MAGIC );

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW iplocation_src AS 
# MAGIC SELECT DISTINCT * FROM df_iplocation_cdc;
# MAGIC
# MAGIC DELETE FROM ctlg_electroniz.sch_bronze.tbl_bronze_iplocation;
# MAGIC INSERT INTO ctlg_electroniz.sch_bronze.tbl_bronze_iplocation SELECT * FROM iplocation_src;
# MAGIC