# Databricks notebook source
import requests, json
import hashlib
import datetime
import pandas as pd 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType, DateType, TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, from_unixtime, unix_timestamp, to_date
from datetime import timedelta, date
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the constants for the silver layer job

# COMMAND ----------

const_timestamp = datetime.datetime.today().replace(second=0, microsecond=0)

currency_api_url = r"https://v6.exchangerate-api.com/v6/479d2c7b7ab257e9cdf338e5/latest/USD"

currency_api_schema = StructType([StructField("currency_name", StringType(), True)\
                     ,StructField("currency_value", FloatType(), True)])

log_file_list = [
    "https://raw.githubusercontent.com/PacktPublishing/Data-Engineering-with-Apache-Spark-Delta-Lake-and-Lakehouse/main/project/prep/ecommerce_logs/electroniz_access_log_1.log",
    "https://raw.githubusercontent.com/PacktPublishing/Data-Engineering-with-Apache-Spark-Delta-Lake-and-Lakehouse/main/project/prep/ecommerce_logs/electroniz_access_log_2.log"
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the silver delta tables using SQL statements

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ctlg_electroniz.sch_silver;
# MAGIC      
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_silver.tbl_silver_customers 
# MAGIC (customer_id Integer,
# MAGIC     customer_name String,
# MAGIC     address String,
# MAGIC     city String,
# MAGIC     postalcode String,
# MAGIC     country String,
# MAGIC     phone String,
# MAGIC     email String,
# MAGIC     credit_card String,
# MAGIC     updated_at Timestamp);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_silver.tbl_silver_orders
# MAGIC (order_number Integer,
# MAGIC     customer_id Integer,
# MAGIC     product_id Integer,
# MAGIC     order_date Date,
# MAGIC     units Integer,
# MAGIC     sale_price Float,
# MAGIC     currency String,
# MAGIC     order_mode String,
# MAGIC     sale_price_usd Float,
# MAGIC     updated_at Timestamp);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_silver.tbl_silver_products
# MAGIC (product_id Integer,
# MAGIC     product_name String,
# MAGIC     product_category String,
# MAGIC     updated_at Timestamp);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_silver.tbl_silver_currency
# MAGIC (currency_value Float,
# MAGIC     currency_name String,
# MAGIC     updated_at Timestamp);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ctlg_electroniz.sch_silver.tbl_silver_iplocation
# MAGIC (ip1 Integer,
# MAGIC     ip2 Integer,
# MAGIC     country_code String,
# MAGIC     country_name String,
# MAGIC     updated_at Timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the  delta tables using Pyspark API

# COMMAND ----------

#Create inventory silver Table
DeltaTable.createIfNotExists(spark) \
    .tableName('ctlg_electroniz.sch_silver.tbl_silver_inventory') \
    .addColumn('inventory_date', 'Timestamp') \
    .addColumn('product', 'String') \
    .addColumn('inventory', 'Integer') \
    .addColumn('updated_at', 'Timestamp') \
	.execute()
    
#Create LOGS silver Table
DeltaTable.createIfNotExists(spark) \
    .tableName('ctlg_electroniz.sch_silver.tbl_silver_logs') \
    .addColumn('time', 'String') \
    .addColumn('remote_ip', 'String') \
    .addColumn('country_name', 'String') \
    .addColumn('ip_number', 'Integer') \
    .addColumn('request', 'String') \
    .addColumn('response', 'String') \
    .addColumn('agent', 'String') \
    .addColumn('updated_at', 'Timestamp') \
	.execute()


#Create ECOMM silver Table
DeltaTable.createIfNotExists(spark) \
    .tableName('ctlg_electroniz.sch_silver.tbl_silver_ecommerce_orders') \
    .addColumn('customer_name', 'String') \
    .addColumn('address', 'String') \
    .addColumn('city', 'String') \
    .addColumn('country', 'String') \
    .addColumn('currency', 'String') \
    .addColumn('email', 'String') \
    .addColumn('order_date', 'Date') \
    .addColumn('order_mode', 'String') \
    .addColumn('order_number', 'Integer') \
    .addColumn('phone', 'String') \
    .addColumn('postalcode', 'String') \
    .addColumn('product_name', 'String') \
    .addColumn('sale_price', 'Float') \
    .addColumn('sale_price_usd', 'Float') \
    .addColumn('updated_at', 'Timestamp') \
	.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the PySpark UDFs
# MAGIC ### UDF’s are used to extend the functions of the framework and re-use them in multiple DataFrames.

# COMMAND ----------

def mask_value(col):
  column = str(col)
  mask_value = hashlib.sha256(column.encode()).hexdigest()
  return mask_value

def curate_email(email):
  curated_value = email.lower()
  return curated_value

def curate_country(country):
  if (country == 'USA' or country == 'United States'):
    curated_value = 'USA'
  elif (country == 'UK' or country == 'United Kingdom'):
    curated_value = 'UK'
  elif (country == 'CAN' or country == 'Canada'):
    curated_value = 'CAN'
  elif (country == 'IND' or country == 'India'):
    curated_value = 'IND'
  else:
    curated_value = country
  return curated_value

def curate_sales_price(currency, currency_value, sales_price):
  if (currency != 'USD'):
    curated_value = float(sales_price)/float(currency_value)
    return float(curated_value)
  else:
    return float(sales_price)

def ip_to_country(ip):
  ipsplit = ip.split(".")
  ip_number=16777216*int(ipsplit[0]) + 65536*int(ipsplit[1]) + 256*int(ipsplit[2]) + int(ipsplit[3])  
  return ip_number

def read_api(url: str):
    normalized_data = dict()
    data = requests.get(currency_api_url).json() 
    normalized_data["_data"] = data 
    return json.dumps(normalized_data)

mask_udf = udf(mask_value, StringType())
curate_email_udf = udf(curate_email, StringType())
curate_country_udf = udf(curate_country, StringType())
curate_sales_price_udf = udf(curate_sales_price, FloatType())
ip_to_country_udf = udf(ip_to_country, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC # Implement Data Quality Rule 1 - Mask PII Data
# MAGIC ![security.jpg](attachment:2d31f71f-58c8-4a76-83bc-e3cbd41b4774.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch customer data from bronze and apply data masking on PII columns

# COMMAND ----------

cust_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_customers")
curated_cust_df = cust_df.withColumn('email_curated',curate_email_udf('email')).drop('email').withColumnRenamed('email_curated', 'email')

curated_cust_df = curated_cust_df.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
 
curated_cust_df = curated_cust_df.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
curated_cust_df = curated_cust_df.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
curated_cust_df = curated_cust_df.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
curated_cust_df = curated_cust_df.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
curated_cust_df = curated_cust_df.withColumn('updated_at', f.lit(const_timestamp))
curated_cust_df.createOrReplaceTempView("store_customers")

# COMMAND ----------

display(curated_cust_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the results to the customer silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_src AS 
# MAGIC SELECT DISTINCT * FROM store_customers;
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_customers c
# MAGIC USING store_customers u
# MAGIC ON c.customer_id = u.customer_id 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET c.customer_id = u.customer_id, 
# MAGIC              c.customer_name = u.customer_name,
# MAGIC              c.address = u.address,
# MAGIC              c.city = u.city,
# MAGIC              c.postalcode = u.postalcode,
# MAGIC              c.country = u.country,
# MAGIC              c.phone = u.phone,
# MAGIC              c.email = u.email,
# MAGIC              c.credit_card = u.credit_card,
# MAGIC              c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Implement Data Quality Rule 2 - Extract currency rates from JSON received from the REST API and convert all sales prices to USD and standardize date format
# MAGIC ![Untitled.png](attachment:82b70015-039b-414f-a7f8-d59653e10d77.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch data from REST API & create a Spark temporary table for the currency rates

# COMMAND ----------

payload = json.loads(read_api(currency_api_url))
result = payload.get('_data')
rates = result.get('conversion_rates')
rates["USD"] = 1.00

ratesList = list(rates.items())
rates_df = spark.createDataFrame(ratesList,schema=currency_api_schema)
rates_df = rates_df.withColumn('updated_at', f.lit(const_timestamp))
rates_df.createOrReplaceTempView("currency_src")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the results from the current API to the currency silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_currency c
# MAGIC USING currency_src u
# MAGIC ON c.currency_name  = u.currency_name 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET c.currency_value = u.currency_value, 
# MAGIC              c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_currency LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the curate_sales_price_udf for conversion of base currency to USD. Also change the date format to a standard format of MM/dd/yyyy.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ctlg_electroniz.sch_silver.tbl_silver_orders

# COMMAND ----------

order_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_orders")
usd_df = spark.sql("""SELECT currency_name AS currency, currency_value 
                        FROM currency_src 
                       WHERE currency_name = 'USD'""")
     
curated_order_df = order_df.join(usd_df, on=['currency'], how="inner")
curated_order_df = curated_order_df.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))
curated_order_df = curated_order_df.withColumn('updated_at', f.lit(const_timestamp))
curated_order_df = curated_order_df.withColumn('order_date_new', f.to_date(curated_order_df.order_date, 'MM/dd/yyyy')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')
curated_order_df = curated_order_df.drop('currency_value')
curated_order_df.createOrReplaceTempView("store_orders")
#display(curated_order_df)
curated_order_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the curated results to the store orders silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW store_orders_src AS 
# MAGIC SELECT DISTINCT * FROM store_orders;
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_orders c
# MAGIC USING store_orders_src u
# MAGIC ON c.order_number  = u.order_number  
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET c.order_number = u.order_number, 
# MAGIC              c.customer_id = u.customer_id,
# MAGIC              c.product_id = u.product_id,
# MAGIC              c.order_date = u.order_date, 
# MAGIC              c.units = u.units,
# MAGIC              c.sale_price = u.sale_price,
# MAGIC              c.sale_price_usd = u.sale_price_usd,
# MAGIC              c.currency = u.currency,
# MAGIC              c.order_mode = u.order_mode,
# MAGIC              c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_orders LIMIT 5;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Curate Products data

# COMMAND ----------

products_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_products")
curated_products_df = products_df.withColumn('updated_at', f.lit(const_timestamp))
curated_products_df = curated_products_df.withColumnRenamed('product_code','product_id')
curated_products_df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the curated results to the products silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW products_src AS
# MAGIC SELECT DISTINCT
# MAGIC   *
# MAGIC FROM
# MAGIC   products;
# MAGIC
# MAGIC MERGE INTO
# MAGIC   ctlg_electroniz.sch_silver.tbl_silver_products c
# MAGIC USING
# MAGIC   products_src u
# MAGIC ON
# MAGIC   c.product_id = u.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   c.product_id = u.product_id,
# MAGIC   c.product_name = u.product_name,
# MAGIC   c.product_category = u.product_category,
# MAGIC   c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN INSERT (product_id, product_name, product_category, updated_at)
# MAGIC   VALUES (u.product_id, u.product_name, u.product_category, u.updated_at);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_products;

# COMMAND ----------

# MAGIC %md
# MAGIC # Implement Data Quality Rule 3 - Deal with non-uniform data related to country names in varying formats
# MAGIC ![New Bitmap image.jpg](attachment:c1b54db3-12d5-494f-97ac-90e7e9d5df2c.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gather e-commerce sales data that is available in the Kusto Database.

# COMMAND ----------

ecommerce_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_ecommerce")

display(ecommerce_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the curation UDF's for masking PII data, conversion of base currency to USD, date format to a standard format of MM/dd/yyyy.

# COMMAND ----------

curated_ecom_df = ecommerce_df.withColumn('updated_at', f.lit(const_timestamp))
curated_ecom_df = curated_ecom_df.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
curated_ecom_df = curated_ecom_df.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
curated_ecom_df = curated_ecom_df.withColumn('order_date', from_unixtime(unix_timestamp('order_date', 'dd/MM/yyy')))
curated_ecom_df = curated_ecom_df.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
  
curated_ecom_df = curated_ecom_df.join(usd_df, on=['currency'], how="inner")
curated_ecom_df = curated_ecom_df.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))

curated_ecom_df = curated_ecom_df.withColumn('order_date_new', to_date(curated_ecom_df.order_date, 'yyyy-MM-dd HH:mm:ss')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')

curated_ecom_df.createOrReplaceTempView("ecommerce_orders")

# COMMAND ----------

display(curated_ecom_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the curated results to the ecommerce orders silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW ecommerce_orders_src AS 
# MAGIC SELECT DISTINCT * FROM ecommerce_orders;
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_ecommerce_orders c
# MAGIC USING ecommerce_orders_src u
# MAGIC ON c.email  = u.email
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET c.customer_name = u.customer_name, 
# MAGIC              c.address = u.address,
# MAGIC              c.city = u.city,
# MAGIC              c.country = u.country,
# MAGIC              c.currency = u.currency,
# MAGIC              c.email = u.email,
# MAGIC              c.order_date = u.order_date,
# MAGIC              c.order_mode = u.order_mode,
# MAGIC              c.order_number = u.order_number,
# MAGIC              c.phone = u.phone,
# MAGIC              c.postalcode = u.postalcode,
# MAGIC              c.product_name = u.product_name,
# MAGIC              c.sale_price = u.sale_price,
# MAGIC              c.sale_price_usd = u.sale_price_usd,
# MAGIC              c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_ecommerce_orders LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Implement Data Quality Rule 3 - Process website tracking locations to map IP addresses of visitors to the e-commerce to country of origin.
# MAGIC ![Untitled.png](attachment:3d853338-d60a-4aa7-9612-1eb82ecaa314.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the bronze geolocation data 

# COMMAND ----------

iplocation_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_iplocation")
curated_iplocation_df = iplocation_df.withColumn('updated_at', f.lit(const_timestamp))
curated_iplocation_df.createOrReplaceTempView("iplocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the curated results to the geolocation silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW iplocation_src AS 
# MAGIC SELECT DISTINCT * FROM iplocation;
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_iplocation c
# MAGIC USING iplocation_src u
# MAGIC ON c.ip1  = u.ip1
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET c.ip1 = u.ip1, 
# MAGIC              c.ip2 = u.ip2,
# MAGIC              c.country_code = u.country_code,
# MAGIC              c.country_name = u.country_name,
# MAGIC              c.updated_at = u.updated_at
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_iplocation LIMIT 5;
# MAGIC      

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the Pandas library to read website logs

# COMMAND ----------

pd_dfs = [] 

for file in log_file_list:
    data = pd.read_json(file, lines=True) 
    pd_dfs.append(data) 

pd_df = pd.concat(pd_dfs, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert the pandas dataframe to a spark dataframe. Apply ip_to_country_udf UDF to map IP address to country.

# COMMAND ----------

logs_df = spark.createDataFrame(pd_df)
curated_logs_df = logs_df.withColumn('updated_at', f.lit(const_timestamp))
curated_logs_df = curated_logs_df.withColumn('time', from_unixtime(unix_timestamp('time', 'dd/MM/yyy:HH:m:ss')))
curated_logs_df = curated_logs_df.withColumn('ip_number',ip_to_country_udf('remote_ip'))
curated_logs_df.createOrReplaceTempView('logs')

# COMMAND ----------

display(curated_logs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now merge the curated results to the website logs silver table

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW logs_src AS 
# MAGIC SELECT DISTINCT * FROM
# MAGIC (SELECT logs.time
# MAGIC 	,logs.remote_ip
# MAGIC 	,iplocation_src.country_name
# MAGIC 	,logs.ip_number
# MAGIC 	,logs.request
# MAGIC 	,logs.response
# MAGIC 	,logs.agent
# MAGIC 	,logs.updated_at
# MAGIC FROM logs
# MAGIC JOIN iplocation_src
# MAGIC WHERE ip1 <= ip_number
# MAGIC 	AND ip2 >= ip_number);
# MAGIC
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_logs c
# MAGIC USING logs_src u
# MAGIC ON c.remote_ip   = u.remote_ip 
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC
# MAGIC SELECT * FROM ctlg_electroniz.sch_silver.tbl_silver_logs LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Implement Data Quality Rule 4 - Process the inventory table

# COMMAND ----------

inventory_df = spark.sql("SELECT * FROM ctlg_electroniz.sch_bronze.tbl_bronze_inventory")
inventory_df.createOrReplaceTempView("inventory")

# COMMAND ----------

# MAGIC %%sql
# MAGIC MERGE INTO ctlg_electroniz.sch_silver.tbl_silver_inventory si
# MAGIC USING inventory i
# MAGIC ON si.product   = i.product 
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ctlg_electroniz.sch_silver.tbl_silver_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), product_id from ctlg_electroniz.sch_silver.tbl_silver_orders group by product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) count, customer_id from ctlg_electroniz.sch_silver.tbl_silver_orders group by customer_id order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.product_name, 
# MAGIC     COUNT(o.product_id) AS request_count
# MAGIC FROM 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_orders o
# MAGIC JOIN 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_products p
# MAGIC ON 
# MAGIC     o.product_id = p.product_id
# MAGIC GROUP BY 
# MAGIC     p.product_name
# MAGIC ORDER BY 
# MAGIC     request_count DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.product_name, 
# MAGIC     COUNT(o.product_id) AS request_count
# MAGIC FROM 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_orders o
# MAGIC JOIN 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_products p
# MAGIC ON 
# MAGIC     o.product_id = p.product_id
# MAGIC WHERE 
# MAGIC     p.product_name = 'DVD'
# MAGIC GROUP BY 
# MAGIC     p.product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.country, 
# MAGIC     COUNT(*) AS tv_order_count
# MAGIC FROM 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_orders o
# MAGIC JOIN 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_products p
# MAGIC ON 
# MAGIC     o.product_id = p.product_id
# MAGIC JOIN 
# MAGIC     ctlg_electroniz.sch_silver.tbl_silver_customers c
# MAGIC ON 
# MAGIC     o.customer_id = c.customer_id
# MAGIC WHERE 
# MAGIC     p.product_name = 'TV'
# MAGIC GROUP BY 
# MAGIC     c.country
# MAGIC ORDER BY 
# MAGIC     tv_order_count DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     browser, 
# MAGIC     COUNT(*) AS usage_count
# MAGIC FROM 
# MAGIC     ctlg_electroniz.sch_silver.tbl_user_sessions
# MAGIC GROUP BY 
# MAGIC     browser
# MAGIC ORDER BY 
# MAGIC     usage_count DESC;