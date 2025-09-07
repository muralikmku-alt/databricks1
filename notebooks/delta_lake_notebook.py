# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, array, ArrayType, DateType, TimestampType, FloatType
from pyspark.sql.functions import *

# COMMAND ----------

df_sales_orders = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                         .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/sales_transactions.csv")
display(df_sales_orders)
df_sales_orders.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

df_sales_orders.write.format("delta").partitionBy("currency").saveAsTable("my_first_catalog.my_first_schema.store_orders")

# COMMAND ----------

# MAGIC %%sql		
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2581; 

# COMMAND ----------

# MAGIC %%sql
# MAGIC UPDATE my_first_catalog.my_first_schema.store_orders SET sale_price=90.50 WHERE order_number=2581;

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2581;
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY my_first_catalog.my_first_schema.store_orders;
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders VERSION AS OF 0 WHERE order_number=2581;

# COMMAND ----------

# MAGIC %%sql
# MAGIC DELETE FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2581;
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2581;
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT count(*) FROM my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC RESTORE TABLE my_first_catalog.my_first_schema.store_orders TO VERSION AS OF 1;
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2581; 

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY my_first_catalog.my_first_schema.store_orders;
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT count(*) FROM my_first_catalog.my_first_schema.store_orders;

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2501; 

# COMMAND ----------

# DBTITLE 1,Merge the next hour data in the delta table 
from pyspark.sql.functions import to_date, col

df_sales_orders_CDC1 = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                         .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/sales_transactions - CDC1.csv")
df_sales_orders_CDC1 = df_sales_orders_CDC1.withColumn("order_date", to_date(df_sales_orders_CDC1.order_date,  'MM/dd/yyyy'))
display(df_sales_orders_CDC1)
df_sales_orders_CDC1.printSchema()
df_sales_orders_CDC1.createOrReplaceTempView("cdc_store_orders")


# COMMAND ----------

spark.sql("SELECT * FROM cdc_store_orders").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO my_first_catalog.my_first_schema.store_orders
# MAGIC USING cdc_store_orders
# MAGIC ON cdc_store_orders.order_number = my_first_catalog.my_first_schema.store_orders.order_number
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2501; 
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY my_first_catalog.my_first_schema.store_orders;
# MAGIC

# COMMAND ----------

# DBTITLE 1,A new file came in with a schema change - a new column got added
df_read_data_schema_change = spark.read                           \
                                .option("header", "true")         \
                                .option("inferSchema", "true")    \
                                .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/schema_change.csv")
df_read_data_schema_change = df_read_data_schema_change.withColumn("order_date", to_date(df_read_data_schema_change.order_date,  'MM/dd/yyyy'))
display(df_read_data_schema_change)
df_read_data_schema_change.printSchema()
df_read_data_schema_change.createOrReplaceTempView("schema_change_store_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO my_first_catalog.my_first_schema.store_orders
# MAGIC USING schema_change_store_orders
# MAGIC ON schema_change_store_orders.order_number = my_first_catalog.my_first_schema.store_orders.order_number
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders; 

# COMMAND ----------

df_read_data_schema_change_test = spark.read                           \
                                .option("header", "true")         \
                                .option("inferSchema", "true")    \
                                .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/schema_change_test.csv")
df_read_data_schema_change_test = df_read_data_schema_change_test.withColumn("order_date", to_date(df_read_data_schema_change_test.order_date,  'MM/dd/yyyy'))
display(df_read_data_schema_change_test)
df_read_data_schema_change_test.printSchema()
df_read_data_schema_change_test.createOrReplaceTempView("schema_change_store_orders_test")

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders WHERE order_number=2501; 

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName("Merge With Schema Evolution") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Load target and source DataFrames
target_table1 = DeltaTable.forName(spark, "my_first_catalog.my_first_schema.store_orders")
source_df1 = spark.table("schema_change_store_orders_test")

# Perform the merge with schema evolution
(
    target_table1.alias("target1")
    .merge(
        source=source_df1.alias("source1"),
        condition="source1.order_number = target1.order_number"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM my_first_catalog.my_first_schema.store_orders; 