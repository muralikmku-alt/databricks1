# Databricks notebook source
df_books = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                         .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv")
#df_books = df_books.withColumn("order_date", to_date(df_books.order_date,  'MM/dd/yyyy'))
display(df_books)
df_books.printSchema()
#df_books.createOrReplaceTempView("cdc_store_orders")

# COMMAND ----------

df_subscriber_books = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                         .parquet("/Volumes/my_first_catalog/my_first_schema/my_staging_data/subcriber_books.parquet")
#df_subscriber_books = df_subscriber_books.withColumn("order_date", to_date(df_subscriber_books.order_date,  'MM/dd/yyyy'))
display(df_subscriber_books)
df_subscriber_books.printSchema()
#df_subscriber_books.createOrReplaceTempView("cdc_store_orders")


# COMMAND ----------

df_subscriber = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                        .option("multiline", "true") \
                         .json("/Volumes/my_first_catalog/my_first_schema/my_staging_data/subscriber.json")
#df_subscriber = df_subscriber.withColumn("order_date", to_date(df_subscriber.order_date,  'MM/dd/yyyy'))
display(df_subscriber)
#df_subscriber.show()
#df_subscriber.printSchema()
#df_subscriber.createOrReplaceTempView("cdc_store_orders")

# COMMAND ----------

df_books = spark.read                          \
                         .option("header", "true")      \
                         .option("inferSchema", "true") \
                         .csv("/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv")
#df_books = df_subscriber.withColumn("order_date", to_date(df_books.order_date,  'MM/dd/yyyy'))
display(df_books)
df_books.printSchema()
#df_books.createOrReplaceTempView("cdc_store_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW subscribers
# MAGIC USING json
# MAGIC OPTIONS (path "/Volumes/my_first_catalog/my_first_schema/my_staging_data/subscriber.json", multiline "true");
# MAGIC
# MAGIC SELECT
# MAGIC   id,
# MAGIC   name,
# MAGIC   EXPLODE(
# MAGIC     TRANSFORM(
# MAGIC       phone_numbers,
# MAGIC       p -> named_struct(
# MAGIC         'type', p.type,
# MAGIC         'number_original', p.number,
# MAGIC         'number_formatted',
# MAGIC           CASE
# MAGIC             WHEN length(regexp_replace(p.number,'\\D','')) = 11
# MAGIC                  AND substr(regexp_replace(p.number,'\\D',''),1,1) = '1'
# MAGIC               THEN concat_ws('-',
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 2, 3),
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 5, 3),
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 8, 4))
# MAGIC             WHEN length(regexp_replace(p.number,'\\D','')) = 10
# MAGIC               THEN concat_ws('-',
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 1, 3),
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 4, 3),
# MAGIC                      substr(regexp_replace(p.number,'\\D',''), 7, 4))
# MAGIC             ELSE NULL
# MAGIC           END
# MAGIC       )
# MAGIC     )
# MAGIC   ) AS ph
# MAGIC FROM subscribers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_csv
# MAGIC USING csv
# MAGIC OPTIONS (path "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv", header "true", inferSchema "true");
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_csv
# MAGIC WHERE CAST(height AS DOUBLE) > 200;
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

path = "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv"  # your uploaded file
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path))

# Filter and display
tall = df.filter(F.col("height") > 200)

# See the rows (adjust columns if needed)
tall.select("*").show(truncate=False)

# If you want a Python list of subscriber names (change 'name' to your column):
names = [r["height"] for r in tall.select("height").collect()]
print(names)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_csv
# MAGIC USING csv
# MAGIC OPTIONS (path "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv", header "true", inferSchema "true");
# MAGIC
# MAGIC SELECT publisher, genre
# MAGIC FROM books_csv
# MAGIC WHERE genre = 'computer_science';

# COMMAND ----------

from pyspark.sql import functions as F

path = "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv"  # your uploaded file
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path))

# Filter and display
tall = df.filter(F.col("genre") == 'computer_science')

# See the rows (adjust columns if needed)
tall.select("*").show(truncate=False)

# If you want a Python list of subscriber names (change 'name' to your column):
publisher = [r["publisher"] for r in tall.select("publisher").collect()]
print(publisher)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_csv
# MAGIC USING csv
# MAGIC OPTIONS (path "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv", header "true", inferSchema "true");
# MAGIC
# MAGIC SELECT publisher, count(*)
# MAGIC FROM books_csv
# MAGIC group by publisher order by 2 desc;

# COMMAND ----------

from pyspark.sql import functions as F

path = "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv"  # your uploaded file
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path))

counts = (df.groupBy("publisher")
            .count()
            .orderBy(F.desc("count")))

counts.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_csv
# MAGIC USING csv
# MAGIC OPTIONS (path "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv", header "true", inferSchema "true");
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_csv
# MAGIC where lower(title) like '%vol i%';

# COMMAND ----------

from pyspark.sql import functions as F

path = "/Volumes/my_first_catalog/my_first_schema/my_staging_data/books.csv"  # your uploaded file
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path))

# Filter and display
tall = df.filter(F.col("title").like('%Vol I%'))

# See the rows (adjust columns if needed)
tall.select("*").show(truncate=False)

# If you want a Python list of subscriber names (change 'name' to your column):
publisher = [r["publisher"] for r in tall.select("publisher").collect()]
print(publisher)

# COMMAND ----------

