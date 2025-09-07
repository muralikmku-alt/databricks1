# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT assert_true(current_version().dbsql_version is not null, 'YOU MUST USE A SQL WAREHOUSE OR SERVERLESS, not a classic cluster');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_query(
# MAGIC   "databricks-meta-llama-3-3-70b-instruct",
# MAGIC   'Describe AI in 100 words'
# MAGIC   ) AS summary
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   AI_QUERY(
# MAGIC     "databricks-gpt-oss-120b",
# MAGIC     "Generate a Python program to add two values"
# MAGIC   ) as python_program

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT currency,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     "Can you tell me the name of currency? currency: " || currency
# MAGIC     ) as currency_detail
# MAGIC   FROM ctlg_electroniz.sch_bronze.tbl_bronze_orders
# MAGIC   LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_gen('Generate a email to thank Manoj for the excellent course') as email_text;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC   ai_gen(
# MAGIC     "Can you list out the top 5 states by country in this country? country: " || country
# MAGIC     ) as currency_detail
# MAGIC   FROM ctlg_electroniz.sch_bronze.tbl_bronze_customers
# MAGIC   LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_fix_grammar('We are going vacation next day') fixed_grammar;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_classify("There is a fire in the building.", ARRAY("enter building", "leave building")) as classified_text;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name,
# MAGIC   ai_classify(product_name,
# MAGIC     ARRAY('clothing', 'shoes', 'electronics', 'furniture') 
# MAGIC     ) as product_classification
# MAGIC   FROM ctlg_electroniz.sch_bronze.tbl_bronze_products
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_translate("Manoj is an OK teacher!", "sp") as spanish

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT address,
# MAGIC   ai_translate(address , "fr"
# MAGIC     ) as french_address
# MAGIC   FROM ctlg_electroniz.sch_bronze.tbl_bronze_customers
# MAGIC   LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_extract(
# MAGIC     'Both Lambda and Kappa architectures are layered—data is collected in the storage/
# MAGIC streaming layer, processed, and finally published to a warehouse.',
# MAGIC     array('architecture', 'layers', 'data') 
# MAGIC   ) AS extractions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC     'My phone number is 647-333-5467, you may fax us at 647-789-4323',
# MAGIC     array('phone', 'fax')
# MAGIC   ) as masked_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT credit_card, address,
# MAGIC   ai_mask(credit_card,
# MAGIC   array('credit_card')
# MAGIC     ) as masked_cc,
# MAGIC     ai_mask(address,
# MAGIC   array('address number')
# MAGIC     ) as masked_address
# MAGIC   FROM ctlg_electroniz.sch_bronze.tbl_bronze_customers
# MAGIC   LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_parse_document(content) as content
# MAGIC   FROM READ_FILES('/Volumes/my_first_catalog/my_first_schema/my_staging_volume/book/book_page.pdf', format => 'binaryFile')
# MAGIC