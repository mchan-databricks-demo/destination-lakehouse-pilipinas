# Databricks notebook source
# MAGIC %md
# MAGIC # Step 0: Defining the Tiers in the Architecture

# COMMAND ----------

# Change to your S3 path once Databricks is mounted on S3 
t0rawLayer = "/mnt/00-mchan-demo/mchan-retail-superstore/t0-raw-layer/"
t1BronzeLayer = "/mnt/00-mchan-demo/mchan-retail-superstore/t1-bronze-layer/"
t2SilverLayer = "/mnt/00-mchan-demo/mchan-retail-superstore/t2-silver-layer/"
t3GoldLayer = "/mnt/00-mchan-demo/mchan-retail-superstore/t3-gold-layer/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Data Ingestion

# COMMAND ----------

# import 4 years' of data from S3 
df = (
  spark.read
       .format("csv")
       .option("inferSchema", "true")
       .option("header", "true")
       .load(t0rawLayer)
)

# COMMAND ----------

display(df.head(4))

# COMMAND ----------

dfRowCount = df.groupBy("dataYear").count().show()

# COMMAND ----------

# Write the data into the Bronze Layer in S3 
(
df.write
  .mode("overwrite")
  .format("delta")
  .partitionBy("dataYear")
  .save(t1BronzeLayer)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Data Transformation

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS mchan_end_to_end_db;
# MAGIC USE mchan_end_to_end_db; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE t1_bronze_orders 
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM DELTA.`/mnt/00-mchan-demo/mchan-retail-superstore/t1-bronze-layer/`; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTION 1: USE SQL TO TRANSFORM -- 
# MAGIC SELECT * 
# MAGIC FROM t1_bronze_orders
# MAGIC ORDER BY rowID 
# MAGIC LIMIT 3

# COMMAND ----------

# OPTION 2: USE PYTHON - Either Pandas or PySpark # 
df = spark.table("mchan_catalog.mchan_end_to_end_db.t1_bronze_orders")
pandas_df = df.toPandas()
pandas_df.sort_values("rowID").head(3)
