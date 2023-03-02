# Databricks notebook source
# Option to use PySpark 
df = spark.table("mchan_catalog.xxxxx.t2_silver_sales_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog mchan_catalog; 
# MAGIC USE mchan_end_to_end_db; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Option to use SparkSQL 
# MAGIC CREATE OR REPLACE TABLE t2_silver_sales_table 
# MAGIC AS
# MAGIC SELECT 
# MAGIC   dataYear AS data_year, 
# MAGIC   date_trunc('week', orderDate)::date as order_week,
# MAGIC   date_trunc('month', orderDate)::date as order_month, 
# MAGIC   date_trunc('quarter', orderDate)::date as order_quarter,
# MAGIC   round(sum(sales),0) AS sales_revenue 
# MAGIC FROM t1_bronze_orders
# MAGIC GROUP BY 1,2,3,4
# MAGIC ORDER BY 1,2;
