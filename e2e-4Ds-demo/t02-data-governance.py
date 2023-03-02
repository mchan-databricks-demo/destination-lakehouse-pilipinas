# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG mchan_catalog;
# MAGIC USE mchan_end_to_end_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can also use the Databricks SQL UI to do the same 
# MAGIC GRANT ALL PRIVILEGES ON DATABASE mchan_end_to_end_db TO `dataengineers`;
# MAGIC GRANT CREATE TABLE ON DATABASE mchan_end_to_end_db TO `datascientists`;
# MAGIC GRANT CREATE VIEW ON DATABASE mchan_end_to_end_db TO `datascientists`;
# MAGIC GRANT CREATE FUNCTION ON DATABASE mchan_end_to_end_db TO `datascientists`;
# MAGIC GRANT SELECT ON TABLE t1_bronze_orders TO `datascientists`;
# MAGIC GRANT SELECT ON TABLE t1_bronze_orders TO `ANALYST_FR`;
# MAGIC GRANT SELECT ON TABLE t1_bronze_orders TO `ANALYST_USA`;
