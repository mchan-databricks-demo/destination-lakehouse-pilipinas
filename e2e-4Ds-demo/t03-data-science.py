# Databricks notebook source
# MAGIC %md
# MAGIC # Build a Sales Forecast Model using ```Prophet```

# COMMAND ----------

# MAGIC %pip install Prophet

# COMMAND ----------

import pandas as pd
from prophet import Prophet

# COMMAND ----------

# Load data into a DataFrame
df = spark.table("mchan_catalog.mchan_end_to_end_db.t2_silver_sales_table").select("order_quarter", "sales_revenue")
df = df.toPandas()
df.head(3)

# COMMAND ----------

# Define the column names for the date and sales columns
df.columns = ['ds', 'y']

# Create a Prophet model with quarterly seasonality
m = Prophet(seasonality_mode='multiplicative', yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
m.add_seasonality(name='quarterly', period=90.5, fourier_order=5)

# Fit the model to the data
m.fit(df)

# Create a DataFrame to hold the future dates
future = m.make_future_dataframe(periods=12, freq='Q')

# Generate the forecast
forecast = m.predict(future)

# Plot the forecast
m.plot(forecast)

# COMMAND ----------

spark_df = spark.createDataFrame(forecast)
display(spark_df)

# COMMAND ----------

spark_df.write.mode("overwrite").saveAsTable("mchan_catalog.mchan_end_to_end_db.t3_gold_sales_forecast")
