# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Create your bronze layer tables ingesting from the landing zone

# COMMAND ----------

@dlt.table(name="customers_raw")
def get_customers_raw():
  return (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "csv")
               .option("cloudFiles.inferColumnTypes", "true")
               .load("/Volumes/dev/demo_db/landing_zone/customers")
               .withColumn("load_time", current_timestamp())
        )

# COMMAND ----------

@dlt.table(name="invoices_raw")
def get_invoices_raw():
  return (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "csv")
               .option("cloudFiles.inferColumnTypes", "true")
               .load("/Volumes/dev/demo_db/landing_zone/invoices")
               .withColumn("load_time", current_timestamp())
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Create your silver layer tables reading incremental data from bronze layer

# COMMAND ----------

@dlt.table(name="customers_cleaned")
@dlt.expect_or_drop("valid_customer", "customer_id is not null")
def get_customers_cleaned():
  return (spark.readStream
               .format("delta")
               .table("live.customers_raw")
               .selectExpr("CustomerID as customer_id", "CustomerName as customer_name", "load_time")
        )

# COMMAND ----------

@dlt.table(name="invoices_cleaned", partition_cols = ["invoice_year", "country"])
@dlt.expect_or_drop("valid_invoice_and_qty", "invoice_no is not null and quantity > 0")
def get_invoices_cleaned():
  return (spark.readStream
               .format("delta")
               .table("live.invoices_raw")
               .selectExpr("InvoiceNo as invoice_no", "StockCode as stock_code", "Description as description",
                           "Quantity as quantity", "to_date(InvoiceDate, 'd-M-y H.m') as invoice_date", 
                           "UnitPrice as unit_price", "CustomerID as customer_id", "Country as country",
                           "year(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_year", 
                           "month(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_month", "load_time"                   
               )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Build your SCD Type 2 dimensions using CDC from silver layer

# COMMAND ----------

dlt.create_streaming_table("customers")

dlt.apply_changes(
  target = "customers",
  source = "customers_cleaned",
  keys = ["customer_id"],
  sequence_by = col("load_time"),
  stored_as_scd_type = 2
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Merge into your fact table using CDC from the silver layer

# COMMAND ----------

dlt.create_streaming_table("invoices", partition_cols = ["invoice_year", "country"])

dlt.apply_changes(
  target = "invoices",
  source = "invoices_cleaned",
  keys = ["invoice_no", "stock_code", "invoice_date"],
  sequence_by = col("load_time")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Materialize your gold layer summary using silver layer fact

# COMMAND ----------

@dlt.table(name="daily_sales_uk_2022")
def compute_daily_sales_uk_2022():
  return (spark.read
               .format("delta")
               .table("live.invoices")
               .where("invoice_year = 2022 AND country = 'United Kingdom'")
               .groupBy("country", "invoice_year", "invoice_month", "invoice_date")
               .agg(expr("round(sum(quantity*unit_price),2)").alias("total_sales"))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
