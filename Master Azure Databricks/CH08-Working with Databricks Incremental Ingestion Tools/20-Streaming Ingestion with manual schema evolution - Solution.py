# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cleanup previous runs

# COMMAND ----------

# MAGIC %run ../utils/cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC #####Setup

# COMMAND ----------

base_dir = "/mnt/files/dataset_ch8"
spark.sql("CREATE CATALOG IF NOT EXISTS dev")
spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Verify you can access the invoices directory

# COMMAND ----------

# MAGIC %fs ls /mnt/files/dataset_ch8/invoices

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Create a delta table to ingest invoices data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.demo_db.invoices_raw(
# MAGIC   InvoiceNo int,
# MAGIC   StockCode string,
# MAGIC   Description string,
# MAGIC   Quantity int,
# MAGIC   InvoiceDate timestamp,
# MAGIC   UnitPrice double,
# MAGIC   CustomerID int)

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Ingest data into invoices_raw table using spark streaming api

# COMMAND ----------

def ingest():
  invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int"""
                    
  source_df = (spark.readStream
                      .format("csv")
                      .option("header", "true")
                      .schema(invoice_schema)
                      .load(f"{base_dir}/invoices")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .outputMode("append")
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db.invoices_raw")
  )

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Check the records after ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####6. Ingest some more data into the invoices directory which comes with an additional column

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Your ingestion code will not break but silently ignore the additional column

# COMMAND ----------

# MAGIC %md
# MAGIC ######7.1 Alter table to evolve the schema

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev.demo_db.invoices_raw ADD COLUMNS (Country string)

# COMMAND ----------

# MAGIC %md
# MAGIC ######7.2 Modify streaming ingestion to accomodate shcema changes

# COMMAND ----------

def ingest():
  invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int, Country string"""
  source_df = (spark.readStream
                      .format("csv")
                      .option("header", "true")
                      .schema(invoice_schema)
                      .load(f"{base_dir}/invoices")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .outputMode("append")
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db.invoices_raw")
  )

ingest()  

# COMMAND ----------

# MAGIC %md
# MAGIC #####9. Check the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
