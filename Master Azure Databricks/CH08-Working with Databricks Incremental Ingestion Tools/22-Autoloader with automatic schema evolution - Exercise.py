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
# MAGIC #####2. Ingest data into invoices_raw table using spark streaming api

# COMMAND ----------

def ingest():
  source_df = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "csv")  
                      .option("header", "true") 
                      .option("timestampFormat","d-M-y H.m")                  
                      .option("cloudFiles.schemaLocation", f"{base_dir}/chekpoint/invoices_schema")
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaHints", "InvoiceNo string, CustomerID string")
                      .load(f"{base_dir}/invoices")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .option("mergeSchema", "true")
                          .outputMode("append")                          
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db.invoices_raw")
  )

ingest() 

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Check the records after ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Ingest some more data into the invoices directory which comes with an additional column

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices/

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Ingest with a retry

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####6. Check the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Ingest some more records with potential bad records

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2022.csv /mnt/files/dataset_ch8/invoices/

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####8. Check the rescued data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw where _rescued_data is not null

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
