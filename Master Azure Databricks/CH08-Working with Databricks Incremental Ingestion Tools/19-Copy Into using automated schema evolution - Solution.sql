-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Cleanup previous runs

-- COMMAND ----------

-- MAGIC %run ../utils/cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Setup

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev;
CREATE DATABASE IF NOT EXISTS dev.demo_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Verify you can access the invoices directory

-- COMMAND ----------

-- MAGIC %fs ls /mnt/files/dataset_ch8/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create a schemaless delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.demo_db.invoices_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Describe the shemaless table

-- COMMAND ----------

DESCRIBE EXTENDED dev.demo_db.invoices_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Ingest data into invoices_raw table using copy into command

-- COMMAND ----------

COPY INTO dev.demo_db.invoices_raw
FROM "/mnt/files/dataset_ch8/invoices"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'd-M-y H.m', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Describe the table after ingestion

-- COMMAND ----------

DESCRIBE dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Ingest some more data into the invoices directory with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Ingest data into invoices_raw table again

-- COMMAND ----------

COPY INTO dev.demo_db.invoices_raw
FROM "/mnt/files/dataset_ch8/invoices"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'd-M-y H.m', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Check the record count and records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Describe the invoices_raw table after ingestion

-- COMMAND ----------

DESCRIBE dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
