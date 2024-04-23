-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####1. Create a multinode shared mode cluster to work with DLT generated tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Cleanup previous runs

-- COMMAND ----------

-- MAGIC %run ../utils/cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Setup the catalog and external location

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev;
CREATE DATABASE IF NOT EXISTS dev.demo_db;

CREATE EXTERNAL LOCATION IF NOT EXISTS external_data
URL 'abfss://dbfs-container@prashantsa.dfs.core.windows.net/dataset_ch9'
WITH (CREDENTIAL `scholarnest-storage-credential`);

CREATE EXTERNAL VOLUME IF NOT EXISTS dev.demo_db.landing_zone
LOCATION 'abfss://dbfs-container@prashantsa.dfs.core.windows.net/dataset_ch9';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Ingest some data

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC mkdirs /Volumes/dev/demo_db/landing_zone/customers/

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC mkdirs /Volumes/dev/demo_db/landing_zone/invoices/

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/customers_1.csv /Volumes/dev/demo_db/landing_zone/customers

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/invoices_2021.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/invoices_2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Create and run the DLT pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Check the final results table

-- COMMAND ----------

select * from dev.demo_db.daily_sales_uk_2022

-- COMMAND ----------

describe extended dev.demo_db.daily_sales_uk_2022

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Ingest some more data

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/customers_2.csv /Volumes/dev/demo_db/landing_zone/customers

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/invoices_01-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/invoices_02-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/landing_zone/invoices_03-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Run the DLT pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Check the results table

-- COMMAND ----------

select * from dev.demo_db.daily_sales_uk_2022

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####10. Check your SCD2 dimension

-- COMMAND ----------

select * from dev.demo_db.customers where customer_id=15311
