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
-- MAGIC #####2. Create a delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.demo_db.invoices_raw(
  InvoiceNo string,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate timestamp,
  UnitPrice double,
  CustomerID string)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Ingest data into invoices_raw table using copy into command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.1 Ingest using copy into command

-- COMMAND ----------

COPY INTO dev.demo_db.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string
      FROM "/mnt/files/dataset_ch8/invoices")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.2 Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.3 COPY INTO is idempotent

-- COMMAND ----------

COPY INTO dev.demo_db.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string
      FROM "/mnt/files/dataset_ch8/invoices")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.4 Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Collect more data into the invoices directory which comes with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Your ingestion code will not break but silently ignore the additional column

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.1 Alter table to mnaully accomodate the additional field

-- COMMAND ----------

ALTER TABLE dev.demo_db.invoices_raw ADD COLUMNS (Country string)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.2 Modify your ingestion code to manually accomodate the additional field

-- COMMAND ----------

COPY INTO dev.demo_db.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string, Country::string
      FROM "/mnt/files/dataset_ch8/invoices")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.3 Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
