-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Create your bronze layer tables ingesting from the landing zone

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_raw
AS SELECT *, current_timestamp() as load_time
FROM cloud_files('/Volumes/dev/demo_db/landing_zone/customers', 
                 "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invoices_raw
AS SELECT *, current_timestamp() as load_time
FROM cloud_files("/Volumes/dev/demo_db/landing_zone/invoices", 
                 "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create your silver layer tables reading incremental data from bronze layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_cleaned (
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW)
AS 
SELECT CustomerID as customer_id, CustomerName as customer_name, load_time   
FROM STREAM(LIVE.customers_raw)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invoices_cleaned (
    CONSTRAINT valid_invoice_and_qty EXPECT (invoice_no IS NOT NULL AND quantity > 0) ON VIOLATION DROP ROW)
    PARTITIONED BY (invoice_year, country)
AS
SELECT InvoiceNo as invoice_no, StockCode as stock_code, Description as description,
        Quantity as quantity, to_date(InvoiceDate, "d-M-y H.m") as invoice_date, 
        UnitPrice as unit_price, CustomerID as customer_id, Country as country,
        year(to_date(InvoiceDate, "d-M-y H.m")) as invoice_year, 
        month(to_date(InvoiceDate, "d-M-y H.m")) as invoice_month,
        load_time
FROM STREAM(LIVE.invoices_raw)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Build your SCD Type 2 dimensions using CDC from silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers;

APPLY CHANGES INTO LIVE.customers
FROM STREAM(LIVE.customers_cleaned)
KEYS (customer_id)
SEQUENCE BY load_time
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Merge into your fact table using CDC from the silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invoices PARTITIONED BY (invoice_year, country);

APPLY CHANGES INTO LIVE.invoices
FROM STREAM(LIVE.invoices_cleaned)
KEYS (invoice_no, stock_code, invoice_date)
SEQUENCE BY load_time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Materialize your gold layer summary using silver layer fact

-- COMMAND ----------

CREATE LIVE TABLE daily_sales_uk_2022
AS SELECT country, invoice_year, invoice_month, invoice_date,
          round(sum(quantity*unit_price),2) as total_sales
FROM LIVE.invoices
WHERE invoice_year = 2022 AND country="United Kingdom"
GROUP BY country, invoice_year, invoice_month, invoice_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
