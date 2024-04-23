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

-- MAGIC %python
-- MAGIC CL = Cleanup()
-- MAGIC def setup():
-- MAGIC         spark.sql("CREATE CATALOG IF NOT EXISTS dev")
-- MAGIC         spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db")
-- MAGIC
-- MAGIC         raw_df = (spark.read
-- MAGIC                 .format("csv")
-- MAGIC                 .option("header", "true")
-- MAGIC                 .option("inferSchema","true")
-- MAGIC                 .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")
-- MAGIC         )
-- MAGIC
-- MAGIC         from pyspark.sql.functions import to_date, to_timestamp, round, year
-- MAGIC         staging_df = (raw_df.withColumnRenamed("Call Number", "CallNumber")
-- MAGIC                         .withColumnRenamed("Unit ID", "UnitID")
-- MAGIC                         .withColumnRenamed("Incident Number", "IncidentNumber")
-- MAGIC                         .withColumnRenamed("Call Date", "CallDate")
-- MAGIC                         .withColumnRenamed("Watch Date", "WatchDate")
-- MAGIC                         .withColumnRenamed("Call Final Disposition", "CallFinalDisposition")
-- MAGIC                         .withColumnRenamed("Available DtTm", "AvailableDtTm")
-- MAGIC                         .withColumnRenamed("Zipcode of Incident", "Zipcode")
-- MAGIC                         .withColumnRenamed("Station Area", "StationArea")
-- MAGIC                         .withColumnRenamed("Final Priority", "FinalPriority")
-- MAGIC                         .withColumnRenamed("ALS Unit", "ALSUnit")
-- MAGIC                         .withColumnRenamed("Call Type Group", "CallTypeGroup")
-- MAGIC                         .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch")
-- MAGIC                         .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict")
-- MAGIC                         .withColumnRenamed("Supervisor District", "SupervisorDistrict")
-- MAGIC                         .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy"))
-- MAGIC                         .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy"))
-- MAGIC                         .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
-- MAGIC                         .withColumn("Delay", round("Delay", 2))
-- MAGIC                         .withColumn("Year", year("CallDate"))
-- MAGIC         )
-- MAGIC
-- MAGIC         (staging_df.write
-- MAGIC                 .format("delta")
-- MAGIC                 .mode("overwrite")
-- MAGIC                 .saveAsTable("dev.demo_db.fire_calls_tbl")
-- MAGIC         )
-- MAGIC setup()        

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####VACUUM utility

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Describe extended table and watchout the table directory in Azure container

-- COMMAND ----------

describe extended dev.demo_db.fire_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Apply some transactions

-- COMMAND ----------

delete from dev.demo_db.fire_calls_tbl where CallDate = "2002-01-24"

-- COMMAND ----------

update dev.demo_db.fire_calls_tbl set Delay = int(Delay)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Show table history

-- COMMAND ----------

describe history dev.demo_db.fire_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Query older versiosn

-- COMMAND ----------

select * from dev.demo_db.fire_calls_tbl version as of 0 where CallDate = "2002-01-24" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Vacuum the table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.1 Count the data files from the backend

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.2 Disable safety check

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.3 Vacuum the table with zero retention

-- COMMAND ----------

VACUUM dev.demo_db.fire_calls_tbl RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.4 Count the data files from the backend

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.5 Read older version of the table

-- COMMAND ----------

select * from dev.demo_db.fire_calls_tbl version as of 0 where CallDate = "2002-01-24" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####REORG and VACUUM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Remove some columns from your table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######1.1 Enable column mapping

-- COMMAND ----------

ALTER TABLE dev.demo_db.fire_calls_tbl SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######1.2 Alter table to remove columns

-- COMMAND ----------

ALTER TABLE dev.demo_db.fire_calls_tbl DROP columns(SupervisorDistrict, FirePreventionDistrict)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######1.3 Check history

-- COMMAND ----------

describe history dev.demo_db.fire_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Reorganize your data files 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######2.1 REORG your table

-- COMMAND ----------

REORG TABLE dev.demo_db.fire_calls_tbl APPLY(PURGE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######2.2 Check files from the backend

-- COMMAND ----------

VACUUM dev.demo_db.fire_calls_tbl RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Optimize and Zorder
-- MAGIC 1. OPTIMIZE - Create evenly-balanced data files with respect to their size on disk
-- MAGIC 2. ZORDER  - Colocate the data by column

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######1. OPTIMIZE and ZORDER

-- COMMAND ----------

OPTIMIZE dev.demo_db.fire_calls_tbl ZORDER BY (Year, CallDate)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######2. VACUUM

-- COMMAND ----------

VACUUM dev.demo_db.fire_calls_tbl RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
