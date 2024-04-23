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
CREATE OR REPLACE TABLE dev.demo_db.people_tbl(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Schema Validations
-- MAGIC
-- MAGIC #####Statements
-- MAGIC 1. INSERT
-- MAGIC 2. OVERWRITE
-- MAGIC 3. MERGE
-- MAGIC 4. DataFrame Append
-- MAGIC
-- MAGIC #####Validation Scenarions
-- MAGIC 1. Column matching approach
-- MAGIC 2. New Columns
-- MAGIC 3. Data Type Mismatch (Not allowed in any case)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Schema Validations Summary
-- MAGIC 1. INSERT &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&ensp;&nbsp;- Column matching by position, New columns not allowed
-- MAGIC 2. OVERWRITE &emsp;&emsp;&emsp;&emsp;&ensp;- Column matching by position, New columns not allowed
-- MAGIC 3. MERGE INSERT &emsp;&emsp;&emsp;&nbsp;- Column matching by name, New columns ignored
-- MAGIC 4. DataFrame Append &emsp;&nbsp;- Column matching by name, New columns not allowed
-- MAGIC 5. Data Type Mismatch &emsp;- Not allowed in any case

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. INSERT - Column matching by position (matching names not mandatory)
-- MAGIC This has a potential to corrupt your data

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname, lname
FROM json.`/mnt/files/dataset_ch7/people.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. INSERT - New columns not allowed

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname, lname, dob
FROM json.`/mnt/files/dataset_ch7/people.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. OVERWRITE - New columns not allowed

-- COMMAND ----------

INSERT OVERWRITE dev.demo_db.people_tbl
SELECT id, fname, lname, dob
FROM json.`/mnt/files/dataset_ch7/people.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. MERGE - Column matching by name (matching by position not allowed)

-- COMMAND ----------

SELECT id, fname, lname FROM json.`/mnt/files/dataset_ch7/people_2.json`

-- COMMAND ----------

MERGE INTO dev.demo_db.people_tbl tgt
USING (SELECT id, fname, lname FROM json.`/mnt/files/dataset_ch7/people_2.json`) src
ON tgt.id = src.id
WHEN NOT MATCHED THEN
    INSERT *  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. MERGE - New columns silently ignored

-- COMMAND ----------

SELECT id, fname firstName, lname lastName, dob FROM json.`/mnt/files/dataset_ch7/people_2.json`

-- COMMAND ----------

MERGE INTO dev.demo_db.people_tbl tgt
USING (SELECT id, fname firstName, lname lastName, dob FROM json.`/mnt/files/dataset_ch7/people_2.json`) src
ON tgt.id = src.id
WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Dataframe append - Column matching by name (matching by position not allowed)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING"
-- MAGIC people_df =  spark.read.format("json").schema(people_schema).load("/mnt/files/dataset_ch7/people_2.json")
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("dev.demo_db.people_tbl")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Dataframe append - New columns not allowed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_schema = "id INT, firstName STRING, lastName STRING, dob STRING"
-- MAGIC people_df =  spark.read.format("json").schema(people_schema).load("/mnt/files/dataset_ch7/people_2.json")
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("dev.demo_db.people_tbl")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
