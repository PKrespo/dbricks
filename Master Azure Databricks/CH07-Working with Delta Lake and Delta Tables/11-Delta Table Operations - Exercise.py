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

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev;
# MAGIC CREATE DATABASE IF NOT EXISTS dev.demo_db;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dev.demo_db.people(
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   lastName STRING,
# MAGIC   birthDate STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT OVERWRITE TABLE dev.demo_db.people
# MAGIC SELECT id, fname as firstName, lname as lastName, dob as birthDate
# MAGIC FROM JSON.`/mnt/files/dataset_ch7/people.json`;
# MAGIC
# MAGIC SELECT * FROM dev.demo_db.people;

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Delete one record from the above table using Spark SQL

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Delete one record from the above table using API

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Update one record in the delta table using API

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Merge the given dataframe into the delta table

# COMMAND ----------

source_df = spark.read.format("json").load("/mnt/files/dataset_ch7/people.json")
display(source_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
