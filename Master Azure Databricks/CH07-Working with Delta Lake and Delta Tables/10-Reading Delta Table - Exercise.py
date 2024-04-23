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

base_dir = "/mnt/files"
spark.sql(f"CREATE CATALOG IF NOT EXISTS dev")
spark.sql(f"CREATE DATABASE IF NOT EXISTS dev.demo_db")

flight_schema_ddl = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""

flight_time_df = (spark.read.format("json")
                    .schema(flight_schema_ddl)
                    .option("dateFormat", "M/d/y")
                    .load(f"{base_dir}/dataset_ch7/flight-time.json")
)

flight_time_df.write.format("delta").mode("overwrite").saveAsTable("dev.demo_db.flight_time_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Read delta table using Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dev.demo_db.flight_time_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Read delta table using dataframe api

# COMMAND ----------

spark.read.format("delta").table("dev.demo_db.flight_time_tbl").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Read delta format data from an external location
# MAGIC 1. Create external location
# MAGIC 2. Read using dataframe API
# MAGIC 3. Create external table

# COMMAND ----------

# MAGIC %md
# MAGIC #####3.1. Create external location

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####3.2. Read using Dataframe API

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####3.3. Create external table

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
