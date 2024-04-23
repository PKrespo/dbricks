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
# MAGIC ######Ensure you have dataset_ch7 setup at the mount point

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

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Create a delta table uing Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.demo_db.flight_time_tbl (
# MAGIC     FL_DATE DATE, 
# MAGIC     OP_CARRIER STRING, 
# MAGIC     OP_CARRIER_FL_NUM INT, 
# MAGIC     ORIGIN STRING, 
# MAGIC     ORIGIN_CITY_NAME STRING, 
# MAGIC     DEST STRING, 
# MAGIC     DEST_CITY_NAME STRING, 
# MAGIC     CRS_DEP_TIME INT, 
# MAGIC     DEP_TIME INT, 
# MAGIC     WHEELS_ON INT, 
# MAGIC     TAXI_IN INT, 
# MAGIC     CRS_ARR_TIME INT, 
# MAGIC     ARR_TIME INT, 
# MAGIC     CANCELLED STRING, 
# MAGIC     DISTANCE INT
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Load data into delta table

# COMMAND ----------

flight_time_df.write.format("delta").mode("append").saveAsTable("dev.demo_db.flight_time_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db.flight_time_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Create a delta table using Delta Table Builder API

# COMMAND ----------

from delta import DeltaTable

(DeltaTable.createOrReplace(spark)
    .tableName("dev.demo_db.flight_time_tbl")
    .addColumn("id", "INT")
    .addColumn("FL_DATE", "DATE")
    .addColumn("OP_CARRIER", "STRING")
    .addColumn("OP_CARRIER_FL_NUM", "INT")
    .addColumn("ORIGIN", "STRING")
    .addColumn("ORIGIN_CITY_NAME", "STRING")
    .addColumn("DEST", "STRING") 
    .addColumn("DEST_CITY_NAME", "STRING")
    .addColumn("CRS_DEP_TIME", "INT")
    .addColumn("DEP_TIME", "INT")
    .addColumn("WHEELS_ON", "INT")
    .addColumn("TAXI_IN", "INT")
    .addColumn("CRS_ARR_TIME", "INT")
    .addColumn("ARR_TIME", "INT")
    .addColumn("CANCELLED", "STRING")
    .addColumn("DISTANCE", "INT")
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db.flight_time_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
