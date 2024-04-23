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

spark.sql("CREATE CATALOG IF NOT EXISTS dev")
spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db")
spark.sql("CREATE VOLUME IF NOT EXISTS dev.demo_db.files")

raw_df = (spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema","true")
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")
)

from pyspark.sql.functions import to_date, to_timestamp, round, year
staging_df = (raw_df.withColumnRenamed("Call Number", "CallNumber")
                    .withColumnRenamed("Unit ID", "UnitID")
                    .withColumnRenamed("Incident Number", "IncidentNumber")
                    .withColumnRenamed("Call Date", "CallDate")
                    .withColumnRenamed("Watch Date", "WatchDate")
                    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition")
                    .withColumnRenamed("Available DtTm", "AvailableDtTm")
                    .withColumnRenamed("Zipcode of Incident", "Zipcode")
                    .withColumnRenamed("Station Area", "StationArea")
                    .withColumnRenamed("Final Priority", "FinalPriority")
                    .withColumnRenamed("ALS Unit", "ALSUnit")
                    .withColumnRenamed("Call Type Group", "CallTypeGroup")
                    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch")
                    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict")
                    .withColumnRenamed("Supervisor District", "SupervisorDistrict")
                    .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy"))
                    .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy"))
                    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
                    .withColumn("Delay", round("Delay", 2))
                    .withColumn("Year", year("CallDate"))
)

(staging_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("Year")
        .save("/Volumes/dev/demo_db/files/fire_calls_tbl")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Check the details of the parquet data set

# COMMAND ----------

# MAGIC %fs ls /Volumes/dev/demo_db/files/fire_calls_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Convert Parquet data set to delta data set

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/Volumes/dev/demo_db/files/fire_calls_tbl`
# MAGIC partitioned by (Year int)

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Show the version history for fire_calls_tbl data after converting to delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/Volumes/dev/demo_db/files/fire_calls_tbl`

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
