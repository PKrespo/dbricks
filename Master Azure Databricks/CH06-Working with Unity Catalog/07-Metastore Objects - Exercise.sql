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
-- MAGIC #####1. Create a DEV and a QA catalog.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create a demo_db database in DEV catalog.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Drop default database from QA catalog.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Grant following privilages to scholarnest dev group
-- MAGIC 1. USE CATALOG on DEV
-- MAGIC 2. ALL PRIVILEGES on demo_db

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Create volume under demo_db

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Upload a data file to your volume

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. List the content of your volume

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Check out Catalog Explorer UI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Create an external location to an existing container/directory in ADLS (Use the Catalog explorer)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####10. Describe external location

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####11. Grant READ FILES privilage on external location to scholarnest dev

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####12. List content of the external location

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####13. Most commonly used securable objects
-- MAGIC Catalog, Schema, Table, View, Volume, External location
-- MAGIC List of privilages by object
-- MAGIC
-- MAGIC [Azure Databricks Doc for Privilages](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges#--privilege-types-by-securable-object-in-unity-catalog)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
