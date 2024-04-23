# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Create a Python Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Execute the following SQL statement from the notebook
# MAGIC ```
# MAGIC SELECT "Hello World!"
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello World!"

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Execute the following Scala code from the notebook
# MAGIC ```scala
# MAGIC     var msg = "Hello World!"
# MAGIC     println(msg)
# MAGIC ```

# COMMAND ----------

# MAGIC %scala
# MAGIC var msg = "Hello World!"
# MAGIC println(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Create following markdown at the last cell of your notebook
# MAGIC ```html
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
# MAGIC ```  

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Use the file system command to list the content of the following directory
# MAGIC ```
# MAGIC /databricks-datasets
# MAGIC ```

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #####6.Use the file system command to explore airlines dataset in the databricks example data sets.

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/airlines

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Use the tail shell command to show few lines of a airlines data file

# COMMAND ----------

# MAGIC %sh
# MAGIC tail /databricks-datasets/airlines/part-00000

# COMMAND ----------

# MAGIC %md
# MAGIC #####8. Show the list of all magic commands in a Databricks Notebook

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %md
# MAGIC #####9. Show the documentation of env magic command

# COMMAND ----------

# MAGIC %env?

# COMMAND ----------

# MAGIC %md
# MAGIC #####10. Show the source code of the pip magic command

# COMMAND ----------

# MAGIC %pip??

# COMMAND ----------

# MAGIC %md
# MAGIC #####11. Run the entire notebook using a single click

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
