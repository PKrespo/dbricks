# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Show the list of all available databricks utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Show the documentation for fs utility

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Show the documentation for the cp command of the fs utiltity

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Use the file system command to list the content of the following directory
# MAGIC ```
# MAGIC /databricks-datasets
# MAGIC ```  

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Use the file system utility to list the content of the following directory
# MAGIC ```
# MAGIC 	/databricks-datasets
# MAGIC ```

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC #####6.Loop through the items in the databricks-datasets and print the path of each item

# COMMAND ----------

for items in dbutils.fs.ls("/databricks-datasets"):
    print(items.path)

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Show the documentation for the notebook utiltity

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####8. Create a new notebook (03-child-notebook) and type the following code
# MAGIC ```python
# MAGIC   print("I am a child notebook")
# MAGIC   dbutils.notebook.exit(101)
# MAGIC ``` 
# MAGIC Call the child-notebook from the next cell 

# COMMAND ----------

dbutils.notebook.run("./03-child-notebook", 10)

# COMMAND ----------

# MAGIC %md
# MAGIC #####9. Show help of the widgets utility

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####10. Do the following in the child notebook
# MAGIC 1. Add a msg argument to the child notebook
# MAGIC 2. Retrive the msg from the dbutils into a python variable
# MAGIC 3. Print a new message in the child notebook to print the argument
# MAGIC ```python
# MAGIC dbutils.widgets.text("msg", "", "Your input parameter")
# MAGIC p_msg = dbutils.widgets.get("msg")
# MAGIC print("I am a child notebook and recieved " + p_msg )
# MAGIC dbutils.notebook.exit("Success")
# MAGIC ```
# MAGIC
# MAGIC Call the child notebook in the next cell passing the notebook argument

# COMMAND ----------

status = dbutils.notebook.run("./03-child-notebook", 10, {"msg":"call from parent"})
print("Received: " + status)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
