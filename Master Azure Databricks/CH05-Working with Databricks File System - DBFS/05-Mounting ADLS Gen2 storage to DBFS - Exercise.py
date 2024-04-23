# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating your DBFS mount for data storage
# MAGIC 1. Create ADLS Gen2 Storage account
# MAGIC 2. Create storage container in your storage account
# MAGIC 3. Create Azure service principal and secret
# MAGIC 4. Grant access to service proncipal for storage account
# MAGIC 5. Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Create ADLS Gen2 Storage account
# MAGIC * Click "Create a resource" on your Azure portal home page
# MAGIC * Search for "Storage account" and click the create button
# MAGIC * Create a storage account using the following
# MAGIC     * Choose an appropriate subscription
# MAGIC     * Select an existing or Create a new Resource group
# MAGIC     * Choose a unique storage account name
# MAGIC     * Choose a region (Choose the same region where your Databricks service is created)
# MAGIC     * Select performance tier (Standard tier is good enough for learning)
# MAGIC     * Choose storage redundency (LRS is good enough for learning)
# MAGIC     * Click Advanced button to move to the next step
# MAGIC     * Select "Enable hierarchical namespace" on the Advanced tab
# MAGIC     * Click "Review" button
# MAGIC     * Click the "Create" button after reviewing your settings
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Create storage container in your storage account
# MAGIC * Go to your Azure storage account page
# MAGIC * Select "Containers" from the left side menu
# MAGIC * Click "+ Container" button from the top menu
# MAGIC * Give a name to your containe (Ex dbfs-container)
# MAGIC * Click the "Create" button

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Create Azure service principal and secret
# MAGIC * Go to Azure Active Directory Service page in your Azure account (Azure Active Directory is now Microsoft Entra ID)
# MAGIC * Select "App registrations" from the left side menu
# MAGIC * Click (+ New registration) from the top menu
# MAGIC * Give a name to your service principal (Ex databricks-app-principal)
# MAGIC * Click the "Register" button
# MAGIC * Service principal will be created and details will be shown on the service principal page
# MAGIC * Copy "Application (client) ID" and "Directory (tenant) ID" values. You will need them later
# MAGIC * Choose "Certificates & secrets" from the left menu
# MAGIC * Click "+ New client secret" on the secrets page
# MAGIC * Enter a description (Ex databricks-app-principal-secret)
# MAGIC * Select an expiry (Ex 3 Months)
# MAGIC * Click the "Add" button
# MAGIC * Secret will be created and shown on the page
# MAGIC * Copy the Secret value. You will need it later
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Grant access to service principal for storage account
# MAGIC * Go to your storage account page
# MAGIC * Click "Access control (IAM)" from the left menu
# MAGIC * Click the "+ Add" button and choose "Add role assignment"
# MAGIC * Search for "Storage Blob Data Contributor" role and select it
# MAGIC * Click "Next" button
# MAGIC * Click the "+ Select members"
# MAGIC * Search for your Databricks service principal (Ex databricks-app-principal) and select it
# MAGIC * Clcik "Select" button
# MAGIC * Click "Review + assign" button twice

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.1 Define necessory variables

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####5.2 Define mount configs
# MAGIC You can follow the instruction and code sample from below documentation page
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####5.3 Mount the container

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####5.4. List contents of your mount point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####5.5. Upload files to your mounted location

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.6. List contents of your mount point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####5.7. Unmount /mnt/data directory

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
