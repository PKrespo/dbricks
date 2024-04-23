# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup Unity Catalog
# MAGIC 1. Create Workspace
# MAGIC 2. Create Data Storage Layer
# MAGIC     1. Storage Account
# MAGIC     2. Storage Container
# MAGIC     3. Databricks Access Connector
# MAGIC     4. Blog Storage Contributor role for Access Conector
# MAGIC 3. Setup Unity Catalog Metastore
# MAGIC 4. Setup Users and Permissions

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.2. Create a storage container for unity catalog metastore (Ex metastore-root)
# MAGIC * Go to your Azure storage account page
# MAGIC * Click "Containers" from the left menu
# MAGIC * Click "+ Container" to create a new container

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.3 Create Databricks Access Connector
# MAGIC * Click "+ Create a resource" button on your Azure portal
# MAGIC * Search "access connector for azure databricks"
# MAGIC * Click "Create" button
# MAGIC * Select your Azure subscription and a resource group on the create page
# MAGIC * Give a name to your connector (Ex databricks_access_connector)
# MAGIC * Select a region. Make sure to select the same region as your Databricks workspace and Azure storage account
# MAGIC * Click "Review + Create"
# MAGIC * Click "Create" button after reviewing your settings
# MAGIC * Go to Access Connector Resource page and copy the Resource ID. You will need it later

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.4 Grant access to Access connector for your Azure storage account
# MAGIC * Go to your storage account page on your Azure portal
# MAGIC * Click "Access control (IAM)" from the left side memu
# MAGIC * Click "+ Add" link from the top menu and select "Add role assignment"
# MAGIC * Search for "Storage blob data contributor" role and select it
# MAGIC * Click "Next" button
# MAGIC * Choose "Managed idendity" radio button
# MAGIC * Click "+ Select members" on the members page
# MAGIC * Choose your Azure subscription
# MAGIC * Choose "All system managed identities"
# MAGIC * Select your Databricks access connector (Ex databricks_access_connector)
# MAGIC * Click "Select" button
# MAGIC * Click "Review + assign" twice

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Setup Unity Catalog Metastore
# MAGIC * Go to Databricks Account console
# MAGIC     * Click your name at the top right corner of the Databricks workspace
# MAGIC     * Click Manage Accounts from the menu
# MAGIC     * Databricks account console will open in a new browser tab
# MAGIC * Perform following in the Databricks Account Console
# MAGIC     * Click Data from the left side menu
# MAGIC     * Click "Create Metastore" button
# MAGIC     * Give a name for metastore (Ex - scholarnest-meta)
# MAGIC     * Choose a region for metatore deployment. Make sure to choose the same region as your workspace and storage account (Ex eastus)
# MAGIC     * Type storage container path for metastore storage (Ex metastore-root@prashantsa.dfs.core.windows.net/)
# MAGIC     * Paste the Access connector resource id
# MAGIC     * Clcik the "Create" button
# MAGIC     * Select all workspace names to connect it with the metastore
# MAGIC     * Click "Assign" button
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Provision your company users to Unity Catalog
# MAGIC 1. Get SCIM connection details
# MAGIC     1. Click settings menu from the left side of Databricks Account Console
# MAGIC     2. Click "Setup user provisioning" button
# MAGIC     3. Copy SCIM token
# MAGIC     4. Copy SCIM URL
# MAGIC     5. Click "Done" button
# MAGIC 2. Sync corporate users from your Active Directory
# MAGIC     1. Go to your organizations Active Directory (Ex Your Azure Active Directory)
# MAGIC     2. Select "Enterprise Applications" from the left side menu
# MAGIC     3. Clcik "+ New Application"
# MAGIC     4. Search for "Azure Databricks SCIM Provisioning Connector" and select it
# MAGIC     5. Click "Create" button
# MAGIC     6. Click "Provisioning" from the left side menu on the SCIM Provisioning Connector page
# MAGIC     7. Click "Get Started" button
# MAGIC     8. Select Provisioning mode (Ex Automatic)
# MAGIC     9. Paste Databricks Account SCIM URL that you copied earlier
# MAGIC     10. Paste Databricks Account SCIM token that you copied earlier
# MAGIC     11. Click "Test Connection" to confirm the connectivity
# MAGIC     12. Click "Save" link at the top
# MAGIC     13. Go back to "SCIM Provisioning Connector" page
# MAGIC     14. Click "Users and groups" from the left side menu
# MAGIC     15. Click "+ Add user/group"
# MAGIC     16. Click "Not Selected" and select desired users
# MAGIC     17. Click "Assign" button
# MAGIC     18. Click "Provisioning" from the left side menu
# MAGIC     19. Click "Start Provisioning" button from the top
# MAGIC     20. Wait for "Initial cycle completed." message

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Create user group
# MAGIC 1. Click "User management" from the left side of Databricks Account Console
# MAGIC 2. Go to groups tab
# MAGIC 3. Click Add group button
# MAGIC 4. Type group name (Ex scholarnest-dev)
# MAGIC 5. Click Save button
# MAGIC 6. Click "Add members" button to add new members to the group

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
