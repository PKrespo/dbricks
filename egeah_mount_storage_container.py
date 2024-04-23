# Databricks notebook source
# MAGIC %md
# MAGIC ####5. Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.1 Define necessary variables

# COMMAND ----------

storage_account_name = "stgaccdbrickstraining"
container_name = "dev"
mount_point = "files"
client_id = "4d5a77c9-79dc-460a-b8d3-297c407fec64"
tenant_id = "7f07d38a-fc6f-40fd-ab54-0ba0a2ba922b"
client_secret ="ew-8Q~j6Rdx8wl-6c5gZeaufhClTrc_~krJuMbC7"

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.2 Define mount configs
# MAGIC You can follow the instruction and code sample from below documentation page
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# MAGIC %md
# MAGIC #####5.3 Mount the Container
# MAGIC

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/files/

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/files/dataset_ch7

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/files
