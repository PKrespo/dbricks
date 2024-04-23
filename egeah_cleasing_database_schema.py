# Databricks notebook source
# MAGIC %md
# MAGIC ### Delete Schema in cascading

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists egeah_catalog_dev.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema dev cascade

# COMMAND ----------

# MAGIC %md
# MAGIC
