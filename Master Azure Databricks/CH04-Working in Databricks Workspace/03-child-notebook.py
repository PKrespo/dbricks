# Databricks notebook source
dbutils.widgets.text("msg", "", "Your input parameter")
p_msg = dbutils.widgets.get("msg")
print("I am a child notebook and recieved " + p_msg )
dbutils.notebook.exit("Success")
