# Databricks notebook source
class Cleanup():
    def __init__(self):
        pass
  
    def cleanup(self):
        try:
            dbutils.fs.rm("/Volumes/dev/demo_db/landing_zone/customers", True)
            dbutils.fs.rm("/Volumes/dev/demo_db/landing_zone/invoices", True)
        except:
            pass
        if spark.sql(f"SHOW CATALOGS").filter(f"catalog == 'dev'").count() == 1:
            print(f"Dropping the dev catalog ...", end='')            
            spark.sql(f"DROP CATALOG DEV CASCADE")
            print("Done")
    
        if spark.sql(f"SHOW CATALOGS").filter(f"catalog == 'qa'").count() == 1:
            print(f"Dropping the qa catalog ...", end='')
            spark.sql(f"DROP CATALOG QA CASCADE")
            print("Done")        
        
        if spark.sql(f"SHOW EXTERNAL LOCATIONS").filter(f"name == 'external_data'").count() == 1:
            print(f"Dropping the external-data ...", end='')
            spark.sql(f"DROP EXTERNAL LOCATION `external_data`")
            print("Done")   

        dbutils.fs.rm("/mnt/files/dataset_ch8/invoices/invoices_2021.csv")
        dbutils.fs.rm("/mnt/files/dataset_ch8/invoices/invoices_2022.csv")
        dbutils.fs.rm("/mnt/files/dataset_ch8//chekpoint", True)     

CL = Cleanup()
CL.cleanup()  
