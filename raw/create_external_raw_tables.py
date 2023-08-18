# Databricks notebook source
# MAGIC %md
# MAGIC ##### This notebook contains the code used to create raw tables as external tables

# COMMAND ----------

#import necessay libraries
import re
dbutils.fs.ls('abfss://raw@crunchbaseldatalake.dfs.core.windows.net/')

#Get the list of files from our raw container

file_list = []
for file in dbutils.fs.ls('abfss://raw@crunchbaseldatalake.dfs.core.windows.net/'):
    file_name = file.name
    table_name = re.search(r'([^/\\]+)\.csv$', file_name)
    table_name = table_name.group(1)
    file_list.append((file_name, table_name))


#Create raw external tables from the list of tables 

# COMMAND ----------

for items in file_list:
    statement = f'''
        CREATE TABLE dev_db.raw.{items[1]}
        USING CSV
        OPTIONS (header "true", inferSchema "true")
        LOCATION 'abfss://raw@crunchbaseldatalake.dfs.core.windows.net/{items[0]}'
    '''
    spark.sql(statement)
