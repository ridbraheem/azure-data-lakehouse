-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### This notebook contains the code used to create external locations, catalogs and schemas

-- COMMAND ----------

CREATE EXTERNAL LOCATION crunchbase_raw
 URL 'abfss://raw@crunchbaseldatalake.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `crunchbase-storage-credential`);

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION crunchbase_raw;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "abfss://raw@crunchbaseldatalake.dfs.core.windows.net/"

-- COMMAND ----------

CREATE EXTERNAL LOCATION intermediate
 URL 'abfss://intermediate@crunchbaseldatalake.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `crunchbase-storage-credential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION core
 URL 'abfss://core@crunchbaseldatalake.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `crunchbase-storage-credential`);

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev_db;

-- COMMAND ----------

USE CATALOG dev_db;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS raw
MANAGED LOCATION "abfss://raw@crunchbaseldatalake.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS intermediate
MANAGED LOCATION "abfss://intermediate@crunchbaseldatalake.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS core
MANAGED LOCATION "abfss://core@crunchbaseldatalake.dfs.core.windows.net/"