-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("storage","abfss://bronze@saproyectofinal.dfs.core.windows.net/bronze")
-- MAGIC dbutils.widgets.text("catalogo","Catalogo_Pfinal")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ruta = dbutils.widgets.get("storage")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Bronze

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS Catalogo_Pfinal.bronze.CLIENTES;
DROP TABLE IF EXISTS Catalogo_Pfinal.bronze.VENTAS;
DROP TABLE IF EXISTS Catalogo_Pfinal.bronze.PRODUCTOS;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## REMOVE DATA (Bronze)
-- MAGIC # Borrar una carpeta
-- MAGIC dbutils.fs.rm("abfss://bronze@saproyectofinal.dfs.core.windows.net/CLIENTES", recurse=True)
-- MAGIC dbutils.fs.rm("abfss://bronze@saproyectofinal.dfs.core.windows.net/VENTAS", recurse=True)
-- MAGIC dbutils.fs.rm("abfss://bronze@saproyectofinal.dfs.core.windows.net/PRODUCTOS", recurse=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Silver

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS catalogo_pfinal.silver.tabale_transformed;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Eliminacion tablas Silver

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS  catalogo_pfinal.golden.table_kpi ;
