-- Databricks notebook source
-- Esto solo crea si no existe en UC
CREATE GROUP `Dvelopers`;
CREATE GROUP `Admins`;

-- COMMAND ----------

SHOW GROUPS;

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------


SHOW SCHEMAS IN `catalogo_pfinal`;

-- COMMAND ----------

USE CATALOG catalogo_pfinal;


-- COMMAND ----------

GRANT USE CATALOG ON CATALOG `catalogo_pfinal` TO `prueba2025@hotmail.com`;
GRANT USE SCHEMA ON SCHEMA `catalogo_pfinal`.`bronze` TO `prueba2025@hotmail.com`;

