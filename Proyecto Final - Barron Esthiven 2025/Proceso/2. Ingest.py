# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT Archivo_Cliente DEFAULT "Clientes.csv";
# MAGIC CREATE WIDGET TEXT Archivo_Producto DEFAULT "Productos.csv";
# MAGIC CREATE WIDGET TEXT Archivo_Ventas DEFAULT "Ventas.csv";
# MAGIC

# COMMAND ----------

##Definicion de constantes
ruta = "abfss://raw@saproyectofinal.dfs.core.windows.net/"
catalogo = "Catalogo_Pfinal"
esquema = "bronze"
##Archivos
Archivo_Cliente = dbutils.widgets.get("Archivo_Cliente")
Archivo_Producto = dbutils.widgets.get("Archivo_Producto")
Archivo_Ventas = dbutils.widgets.get("Archivo_Ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC #CLIENTES

# COMMAND ----------

## CREACION DE SCHEMA

cliente_schema = StructType(fields=[ StructField("cliente_Id", StringType(), False),
                                     StructField("nombre", StringType(), True),
                                     StructField("edad", IntegerType(), True),
                                     StructField("genero", StringType(), True),
                                     StructField("ciudad", StringType(), True)                                    
])

## CREACION DE DATAFRAME

df_clientes_final = (
    spark.read.option("header", True)
              .option("delimiter", ";")   
              .schema(cliente_schema)
              .csv(ruta + Archivo_Cliente)
)

## SELECIONAR COLUMNAS

clientes_selected_df = df_clientes_final.select(col("cliente_Id"), 
                                                col("nombre"), 
                                                col("edad"), col("genero"), 
                                                col("ciudad"))

## RENOMBRAR COLUMNAS

clientes_selected_df = clientes_selected_df.withColumnRenamed("cliente_tId", "Cliente_Id") \
.withColumnRenamed("nombre", "Nombre") \
.withColumnRenamed("edad", "Edad") \
.withColumnRenamed("genero", "Genero") \
.withColumnRenamed("ciudad", "Ciudad") 

clientes_selected_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #PRODUCTO

# COMMAND ----------

## CREACION DE SCHEMA

producto_schema = StructType(fields=[ StructField("producto_Id", StringType(), False),
                                     StructField("producto", StringType(), True),
                                     StructField("categoria", StringType(), True),
                                     StructField("precio", DoubleType(), True)                                 
])

df_producto_final = (
    spark.read.option("header", True)
              .option("delimiter", ";")   
              .schema(producto_schema)
              .csv(ruta + Archivo_Producto)
)

## SELECIONAR COLUMNAS

producto_selected_df = df_producto_final.select(col("producto_Id"), 
                                                col("producto"), 
                                                col("categoria"),
                                                col("precio"))

## RENOMBRAR COLUMNAS

producto_selected_df = producto_selected_df.withColumnRenamed("producto_Id", "Producto_Id") \
.withColumnRenamed("producto", "Producto") \
.withColumnRenamed("categoria", "categoria") \
.withColumnRenamed("precio", "Precio")

producto_selected_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Ventas

# COMMAND ----------

## CREACION DE SCHEMA

venta_schema = StructType(fields=[ StructField("venta_id", StringType(), False),
                                     StructField("fecha", StringType(), True),
                                     StructField("cliente_id", StringType(), True),
                                     StructField("producto_id", StringType(), True), 
                                     StructField("cantidad", IntegerType(), True),
                                     StructField("tienda", StringType(), True),
                                     StructField("ciudad", StringType(), True)                                
])

df_venta_final = (
    spark.read.option("header", True)
              .option("delimiter", ";")   
              .schema(venta_schema)
              .csv(ruta + Archivo_Ventas)
)

## SELECIONAR COLUMNAS

venta_selected_df = df_venta_final.select( col("venta_id"),
                                                col("fecha"), 
                                                col("cliente_id"), 
                                                col("producto_id"),
                                                col("cantidad"),
                                                col("tienda"),
                                                col("ciudad"))

## RENOMBRAR COLUMNAS

venta_selected_df = venta_selected_df.withColumnRenamed("venta_id", "Venta_Id") \
.withColumnRenamed("fecha", "fecha") \
.withColumnRenamed("cliente_id", "Cliente_Id") \
.withColumnRenamed("producto_id", "Producto_Id") \
.withColumnRenamed("cantidad", "Cantidad") \
.withColumnRenamed("tienda", "Tienda") \
.withColumnRenamed("ciudad", "Ciudad") 

venta_selected_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC #INSERCION A TABLAS

# COMMAND ----------

producto_selected_df.describe().show()

# COMMAND ----------

venta_selected_df.describe().show()

# COMMAND ----------

clientes_selected_df.describe().show()

# COMMAND ----------

clientes_selected_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.CLIENTES")
venta_selected_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.VENTAS")
producto_selected_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.PRODUCTOS")

# COMMAND ----------

# MAGIC %md
# MAGIC #Verificar la Insercion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalogo_pfinal.bronze.clientes
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalogo_pfinal.bronze.productos
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalogo_pfinal.bronze.ventas
# MAGIC
