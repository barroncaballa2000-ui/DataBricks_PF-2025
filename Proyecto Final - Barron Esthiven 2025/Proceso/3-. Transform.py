# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

catalogo = "Catalogo_Pfinal"
esquema_source = "bronze"
esquema_sink = "silver"

# COMMAND ----------

df_venta = spark.table(f"{catalogo}.{esquema_source}.ventas")
df_producto = spark.table(f"{catalogo}.{esquema_source}.productos")
df_cliente = spark.table(f"{catalogo}.{esquema_source}.clientes")


# COMMAND ----------

# MAGIC %md
# MAGIC #Eliminando  los NULL

# COMMAND ----------

df_producto = df_producto.dropna(how="all")
df_cliente  = df_cliente.dropna(how="all")
df_venta = df_venta.dropna(how="all")\
                    .filter((col("venta_id").isNotNull()) | (col("cliente_id")).isNotNull()| (col("producto_id")).isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC #JOIN 

# COMMAND ----------

# Hacemos el join 
df_join = df_venta.join(df_producto, on="producto_id", how="inner")
# Creamos nueva columna "total"
df_venta = df_join.select(
    df_venta["venta_id"],
    df_venta["fecha"],
    df_venta["cliente_id"],
    df_venta["producto_id"],
    df_venta["cantidad"],
    df_venta["tienda"],
    df_venta["ciudad"],
    (col("cantidad") * col("precio")).alias("total")
)
df_venta.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #Modificacion de Registros

# COMMAND ----------


df_venta = df_venta.withColumn(
    "ciudad",
    when(col("ciudad") == "Anchah", "Ancash")
    .when(col("ciudad") == "Cusco", "Cuzco")
    .otherwise(col("ciudad"))
)

# COMMAND ----------

df_cliente = df_cliente.withColumn(
    "ciudad",
    when(col("ciudad") == "Anchah", "Ancash")
    .when(col("ciudad") == "Cusco", "Cuzco")
    .otherwise(col("ciudad"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Modificacion de Columnas

# COMMAND ----------

df_cliente = df_cliente.withColumn(
    "nombre",
    initcap(col("nombre"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #modificacion de type

# COMMAND ----------


df_venta = df_venta.withColumn(
    "Fecha",
    to_date(col("Fecha"), "dd/MM/yyyy")
)



# COMMAND ----------

df_cliente.display()


# COMMAND ----------

df_join_01 = (
    df_venta.alias("x")
    .join(df_producto.alias("y"), col("x.producto_id") == col("y.producto_id"), "inner")
    .select(
        col("x.venta_id"),
        col("x.fecha"),
        col("x.cliente_id"),
        col("x.producto_id"),   # solo una vez
        col("x.cantidad"),
        col("x.tienda"),
        col("x.ciudad"),
        col("y.producto"),
        col("y.categoria"),
        col("y.precio"),
        col("x.total"),
    )
)

df_join_01.display()



# COMMAND ----------

df_join_final = (
    df_join_01.alias("x")
    .join(df_cliente.alias("y"), col("x.cliente_id") == col("y.cliente_id"), "inner")
    .select(
        col("x.venta_id"),
        col("x.fecha"),
        col("x.tienda"),
        col("x.ciudad"),
        col("x.producto_id"),
        col("x.producto"),     # ya viene de df_producto en df_join_01
        col("x.categoria"),    # ya viene de df_producto en df_join_01
        col("x.cantidad"),
        col("x.precio"),       # este también ya está en df_join_01
        col("x.total"),
        col("y.cliente_id"),
        col("y.nombre"),
        col("y.edad"),
        col("y.genero")
    )
)

df_join_final.display()

# COMMAND ----------


df_join_final = df_join_final.orderBy(col("fecha").asc())
df_join_final.display()


# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tabale_transformed")

# COMMAND ----------

# MAGIC %md
# MAGIC #verficar tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalogo_pfinal.silver.tabale_transformed
