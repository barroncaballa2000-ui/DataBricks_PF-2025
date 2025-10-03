# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

catalogo = "catalogo_pfinal"
esquema_source = "silver"
esquema_sink = "golden"

# COMMAND ----------

spark.sql(f""" create volume if not exists {catalogo}.{esquema_sink}.golden_volume""")

# COMMAND ----------

tabale_transformed1 = spark.table(f"{catalogo}.{esquema_source}.tabale_transformed")

# COMMAND ----------

tabale_transformed1.display()

# COMMAND ----------

df_transformed = tabale_transformed1.groupBy(col("fecha")).agg(
                                                     count(col("tienda")).alias("conteo"),
                                                     max(col("edad")).alias("max_edad"),
                                                     min(col("edad")).alias("min_edad"),
                                                     max(col("total")).alias("total_max"),
                                                     min(col("total")).alias("total_min"),
                                                     count(col("genero")).alias("genero")
                                                     ).orderBy(col("fecha").desc())

df_transformed.display()

# COMMAND ----------

dbutils.fs.rm("/Volumes/catalogo_pfinal/golden/golden_volume/golden_partitioned",True)

# COMMAND ----------

df_transformed.write.\
                format("delta").\
                mode("overwrite").\
                partitionBy("fecha").\
                save(f"/Volumes/catalogo_pfinal/golden/golden_volume/golden_partitioned")

# COMMAND ----------

df_agrupado = tabale_transformed1.groupBy(
    "fecha","ciudad", "categoria", "producto"
).agg(
    F.sum("cantidad").alias("cantidad_total"),
    F.sum("precio").alias("precio_total"),
    F.sum("total").alias("total_general")
)

df_agrupado.display()


# COMMAND ----------

df_agrupado.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.table_kpi")
