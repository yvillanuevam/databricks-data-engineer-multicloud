# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze | Auto Loader y Delta
# MAGIC Conserva los campos de origen como texto y añade el archivo de procedencia.
# MAGIC Auto Loader procesa los archivos nuevos con un checkpoint persistente.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

campos = """
    pedido_id STRING, revision STRING, fecha_pedido STRING, fecha_evento STRING,
    sede STRING, categoria STRING, canal STRING, unidades STRING, importe_pen STRING,
    estado STRING, fecha_promesa STRING, fecha_entrega STRING
"""
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.bronze}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.eventos} ({campos}, archivo_origen STRING,
    _rescued_data STRING) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

archivos = (
    spark.readStream.format("cloudFiles")
    .schema(campos)
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{cfg.volumen}/estado/esquema_bronze")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("header", "true")
    .option("pathGlobFilter", "pedidos_*.csv")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{cfg.volumen}/entradas")
    .select("*", F.col("_metadata.file_path").alias("archivo_origen"))
)

carga = (
    archivos.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{cfg.volumen}/estado/checkpoint_bronze")
    .trigger(availableNow=True)
    .toTable(cfg.eventos)
)
carga.awaitTermination()
print(f"Bronze completado. Filas acumuladas: {spark.table(cfg.eventos).count()}.")