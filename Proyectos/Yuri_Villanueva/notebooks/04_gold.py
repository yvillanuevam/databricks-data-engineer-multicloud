# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold | Modelo estrella de ventas y cumplimiento
# MAGIC Grano: una fila por pedido. Tres dimensiones filtran la tabla de hechos:
# MAGIC fecha del pedido, sede y categoría. El canal queda en la tabla de hechos.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.gold}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.gold}.dim_fecha (
      fecha DATE NOT NULL, anio INT NOT NULL, mes INT NOT NULL,
      anio_mes STRING NOT NULL, dia INT NOT NULL
    ) USING DELTA
""")
for dimension, etiqueta in (("dim_sede", "sede"), ("dim_categoria", "categoria")):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.gold}.{dimension} (
          {etiqueta}_id STRING NOT NULL, {etiqueta} STRING NOT NULL
        ) USING DELTA
    """)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.hechos} (
      pedido_id STRING NOT NULL, fecha_pedido DATE NOT NULL, sede_id STRING NOT NULL,
      categoria_id STRING NOT NULL, canal STRING NOT NULL, revision INT NOT NULL,
      estado STRING NOT NULL, unidades INT NOT NULL, importe_pedido_pen DECIMAL(14,2) NOT NULL,
      venta_entregada_pen DECIMAL(14,2) NOT NULL, es_entregado INT NOT NULL,
      es_puntual INT NOT NULL, es_cancelado INT NOT NULL, dias_atraso INT,
      fecha_promesa DATE NOT NULL, fecha_entrega DATE
    ) USING DELTA
""")

# COMMAND ----------

def aplicar_cambios_gold(cambios):
    orden = Window.partitionBy("pedido_id").orderBy(F.desc("_commit_version"))
    ultima_imagen = (
        cambios.where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .withColumn("orden", F.row_number().over(orden))
        .where(F.col("orden") == 1)
    )
    activos = ultima_imagen.where(F.col("_change_type") != "delete")

    fechas = activos.select(F.col("fecha_pedido").alias("fecha")).distinct().select(
        "fecha", F.year("fecha").alias("anio"), F.month("fecha").alias("mes"),
        F.date_format("fecha", "yyyy-MM").alias("anio_mes"),
        F.dayofmonth("fecha").alias("dia"),
    )
    fechas.createOrReplaceTempView("fechas_lote")
    spark.sql(f"""
        MERGE INTO {cfg.gold}.dim_fecha t USING fechas_lote s ON t.fecha = s.fecha
        WHEN NOT MATCHED THEN INSERT *
    """)
    for dimension, etiqueta in (("dim_sede", "sede"), ("dim_categoria", "categoria")):
        activos.select(F.sha2(etiqueta, 256).alias(f"{etiqueta}_id"), etiqueta).distinct(
        ).createOrReplaceTempView("dimension_lote")
        spark.sql(f"""
            MERGE INTO {cfg.gold}.{dimension} t USING dimension_lote s
            ON t.{etiqueta}_id = s.{etiqueta}_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    entregado = F.col("estado") == "ENTREGADO"
    delta_dias = F.datediff("fecha_entrega", "fecha_promesa")
    ultima_imagen.select(
        "pedido_id", "fecha_pedido", F.sha2("sede", 256).alias("sede_id"),
        F.sha2("categoria", 256).alias("categoria_id"), "canal", "revision", "estado", "unidades",
        F.col("importe_pen").alias("importe_pedido_pen"),
        F.when(entregado, F.col("importe_pen")).otherwise(F.lit(0)).alias("venta_entregada_pen"),
        entregado.cast("int").alias("es_entregado"),
        F.when(entregado & (delta_dias <= 0), 1).otherwise(0).alias("es_puntual"),
        (F.col("estado") == "CANCELADO").cast("int").alias("es_cancelado"),
        F.when(entregado, F.greatest(F.lit(0), delta_dias)).alias("dias_atraso"),
        "fecha_promesa", "fecha_entrega", "_change_type",
    ).createOrReplaceTempView("hechos_lote")
    spark.sql(f"""
        MERGE INTO {cfg.hechos} t USING hechos_lote s ON t.pedido_id = s.pedido_id
        WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s._change_type <> 'delete' THEN INSERT *
    """)


cambios, identidad, version = cambios_pendientes(cfg.control, cfg.pedidos, "silver_gold")
if cambios is not None:
    if cambios.limit(1).count():
        aplicar_cambios_gold(cambios)
    confirmar_version(cfg.control, "silver_gold", identidad, version)

print(f"Gold: {spark.table(cfg.hechos).count()} filas en fact_pedido.")