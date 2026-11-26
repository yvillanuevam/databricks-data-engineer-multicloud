# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver | Calidad y última revisión por pedido
# MAGIC Lee CDF desde Bronze, separa registros inválidos y aplica la revisión más
# MAGIC reciente de cada pedido con MERGE. Los estados se conservan en Silver.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.silver}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.pedidos} (
      pedido_id STRING NOT NULL, revision INT NOT NULL, fecha_pedido DATE NOT NULL,
      fecha_evento DATE NOT NULL, sede STRING NOT NULL, categoria STRING NOT NULL,
      canal STRING NOT NULL, unidades INT NOT NULL, importe_pen DECIMAL(14,2) NOT NULL,
      estado STRING NOT NULL, fecha_promesa DATE NOT NULL, fecha_entrega DATE,
      archivo_origen STRING NOT NULL
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.rechazos} (
      rechazo_id STRING NOT NULL, pedido_id STRING, archivo_origen STRING,
      motivo STRING NOT NULL, registro_original STRING
    ) USING DELTA
""")

# COMMAND ----------

def aplicar_cambios_silver(cambios):
    if cambios.where(F.col("_change_type") != "insert").limit(1).count():
        raise ValueError("Bronze es append-only. Se detectó una modificación física.")

    campos = spark.table(cfg.eventos).columns
    tipados = (
        cambios.select(*campos)
        .withColumn("registro_original", F.to_json(F.struct(*campos)))
        .withColumn("pedido_id", F.trim("pedido_id"))
        .withColumn("revision", F.expr("try_cast(revision AS INT)"))
        .withColumn("fecha_pedido", F.expr("try_cast(fecha_pedido AS DATE)"))
        .withColumn("fecha_evento", F.expr("try_cast(fecha_evento AS DATE)"))
        .withColumn("sede", F.trim("sede"))
        .withColumn("categoria", F.trim("categoria"))
        .withColumn("canal", F.trim("canal"))
        .withColumn("unidades", F.expr("try_cast(unidades AS INT)"))
        .withColumn("importe_pen", F.expr("try_cast(importe_pen AS DECIMAL(14,2))"))
        .withColumn("estado", F.upper(F.trim("estado")))
        .withColumn("fecha_promesa", F.expr("try_cast(fecha_promesa AS DATE)"))
        .withColumn("entrega_original", F.trim("fecha_entrega"))
        .withColumn("fecha_entrega", F.expr("try_cast(fecha_entrega AS DATE)"))
    )
    reglas = (
        F.when(F.col("_rescued_data").isNotNull(), "ESTRUCTURA_CSV")
        .when(~F.coalesce(F.col("pedido_id").rlike(r"^[0-9]{8}-[0-9]{3}$"), F.lit(False)),
              "ID_PEDIDO_INVALIDO")
        .when(~F.coalesce(F.col("revision") >= 1, F.lit(False)), "REVISION_INVALIDA")
        .when(~F.coalesce((F.col("fecha_evento") >= F.col("fecha_pedido")) &
                          (F.col("fecha_promesa") >= F.col("fecha_pedido")), F.lit(False)),
              "FECHAS_INVALIDAS")
        .when(~F.coalesce(F.col("unidades") > 0, F.lit(False)), "UNIDADES_INVALIDAS")
        .when(~F.coalesce(F.col("importe_pen") > 0, F.lit(False)), "IMPORTE_INVALIDO")
        .when(~F.coalesce(F.col("sede").isin("Lima Este", "Lima Norte", "Lima Sur", "Callao") &
                          F.col("categoria").isin("Tecnología", "Hogar", "Moda", "Deportes") &
                          F.col("canal").isin("Web", "App", "Tienda") &
                          F.col("estado").isin("PENDIENTE", "ENTREGADO", "CANCELADO"),
                          F.lit(False)), "CATALOGO_INVALIDO")
        .when((F.col("estado") == "ENTREGADO") &
              ~F.coalesce((F.col("fecha_entrega") >= F.col("fecha_pedido")) &
                          (F.col("fecha_entrega") <= F.col("fecha_evento")), F.lit(False)),
              "ENTREGA_INVALIDA")
        .when((F.col("estado") != "ENTREGADO") &
              (F.coalesce(F.col("entrega_original"), F.lit("")) != ""), "ENTREGA_INESPERADA")
    )
    evaluados = tipados.withColumn("motivo", reglas)
    invalidos = (
        evaluados.where(F.col("motivo").isNotNull())
        .withColumn("rechazo_id", F.sha2("registro_original", 256))
        .select("rechazo_id", "pedido_id", "archivo_origen", "motivo", "registro_original")
        .dropDuplicates(["rechazo_id"])
    )
    invalidos.createOrReplaceTempView("rechazos_lote")
    spark.sql(f"""
        MERGE INTO {cfg.rechazos} t USING rechazos_lote s ON t.rechazo_id = s.rechazo_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    orden = Window.partitionBy("pedido_id").orderBy(
        F.desc("revision"), F.desc("fecha_evento"), F.desc("archivo_origen"),
        F.desc("registro_original")
    )
    validos = (
        evaluados.where(F.col("motivo").isNull())
        .withColumn("orden_revision", F.row_number().over(orden))
        .where(F.col("orden_revision") == 1)
        .select(*spark.table(cfg.pedidos).columns)
    )
    validos.createOrReplaceTempView("pedidos_lote")
    spark.sql(f"""
        MERGE INTO {cfg.pedidos} t USING pedidos_lote s ON t.pedido_id = s.pedido_id
        WHEN MATCHED AND s.revision > t.revision THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


cambios, identidad, version = cambios_pendientes(cfg.control, cfg.eventos, "bronze_silver")
if cambios is not None:
    if cambios.limit(1).count():
        aplicar_cambios_silver(cambios)
    confirmar_version(cfg.control, "bronze_silver", identidad, version)

print(f"Silver: {spark.table(cfg.pedidos).count()} pedidos y {spark.table(cfg.rechazos).count()} rechazos.")