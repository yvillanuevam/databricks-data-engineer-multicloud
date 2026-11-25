# Databricks notebook source
# MAGIC %md
# MAGIC # Parámetros y control incremental
# MAGIC El catálogo es configurable. Cada etapa registra la última versión Delta
# MAGIC aplicada después de completar sus escrituras.

# COMMAND ----------

import re
from types import SimpleNamespace

from pyspark.sql import Window
from pyspark.sql import functions as F


def configurar(catalogo):
    catalogo = catalogo.strip() or spark.sql("SELECT current_catalog()").first()[0]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalogo) or catalogo.lower() in {
        "hive_metastore", "system", "samples"
    }:
        raise ValueError("Indica un catálogo de Unity Catalog en el parámetro catalogo.")

    raw = f"`{catalogo}`.yvillanuevam_raw"
    bronze = f"`{catalogo}`.yvillanuevam_bronze"
    silver = f"`{catalogo}`.yvillanuevam_silver"
    gold = f"`{catalogo}`.yvillanuevam_gold"
    return SimpleNamespace(
        raw=raw, bronze=bronze, silver=silver, gold=gold,
        volumen=f"/Volumes/{catalogo}/yvillanuevam_raw/archivos",
        control=f"{raw}.control_cdf", eventos=f"{bronze}.eventos_pedido",
        pedidos=f"{silver}.pedidos", rechazos=f"{silver}.rechazos_pedido",
        hechos=f"{gold}.fact_pedido",
    )

# COMMAND ----------

def cambios_pendientes(control, tabla, etapa):
    """Lee un rango CDF inclusivo y deja su confirmación a la etapa consumidora."""
    identidad = spark.sql(f"DESCRIBE DETAIL {tabla}").first()["id"]
    registrado = spark.table(control).where(F.col("etapa") == etapa).first()
    ultima = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first()["version"]
    if registrado and (registrado["tabla_id"] != identidad or registrado["version"] > ultima):
        raise RuntimeError(f"Cambió el origen de {etapa}. Se requiere reconstruir la etapa.")
    inicio = registrado["version"] + 1 if registrado else 0
    if inicio > ultima:
        print(f"{etapa}: sin versiones nuevas (última: {ultima}).")
        return None, identidad, ultima
    print(f"{etapa}: versiones {inicio} a {ultima}.")
    cambios = (spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", inicio)
        .option("endingVersion", ultima)
        .table(tabla))
    return cambios, identidad, ultima


def confirmar_version(control, etapa, identidad, version):
    spark.createDataFrame(
        [(etapa, identidad, version)], "etapa STRING, tabla_id STRING, version LONG"
    ).createOrReplaceTempView("version_aplicada")
    spark.sql(f"""
        MERGE INTO {control} t USING version_aplicada s ON t.etapa = s.etapa
        WHEN MATCHED AND s.version > t.version THEN UPDATE SET
          t.version = s.version, t.actualizado_utc = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (etapa, tabla_id, version, actualizado_utc)
        VALUES (s.etapa, s.tabla_id, s.version, current_timestamp())
    """)
