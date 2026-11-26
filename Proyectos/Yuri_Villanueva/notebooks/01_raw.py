# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Raw | Archivos CSV de pedidos y cambios de estado
# MAGIC Simulación determinística de un export diario del OMS de un retail.
# MAGIC Cada CSV es inmutable. El volumen conserva sus bytes sin normalizarlos.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

import csv
from datetime import date, datetime, timedelta
from io import StringIO
from random import Random
from zoneinfo import ZoneInfo

dbutils.widgets.text("fecha_corte", "")
corte = dbutils.widgets.get("fecha_corte").strip()
hoy_lima = datetime.now(ZoneInfo("America/Lima")).date()
fecha_corte = date.fromisoformat(corte) if corte else hoy_lima
inicio = date(2026, 10, 1)
if fecha_corte < inicio:
    raise ValueError(f"fecha_corte debe ser desde {inicio} en formato AAAA-MM-DD.")

# COMMAND ----------

def exportar_dia(dia):
    salida = StringIO(newline="")
    escritor = csv.writer(salida, lineterminator="\n")
    escritor.writerow([
        "pedido_id", "revision", "fecha_pedido", "fecha_evento", "sede",
        "categoria", "canal", "unidades", "importe_pen", "estado",
        "fecha_promesa", "fecha_entrega",
    ])

    def pedidos_de(fecha, actualizar):
        aleatorio = Random(fecha.toordinal())
        for posicion in range(1, 33):
            sede = aleatorio.choice(["Lima Este", "Lima Norte", "Lima Sur", "Callao"])
            categoria, base = aleatorio.choice([
                ("Tecnología", 209), ("Hogar", 89), ("Moda", 49), ("Deportes", 69),
            ])
            canal = aleatorio.choice(["Web", "App", "Tienda"])
            unidades = aleatorio.randint(1, 4)
            precio_cent = (base + aleatorio.randint(0, 50)) * unidades * 100 + 90
            resultado = aleatorio.random()
            fuera_plazo = resultado < (0.30 if sede == "Callao" else 0.14)
            promesa = fecha + timedelta(days=1)
            estado = "PENDIENTE"
            entrega = ""
            if actualizar:
                if resultado > 0.94:
                    estado = "CANCELADO"
                else:
                    estado = "ENTREGADO"
                    entrega = (promesa + timedelta(days=int(fuera_plazo))).isoformat()
            yield [
                f"{fecha:%Y%m%d}-{posicion:03}", 2 if actualizar else 1,
                fecha.isoformat(), dia.isoformat(), sede, categoria, canal, unidades,
                f"{precio_cent // 100}.{precio_cent % 100:02}", estado,
                promesa.isoformat(), entrega,
            ]

    nuevos = list(pedidos_de(dia, False))
    escritor.writerows(nuevos)
    # A los dos días llega el estado final. Permite observar UPDATE en el CDF de Silver.
    if dia - timedelta(days=2) >= inicio:
        escritor.writerows(pedidos_de(dia - timedelta(days=2), True))
    if dia.weekday() == 0:
        escritor.writerow(nuevos[0])  # Duplicado controlado de la fuente.
        invalido = nuevos[1].copy()
        invalido[0] = f"{dia:%Y%m%d}-099"
        invalido[7] = "SIN_DATO"
        escritor.writerow(invalido)
    return salida.getvalue()


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.raw}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.raw}.archivos")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.control} (
      etapa STRING NOT NULL, tabla_id STRING NOT NULL, version BIGINT NOT NULL,
      actualizado_utc TIMESTAMP NOT NULL
    ) USING DELTA
""")
entrada = f"{cfg.volumen}/entradas"
dbutils.fs.mkdirs(entrada)
existentes = {archivo.name for archivo in dbutils.fs.ls(entrada)}
nuevos_archivos = 0
for desplazamiento in range((fecha_corte - inicio).days + 1):
    dia = inicio + timedelta(days=desplazamiento)
    nombre = f"pedidos_{dia.isoformat()}.csv"
    if nombre not in existentes:
        dbutils.fs.put(f"{entrada}/{nombre}", exportar_dia(dia), overwrite=False)
        nuevos_archivos += 1

print(f"Fecha de corte solicitada: {fecha_corte} (Lima). CSV nuevos: {nuevos_archivos}.")
print(f"Ruta Raw: {entrada}")