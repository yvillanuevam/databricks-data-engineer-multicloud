# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Mantenimiento Delta | OPTIMIZE con ZORDER y VACUUM
# MAGIC Se aplica a la tabla de hechos Gold. Bronze y Silver conservan su historial CDF
# MAGIC sujeto a la retención del catálogo. Se conserva el umbral de seguridad de 7 días.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {cfg.hechos} ZORDER BY (fecha_pedido, sede_id)"))
display(spark.sql(f"VACUUM {cfg.hechos} RETAIN 168 HOURS DRY RUN"))
display(spark.sql(f"VACUUM {cfg.hechos} RETAIN 168 HOURS"))
display(spark.sql(f"DESCRIBE HISTORY {cfg.hechos} LIMIT 10"))