# Databricks notebook source
# MAGIC %md
# MAGIC # Calidad | Conciliación Gold, Silver y dimensiones
# MAGIC La tarea falla si hay duplicados, huérfanos o diferencias en el negocio.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
cfg = configurar(dbutils.widgets.get("catalogo"))

# COMMAND ----------

validacion = spark.sql(f"""
WITH comparacion AS (
  SELECT p.pedido_id AS id_silver, f.pedido_id AS id_gold,
         f.pedido_id IS NULL OR p.pedido_id IS NULL OR NOT (
             p.revision <=> f.revision AND p.fecha_pedido <=> f.fecha_pedido
             AND sha2(p.sede,256) <=> f.sede_id AND sha2(p.categoria,256) <=> f.categoria_id
             AND p.canal <=> f.canal AND p.estado <=> f.estado AND p.unidades <=> f.unidades
             AND p.importe_pen <=> f.importe_pedido_pen
             AND p.fecha_entrega <=> f.fecha_entrega AND p.fecha_promesa <=> f.fecha_promesa
             AND IF(p.estado = 'ENTREGADO', p.importe_pen, 0) <=> f.venta_entregada_pen
             AND IF(p.estado = 'ENTREGADO', 1, 0) <=> f.es_entregado
             AND IF(p.estado = 'CANCELADO', 1, 0) <=> f.es_cancelado
             AND IF(p.estado = 'ENTREGADO' AND p.fecha_entrega <= p.fecha_promesa, 1, 0)
                 <=> f.es_puntual
         ) AS distinto
  FROM {cfg.pedidos} p FULL OUTER JOIN {cfg.hechos} f ON p.pedido_id = f.pedido_id
),
huerfanos AS (
  SELECT count(*) AS total
  FROM {cfg.hechos} f
  LEFT JOIN {cfg.gold}.dim_fecha d ON d.fecha = f.fecha_pedido
  LEFT JOIN {cfg.gold}.dim_sede s ON s.sede_id = f.sede_id
  LEFT JOIN {cfg.gold}.dim_categoria c ON c.categoria_id = f.categoria_id
  WHERE d.fecha IS NULL OR s.sede_id IS NULL OR c.categoria_id IS NULL
)
SELECT
  (SELECT count(*) FROM {cfg.hechos}) AS pedidos_gold,
  (SELECT count(*) - count(DISTINCT pedido_id) FROM {cfg.hechos}) AS duplicados_fact,
  (SELECT count(*) - count(DISTINCT fecha) FROM {cfg.gold}.dim_fecha) +
  (SELECT count(*) - count(DISTINCT sede_id) FROM {cfg.gold}.dim_sede) +
  (SELECT count(*) - count(DISTINCT categoria_id) FROM {cfg.gold}.dim_categoria) AS duplicados_dims,
  (SELECT count(*) FROM comparacion WHERE distinto) AS diferencias_silver_gold,
  (SELECT total FROM huerfanos) AS huerfanos,
  (SELECT count(*) FROM {cfg.rechazos}) AS rechazados_silver
""").first().asDict()
print(validacion)
if not validacion["pedidos_gold"] or any(validacion[campo] for campo in (
    "duplicados_fact", "duplicados_dims", "diferencias_silver_gold", "huerfanos"
)):
    raise RuntimeError(f"Fallaron los controles de calidad: {validacion}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT count(*) AS pedidos, sum(es_entregado) AS entregados,
           sum(es_cancelado) AS cancelados, sum(venta_entregada_pen) AS ventas_entregadas_pen,
           round(100 * try_divide(sum(es_puntual), sum(es_entregado)), 2) AS puntualidad_pct
    FROM {cfg.hechos}
"""))
display(spark.table(cfg.control))
