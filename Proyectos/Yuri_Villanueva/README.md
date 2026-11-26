# Ventas y cumplimiento de entregas en un retail peruano

- **Curso** Databricks Certification · Data Engineer Multicloud
- **Integrante** Yuri Villanueva Martínez
- **Rama de entrega** `feature/yvillanuevam`

El proyecto procesa pedidos simulados de un retail peruano para responder qué categorías generan más ventas entregadas y qué sedes tienen menor puntualidad. Implementa un ELT incremental con Python, PySpark, Delta Lake, Unity Catalog, Lakeflow Jobs y Power BI. Puede ejecutarse en Databricks Free Edition sin depender de servicios externos para la ingesta.

## Fuente, capas y flujo de datos

La fuente simula los CSV diarios de un sistema de gestión de pedidos desde el **1 de octubre de 2026**. Cada lote contiene 32 altas. Dos días después llega una segunda revisión que cambia el estado a entregado o cancelado. Los lunes se incluye una fila duplicada y otra con unidades inválidas para demostrar deduplicación y cuarentena. Los archivos se generan de forma determinística y se publican una sola vez.

| Etapa | Notebook | Salida y operación |
| --- | --- | --- |
| Raw | `01_raw.py` | CSV originales en `/Volumes/<catálogo>/yvillanuevam_raw/archivos/entradas`. |
| Bronze | `02_bronze.py` | `eventos_pedido` en Delta con columnas de origen en texto. Auto Loader detecta archivos nuevos con un checkpoint persistente y termina con `AvailableNow`. |
| Silver | `03_silver.py` | Lee el CDF de Bronze. Tipifica fechas e importes, valida campos, separa rechazos y guarda la revisión vigente por pedido con `MERGE`. |
| Gold | `04_gold.py` | Lee el CDF de Silver. Aplica cambios de pedidos a la tabla de hechos y carga las claves nuevas de las dimensiones. |
| Calidad | `05_calidad.py` | Verifica unicidad, integridad referencial, conciliación Silver–Gold e indicadores. Si falla, bloquea el mantenimiento. |
| Mantenimiento | `06_mantenimiento.py` | `OPTIMIZE … ZORDER BY (fecha_pedido, sede_id)` y `VACUUM … RETAIN 168 HOURS` sobre `fact_pedido`. Muestra el historial Delta. |

El esquema Raw contiene el volumen y la tabla `control_cdf`. Cada consumidor registra la versión Delta que terminó de aplicar. Si una tarea se interrumpe antes de confirmar, puede volver a leer ese rango y aplicar la escritura con las mismas claves. Los cancelados conservan su pedido y su estado. La retención del CDF depende del historial de Delta. Ante una brecha de retención o reemplazo del origen, reconstruye las capas en un catálogo de prueba limpio a partir de Raw.

## Modelo estrella Gold

**Grano:** un registro por pedido, con su último estado conocido. Las dimensiones filtran los hechos en sentido único. La fecha del análisis corresponde a la creación del pedido.

| Tabla | Clave y relación con los hechos | Análisis |
| --- | --- | --- |
| `fact_pedido` | `pedido_id` único | Importe, unidades, canal, estado y cumplimiento. |
| `dim_fecha` | `fecha` 1 a muchos con `fact_pedido.fecha_pedido` | Día y mes del pedido. |
| `dim_sede` | `sede_id` 1 a muchos con `fact_pedido.sede_id` | Desempeño logístico por sede. |
| `dim_categoria` | `categoria_id` 1 a muchos con `fact_pedido.categoria_id` | Aporte comercial por categoría. |

Las claves de sede y categoría se calculan con SHA‑256 sobre sus etiquetas normalizadas. La fuente de este caso asigna una categoría principal por pedido. Si el sistema real produce pedidos con líneas de distintas categorías, el grano comercial debe pasar a línea de pedido.

| Indicador | Cálculo |
| --- | --- |
| Pedidos registrados | Filas de la tabla de hechos. |
| Ventas entregadas | Suma del importe en PEN de pedidos con estado `ENTREGADO`. |
| Puntualidad | Entregados en o antes de la fecha prometida ÷ total de entregados. |
| Cancelación | Cancelados ÷ total de pedidos registrados. |

