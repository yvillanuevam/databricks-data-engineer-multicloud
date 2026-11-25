# Guion de sustentación | 15 minutos

## Preparación

- Carga inicial con `fecha_corte=2026-11-24` en un catálogo limpio. Para que el lote del 25 siga disponible para la demo, pausa el horario automático hasta que termines el ensayo y actívalo al concluir.
- Ten abiertas cuatro pestañas: rama de GitHub, Job en Databricks, notebooks y Power BI con `ModoDemo=false`.
- Ensaya el flujo con anticipación y conserva el enlace de una ejecución exitosa en otro catálogo de prueba o una corrida de verificación. En la sesión muestra también la corrida que inicies en vivo.

| Tiempo | Pantalla | Guion |
| --- | --- | --- |
| 0:00–1:30 | GitHub y Job | «El proyecto estudia qué categorías generan más ventas entregadas y qué sedes tienen menor puntualidad. Simulamos los archivos diarios de un sistema de pedidos. Lanzo el Job con el lote del 25 de noviembre mientras explico el código». |
| 1:30–4:00 | `01_raw` y `02_bronze` | «El origen genera 32 pedidos nuevos por día y, dos días después, envía la revisión con el estado final. Raw conserva el CSV. Auto Loader marca los archivos procesados en el checkpoint y guarda el contenido en Delta». |
| 4:00–6:00 | `03_silver` | «Silver lee las versiones nuevas de Bronze, convierte importes y fechas, separa filas inválidas y usa la revisión del pedido para decidir cuándo actualizar. La misma fila repetida no crea un segundo pedido». |
| 6:00–9:00 | `04_gold` y vista Modelo de Power BI | «Gold lee los cambios de Silver y actualiza los pedidos afectados. Cada pedido ocupa una fila. Fecha, sede y categoría aportan el contexto de análisis. Un pedido entregado suma a ventas y cuenta como puntual si la entrega ocurre hasta la fecha prometida». |
| 9:00–11:30 | Job Runs, `05_calidad` y `06_mantenimiento` | «El Job corre a las 06:00 hora de Lima. Las seis tareas tienen dependencias. Revisamos el estado de la ejecución, la conciliación Silver–Gold y el historial donde aparecen OPTIMIZE con ZORDER y VACUUM con 168 horas». |
| 11:30–14:00 | Power BI | «Actualizo el reporte y filtro por mes. El gráfico de categorías muestra dónde se concentra el importe entregado. El gráfico de sedes señala dónde investigar atrasos. La tarjeta de puntualidad divide entregas a tiempo entre el total entregado». |
| 14:00–15:00 | Preguntas | Muestra una revisión actualizada en Gold o explica cómo el checkpoint y el control de versiones permiten reintentar una carga. |

Con el corte de la demo, Tecnología concentra **S/ 233.642,30** en ventas entregadas y es la categoría con mayor importe. Callao tiene la menor puntualidad, **66,32 %** frente al **79,71 %** total. En la simulación, esa brecha orienta la revisión hacia el despacho en Callao.

## Evidencia para la demo

En la consulta SQL de Databricks, reemplaza `workspace` por tu catálogo.

```sql
USE CATALOG workspace;

SELECT * FROM yvillanuevam_raw.control_cdf ORDER BY etapa;

SELECT estado, COUNT(*) AS pedidos
FROM yvillanuevam_gold.fact_pedido
GROUP BY estado ORDER BY estado;

DESCRIBE HISTORY yvillanuevam_gold.fact_pedido LIMIT 10;
```

Para demostrar el CDF de Silver, toma de `DESCRIBE HISTORY yvillanuevam_silver.pedidos` la versión del `MERGE` ejecutado por el lote del 25. Usa ese número en ambos argumentos de versión de `table_changes('yvillanuevam_silver.pedidos', versión, versión)` y agrupa por `_change_type`. En el ensayo se vieron 32 inserciones y 32 actualizaciones, cada una con su imagen anterior y posterior.
