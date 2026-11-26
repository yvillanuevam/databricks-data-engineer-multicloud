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

## Publicación en GitHub

El repositorio del curso requiere una cuenta con permiso **Write**. Si no aparece **New branch** en [Branches](https://github.com/juan293114/databricks-data-engineer-multicloud/branches), pide al docente que habilite tu cuenta o cree la rama y te permita publicar en ella.

1. Descomprime el archivo ZIP. Clona el repositorio del curso en una carpeta distinta e inicia la rama desde `main`.

   ```powershell
   git clone https://github.com/juan293114/databricks-data-engineer-multicloud.git
   cd databricks-data-engineer-multicloud
   git switch -c feature/yvillanuevam origin/main
   ```

   Si la rama remota ya existe al clonar, usa `git switch --track origin/feature/yvillanuevam` para continuar en ella.

2. Copia `Proyectos/Yuri_Villanueva` del ZIP a la misma ruta del repositorio clonado. Luego registra y publica el cambio.

   ```powershell
   git add -- Proyectos/Yuri_Villanueva
   git commit -m "Agregar proyecto final de Databricks de Yuri Villanueva"
   git push -u origin feature/yvillanuevam
   ```

3. En GitHub selecciona `feature/yvillanuevam` y abre `Proyectos/Yuri_Villanueva/README.md`. Completa el registro de la pareja en la hoja del curso y agrega el nombre del segundo integrante al encabezado antes de entregar.

## Despliegue y ejecución en Databricks

Usa un workspace con Unity Catalog, compute serverless y permiso para crear esquemas en el catálogo elegido. Consulta el catálogo con `SELECT current_catalog();` o selecciónalo en **Catalog**. El proyecto crea los cuatro esquemas con prefijo `yvillanuevam_`. Usa un catálogo propio o acordado con el propietario.

### Opción 1. Desplegar el Job con Databricks CLI

Desde una terminal con la [CLI oficial](https://docs.databricks.com/aws/en/dev-tools/cli/install):

```powershell
cd Proyectos/Yuri_Villanueva
databricks auth login --host https://TU-WORKSPACE --profile curso
databricks bundle validate -p curso --var="catalogo=workspace"
databricks bundle deploy -p curso --var="catalogo=workspace"
databricks bundle run -p curso --var="catalogo=workspace" --params="fecha_corte=2026-11-24" retail_pedidos
```

Cambia `workspace` por el nombre de tu catálogo. El Job queda programado todos los días a las **06:00, hora de Lima**, con máximo una ejecución simultánea. La fecha de corte queda vacía por defecto y toma la fecha local de cada corrida programada. Una fecha fija permite reproducir la demostración. Volver a una fecha anterior no elimina datos cargados previamente.

### Opción 2. Configurar el Job desde la interfaz

1. En **Workspace**, crea un **Git folder** con el repositorio del curso y selecciona `feature/yvillanuevam`. Para la conexión Git, usa tu identidad autorizada en **Settings > Linked accounts**. Si aún no puedes publicar, crea una carpeta de Workspace e importa los siete `.py` de `notebooks` como notebooks Databricks. Deben estar juntos para resolver `%run ./00_config`.
2. En **Jobs & Pipelines > Create > Job**, agrega seis tareas de tipo **Notebook** con compute **Serverless**. Selecciona `01_raw`, `02_bronze`, `03_silver`, `04_gold`, `05_calidad` y `06_mantenimiento` en ese orden. Configura **Depends on** para que cada tarea dependa de la anterior.
3. En **Job parameters**, crea `catalogo` con el catálogo elegido y `fecha_corte` con valor predeterminado vacío. En **Schedules & Triggers**, agrega una programación diaria a las 06:00, zona **America/Lima**, y actívala. Mantén **Maximum concurrent runs** en 1.
4. Usa **Run now with different parameters** e indica `fecha_corte=2026-11-24` para la carga de ensayo. Abre la ejecución desde **Runs**. Para la demo incremental, ejecuta con `fecha_corte=2026-11-25` en el mismo catálogo de ensayo.

### Resultados de referencia

La transformación se ensayó con Spark 4.0.1 y Delta Lake 4.0.0 locales. Los CSV se insertaron en Bronze mediante un adaptador de prueba. El conector Auto Loader, Unity Catalog, la ejecución de Lakeflow Jobs y la conexión autenticada de Power BI se verifican en el workspace del equipo.

| Fecha de corte | Eventos acumulados en Bronze | Pedidos Gold | Rechazados Silver | Efecto esperado |
| --- | ---: | ---: | ---: | --- |
| 24/11/2026 | 3.472 | 1.760 | 8 | Carga inicial en catálogo limpio. |
| 25/11/2026 | 3.536 | 1.792 | 8 | Entran 64 eventos. Silver inserta 32 pedidos y actualiza 32. |
| Repetición del 25/11 | 3.536 | 1.792 | 8 | Sin archivos nuevos ni cambios de datos pendientes. |

Al 25/11 la prueba local produjo **S/ 521.743,40** en ventas entregadas, **1.636** pedidos entregados, **79,71 %** de puntualidad y **92** cancelados. El control verificó cero duplicados, huérfanos y diferencias entre Silver y Gold. `OPTIMIZE` y `VACUUM` quedaron registrados en el historial Delta de la prueba.

## Power BI

- [Conectar y actualizar el informe de una página](powerbi/README.md).
