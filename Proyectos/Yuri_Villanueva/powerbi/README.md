# Power BI | Ventas y cumplimiento de entregas

Abre `RetailPedidos.pbip` en **Power BI Desktop** actualizado para Windows. Conserva el archivo y las carpetas `RetailPedidos.Report` y `RetailPedidos.SemanticModel` bajo el mismo directorio. Si tu versión los lista como características en vista previa, habilita **Power BI Project (.pbip)** y **PBIR** en **Archivo > Opciones y configuración > Opciones > Características de vista previa** y reinicia.

El proyecto abre con `ModoDemo=true`. Incluye una copia simulada del modelo Gold al 25/11/2026 para revisar la página sin credenciales. Selecciona **Inicio > Actualizar** para cargarla si los visuales aparecen vacíos. Para la sustentación, conecta el informe al SQL warehouse con los pasos siguientes.

## Conexión al modelo Gold

1. Ejecuta el Job en Databricks y confirma que `dim_fecha`, `dim_sede`, `dim_categoria` y `fact_pedido` figuran en el esquema `yvillanuevam_gold`.
2. En Databricks, abre **SQL Warehouses**, inicia tu warehouse y copia **Server Hostname** y **HTTP Path** desde **Connection Details**. La cuenta que usará Power BI necesita **CAN USE** en el warehouse, **USE CATALOG**, **USE SCHEMA** y **SELECT** sobre las tablas.
3. En Power BI Desktop, abre **Inicio > Transformar datos > Editar parámetros**. Según la versión, puedes configurar los parámetros desde **Administrar parámetros** en el Editor de Power Query.

   | Parámetro | Valor |
   | --- | --- |
   | `Servidor` | Server Hostname sin `https://`. |
   | `RutaHTTP` | HTTP Path del warehouse. |
   | `Catalogo` | El catálogo usado por el Job. |
   | `EsquemaGold` | `yvillanuevam_gold`. |
   | `Proveedor` | `Databricks` si tu workspace admite ese conector. Para Azure o cuando tu entorno requiera el conector alternativo, usa `Azure Databricks`. |
   | `ModoDemo` | `false` para leer las tablas de Databricks. |

4. Pulsa **Cerrar y aplicar**. Selecciona el método de autenticación habilitado en tu workspace y usa tu identidad autorizada. Si Power BI solicita revisar la privacidad de orígenes, aplica la configuración permitida por tu organización.
5. Ejecuta **Actualizar** y verifica que el informe coincide con la salida de `05_calidad`. El modelo está en modo **Importación**. Después de una nueva ejecución del Job, vuelve a pulsar **Actualizar** para acceder a los cambios.
6. Si deseas compartir un archivo autónomo después de validar, usa **Archivo > Guardar como > Archivo .pbix** y conserva el PBIP como código de entrega.

## Lectura de la página

- **Ventas entregadas por categoría.** Identifica la categoría con mayor importe entregado. Sirve para priorizar el seguimiento de surtido e inventario.
- **Puntualidad de entregas por sede.** Ordena las sedes de menor a mayor cumplimiento para ubicar dónde revisar la operación.
- **Segmentadores.** Mes del pedido, sede y categoría filtran las tarjetas y los gráficos.
- **Tarjetas.** Pedidos registrados, ventas entregadas, puntualidad y cancelación. La cancelación usa todos los pedidos como denominador. La puntualidad usa solo los pedidos entregados.

Si la conexión falla, abre primero **Obtener datos > Databricks** en un PBIX nuevo con el mismo hostname, HTTP Path y cuenta. Eso permite distinguir una restricción de autenticación, un warehouse detenido o permisos insuficientes. Los números del modo demo sirven para ensayar la lectura del informe. Para acreditar el requisito del curso, muestra el informe con `ModoDemo=false` y un refresh exitoso desde Gold.

[Documentación del conector Databricks](https://learn.microsoft.com/es-es/power-query/connectors/databricks)

El recurso de tema base `CY23SU04` se distribuye bajo la licencia MIT del [ejemplo de Microsoft Analysis Services](https://github.com/microsoft/Analysis-Services/tree/master/pbidevmode/fabricps-pbip/SamplePBIP).
