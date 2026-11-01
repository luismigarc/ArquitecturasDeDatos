# Data Lake de noticias RSS

Proyecto de Ingeniería de Datos 2026-2. Implementa un pipeline de noticias de El Tiempo en AWS: ingesta RSS, almacenamiento Bronze en S3, limpieza y deduplicación en Silver, enriquecimiento con Amazon Comprehend, agregaciones Gold, catálogo Glue y consultas Athena. Airflow coordina el flujo desde una instancia EC2.

## Recorrido del dato

```text
3 feeds RSS
   → Airflow en EC2
   → S3 Bronze (XML original + metadata por fuente y lote)
   → Silver (noticias vigentes + cuarentena)
   → Amazon Comprehend (entidades, frases clave y caché por hash)
   → Gold (Parquet agregado)
   → Glue Data Catalog
   → Athena
```

## Estructura del proyecto

| Ruta | Contenido |
|---|---|
| `src/config/sources.json` | Feeds RSS seleccionados y sus secciones |
| `dags/data_lake_noticias_dag.py` | DAG: ingesta, transformación, enriquecimiento, Glue y validación Athena |
| `src/scripts/` | Scripts de ingesta, Silver, Comprehend y Gold |
| `docker/docker-compose.yaml` y `docker/Dockerfile` | Despliegue de Airflow en Docker Compose |
| `docs/arquitectura_data_lake.png` | Diagrama de arquitectura |
| `evidencias/` | Evidencias clasificadas por capa y componente |

Los scripts se montan en Airflow bajo `/opt/airflow/scripts`; la configuración se monta bajo `/opt/airflow/config`. `ingest_rss.py` carga `src/config/sources.json` desde la raíz del proyecto.

## Fuentes RSS

Se seleccionaron tres secciones del directorio RSS de El Tiempo. Las URL configuradas son feeds XML directos, no la página del directorio:

| Medio | Sección | Feed |
|---|---|---|
| El Tiempo | Tecnología | `https://www.eltiempo.com/rss/tecnosfera_novedades-tecnologia.xml` |
| El Tiempo | Videojuegos | `https://www.eltiempo.com/rss/tecnosfera_videojuegos.xml` |
| El Tiempo | Entretenimiento | `https://www.eltiempo.com/rss/cultura_entretenimiento.xml` |

La ingesta conserva XML original, URL, estado HTTP, hash SHA-256, fecha UTC, `run_id` y ubicación S3. Si falla una fuente, la tarea termina con error para que una respuesta fallida no parezca una fuente vacía.

## Capas y reglas

- **Bronze:** respuesta XML original por fuente, sección, fecha de ingesta y `run_id`; sus metadatos permiten localizar y reprocesar el contenido.
- **Silver:** extrae GUID/identificador o enlace, limpia HTML, conserva la fecha original y la normaliza a UTC, valida campos obligatorios y manda rechazos a cuarentena con motivo y referencia Bronze. La clave de noticia es un SHA-256 del identificador disponible. Si la misma noticia llega en varios feeds, se conserva una fila vigente por identificador; la sección y fuente vigentes corresponden al registro más reciente procesado.
- **Comprehend:** analiza título y descripción en español. Guarda entidades PERSON, ORGANIZATION y LOCATION, frases clave, texto analizado, idioma, fecha, puntuaciones y estado. Una caché por hash evita volver a analizar texto idéntico; el texto se limita al máximo tratado por la API síncrona.
- **Gold:** `news_daily` cuenta noticias por fecha, medio y sección; `top_entities` cuenta noticias distintas por entidad; `key_phrases_by_section` cuenta noticias distintas por frase y sección; `entity_daily` mide noticias distintas por entidad y día.
- **Glue y Athena:** un crawler registra tablas y particiones; Athena consulta los Parquet y permite comparar bytes escaneados con y sin filtro de partición.

## Ejecución y reproceso

El DAG es `data_lake_noticias`. Para una ingesta nueva se ejecuta normalmente. Para reprocesar el contenido ya guardado en Bronze, se dispara con esta configuración JSON:

```json
{"reprocess_from_bronze": true}
```

La rama de reproceso omite la consulta a RSS y continúa desde los objetos existentes en Bronze. El Compose se ejecuta desde `docker/`. Antes de ejecutar en AWS, configure la región, el bucket y el rol/permisos de la instancia EC2 para S3, Comprehend, Glue y Athena. No se deben incluir credenciales en el repositorio.

```bash
cd docker
docker compose up --build -d
```

La consola de Airflow se publica en el puerto 8080 según `docker-compose.yaml`. Para el despliegue en EC2 y la configuración de servicios, se deben usar las credenciales IAM de la instancia y los recursos existentes de la cuenta del curso.

## Evidencias para la evaluación

| Requisito | Evidencias |
|---|---|
| Arquitectura y despliegue | `docs/`, `evidencias/airflow/` |
| Bronze y metadatos por lote | `evidencias/bronze/` |
| Silver, deduplicación y cuarentena | `evidencias/silver/` |
| Comprehend y reutilización de caché | `evidencias/comprehend/` |
| Agregaciones Gold | `evidencias/gold/` |
| Glue y particiones | `evidencias/glue/` |
| Consultas Athena y bytes escaneados | `evidencias/athena/` |
| DAG, idempotencia, fallo, recuperación y reproceso | `evidencias/airflow/` |

Las carpetas de evidencias contienen capturas, resultados y algunos Parquet/XML de muestra. La demostración debe ejecutarse sobre los recursos AWS y mostrar una ingesta, consultas Athena y reproceso/recuperación.

## Alcance y limitaciones

El pipeline procesa únicamente el contenido publicado en los feeds RSS; no descarga el texto completo de las páginas enlazadas. La identidad depende del GUID o enlace que entregue cada feed, por lo que variaciones del identificador pueden representar la misma noticia como registros distintos. Comprehend analiza el título y la descripción disponibles, no el artículo completo. El DAG actual se dispara manualmente (`schedule=None`); si se requiere una ejecución periódica, configure el horario para el laboratorio.

