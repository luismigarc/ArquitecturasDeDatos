# Arquitecturas de Datos — entrega 2026-2

Este repositorio reúne las dos soluciones de la actividad evaluativa. Comparten el propósito académico de demostrar pipelines de datos en AWS, pero resuelven problemas distintos y por eso se presentan como dos proyectos independientes:

| Proyecto | Problema | Flujo principal | Consumidor |
|---|---|---|---|
| [Data Lake de noticias](data-lake/README.md) | Ingestar y analizar noticias de feeds RSS | RSS → S3 Bronze/Silver/Gold → Comprehend → Glue | Athena |
| [Data Warehouse de e-commerce](warehouse/README.md) | Migrar el DW local y analizar ventas, reseñas y eventos | RDS/DocumentDB → S3 → Redshift → dbt | QuickSight |

La primera solución explora datos semiestructurados y análisis de texto; la segunda conserva un modelo dimensional transaccional. No son etapas de un mismo pipeline ni comparten conjuntos de datos. La actividad reúne ambas arquitecturas y este repositorio las organiza con una convención común.

## Estructura común de cada proyecto

```text
<proyecto>/
├── README.md       # propósito, arquitectura y guía de lectura
├── docs/           # despliegue, decisiones y modelo
├── dags/           # orquestación Airflow
├── src/            # código y configuración de la solución
├── docker/         # Dockerfile y Compose
└── evidencias/     # resultados organizados por escenario o componente
```

El Warehouse incluye además `infra/`, `sql/`, `tests/` y `tools/`, necesarios para su despliegue AWS, modelos analíticos y validaciones. Cada README de proyecto indica sus particularidades y conduce a las instrucciones y evidencias específicas.

## Orden sugerido para revisar la entrega

1. Leer este resumen y luego el README de cada proyecto.
2. Revisar los diagramas y las decisiones de arquitectura en `docs/`.
3. Seguir las guías de despliegue y ejecución antes de reproducir escenarios.
4. Contrastar resultados y capturas en `evidencias/` con los requisitos de calidad, incrementalidad y recuperación.

Las evidencias muestran ejecuciones realizadas; los comandos de despliegue y los archivos de código permiten entender cómo reproducirlas. Las capturas son complementarias.
