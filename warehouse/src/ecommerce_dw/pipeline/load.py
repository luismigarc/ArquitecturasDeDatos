"""Carga de un lote de S3 a la capa raw de Redshift con COPY.

Por cada entidad, en UNA transacción:
    CREATE TABLE IF NOT EXISTS raw.<tabla>
    DELETE FROM raw.<tabla> WHERE _batch_id = <lote>      -- reprocesar no duplica
    COPY raw.<tabla> FROM 's3://.../part-00000.parquet' IAM_ROLE '<rol>' FORMAT AS PARQUET
    verificación: filas cargadas del lote = filas del manifiesto
Raw acumula todos los lotes (historial de lo recibido); staging en dbt toma la versión
vigente de cada llave.

El acceso de Redshift a S3 se autoriza con un rol IAM asociado al namespace (no hay llaves).
WAREHOUSE_ENGINE=postgres es sólo para las pruebas locales: inserta las filas del Parquet.
"""
from __future__ import annotations

import io
import logging
import os

import pyarrow.parquet as pq

from ..connections import connect_warehouse
from .entities import ENTITIES
from .s3io import S3

log = logging.getLogger("pipeline")


def _copy_redshift(cur, entity, bucket: str, key: str):
    role = os.environ["REDSHIFT_IAM_ROLE_ARN"]
    cur.execute(
        f"COPY {entity.raw_table} FROM %s IAM_ROLE %s FORMAT AS PARQUET",
        (f"s3://{bucket}/{key}", role),
    )


def _insert_postgres(cur, entity, s3: S3, key: str):
    from psycopg2.extras import execute_values

    table = pq.read_table(io.BytesIO(s3.get_bytes(key)))
    names = [c.name for c in entity.all_columns]
    rows = [tuple(r[n] for n in names) for r in table.to_pylist()]
    execute_values(cur, f"INSERT INTO {entity.raw_table} ({', '.join(names)}) VALUES %s", rows)


def load_batch(s3: S3, manifest: dict, engine: str | None = None, conn=None) -> dict:
    engine = engine or os.getenv("WAREHOUSE_ENGINE", "redshift")
    batch_id = manifest["batch_id"]
    own = conn is None
    conn = conn or connect_warehouse()
    results = {}
    try:
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS "raw"')  # RAW es palabra reservada en Redshift
        conn.commit()
        for entity in ENTITIES:
            m = manifest["entities"][entity.name]
            with conn.cursor() as cur:
                cur.execute(entity.ddl())
                cur.execute(f"DELETE FROM {entity.raw_table} WHERE _batch_id = %s", (batch_id,))
                deleted = cur.rowcount
                if m["rows"] and m.get("s3_key"):
                    if engine == "redshift":
                        _copy_redshift(cur, entity, s3.bucket, m["s3_key"])
                    else:
                        _insert_postgres(cur, entity, s3, m["s3_key"])
                cur.execute(f"SELECT count(*) FROM {entity.raw_table} WHERE _batch_id = %s", (batch_id,))
                loaded = cur.fetchone()[0]
                if loaded != m["rows"]:
                    conn.rollback()
                    raise RuntimeError(f"{entity.raw_table}: manifiesto={m['rows']} cargadas={loaded} (lote {batch_id})")
            conn.commit()
            results[entity.name] = {"rows": loaded, "replaced_previous": max(deleted, 0)}
            log.info("[batch=%s][entity=%s] %s filas en %s (reemplazadas %s)", batch_id, entity.name,
                     loaded, entity.raw_table, max(deleted, 0))
    finally:
        if own:
            conn.close()
    return results


def count_raw(conn=None) -> dict:
    own = conn is None
    conn = conn or connect_warehouse()
    try:
        with conn.cursor() as cur:
            out = {}
            for e in ENTITIES:
                cur.execute(f"SELECT count(*), count(distinct _batch_id) FROM {e.raw_table}")
                rows, batches = cur.fetchone()
                out[e.raw_table] = {"rows": rows, "batches": batches}
            return out
    finally:
        if own:
            conn.close()
