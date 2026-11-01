"""Extracción desde RDS MySQL y DocumentDB a Parquet en un bucket S3 privado.

Ruta de cada lote:  s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet
                    s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/_manifest.json

* El Parquet se escribe con escritura condicional (If-None-Match): nunca se sobrescribe en
  silencio el archivo de otro lote ni el del mismo lote.
* Si el lote ya tiene _manifest.json (por ejemplo, un reintento de Airflow), se reutiliza y
  NO se vuelve a consultar la fuente.
* Formato Parquet: conserva tipos (DECIMAL, TIMESTAMP, BOOLEAN), es columnar y comprimido, y
  Redshift lo carga con COPY ... FORMAT AS PARQUET sin problemas de separadores o comillas.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from bson import json_util
from pymysql.cursors import SSCursor

from ..connections import connect_mongo, connect_mysql, mongo_database
from .entities import Entity
from .s3io import S3, AlreadyExists

log = logging.getLogger("pipeline")
BATCH_SIZE = 1000


def _naive_utc(value):
    """Fechas de DocumentDB llegan con zona (UTC); Redshift TIMESTAMP se guarda en UTC sin zona."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _to_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def mysql_rows(entity: Entity, watermark: int | None):
    cols = ", ".join(c.name for c in entity.columns)
    sql = f"SELECT {cols} FROM {entity.name}"
    params: tuple = ()
    if watermark is not None:
        sql += f" WHERE {entity.key} > %s"
        params = (watermark,)
    sql += f" ORDER BY {entity.key}"
    conn = connect_mysql(cursorclass=SSCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            names = [c.name for c in entity.columns]
            while batch := cur.fetchmany(BATCH_SIZE):
                for row in batch:
                    yield dict(zip(names, row))
    finally:
        conn.close()


def flatten_mongo(collection: str, doc: dict) -> dict:
    customer = doc.get("customer") or {}
    product = doc.get("product") or {}
    if collection == "reviews":
        verification = doc.get("verification") or {}
        return {
            "review_id": _to_int(doc.get("review_id")),
            "customer_id": _to_int(customer.get("customer_id")),
            "product_id": _to_int(product.get("product_id")),
            "rating": _to_int(doc.get("rating")),
            "reviewed_at": _naive_utc(doc.get("reviewed_at")),
            "is_verified_purchase": verification.get("purchase_confirmed"),
            "document_json": json_util.dumps(doc, ensure_ascii=False),
        }
    device = doc.get("device") or {}
    return {
        "event_id": _to_int(doc.get("event_id")),
        "event_type": doc.get("event_type"),
        "occurred_at": _naive_utc(doc.get("occurred_at")),
        "customer_id": _to_int(customer.get("customer_id")),
        "product_id": _to_int(product.get("product_id")),
        "sales_channel": device.get("sales_channel"),
        "document_json": json_util.dumps(doc, ensure_ascii=False),
    }


def mongo_rows(entity: Entity, watermark: int | None, client=None):
    own = client is None
    client = client or connect_mongo(tz_aware=True)
    try:
        coll = mongo_database(client)[entity.name]
        query = {entity.key: {"$gt": watermark}} if watermark is not None else {}
        for doc in coll.find(query).sort(entity.key, 1).batch_size(BATCH_SIZE):
            yield flatten_mongo(entity.name, doc)
    finally:
        if own:
            client.close()


def extract_entity(s3: S3, entity: Entity, batch_id: str, mode: str, watermarks: dict,
                   row_source=None) -> dict:
    """Extrae una entidad al prefijo del lote. Devuelve su manifiesto.

    mode: 'incremental' respeta la marca de agua de las entidades incrementales;
          'full' fuerza la copia completa (recuperación tras regenerar las fuentes).
    """
    prefix = entity.s3_prefix(batch_id)
    manifest_key = prefix + "_manifest.json"
    existing = s3.get_json_or_none(manifest_key)
    if existing and existing.get("status") == "OK":
        log.info("[batch=%s][entity=%s] ya extraída; se reutiliza sin consultar la fuente", batch_id, entity.name)
        existing["reused"] = True
        return existing

    effective_mode = "incremental" if (entity.mode == "incremental" and mode == "incremental") else "full"
    watermark = watermarks.get(entity.name, 0) if effective_mode == "incremental" else None
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    if row_source is not None:
        rows_iter = row_source(entity, watermark)
    elif entity.source == "mysql":
        rows_iter = mysql_rows(entity, watermark)
    else:
        rows_iter = mongo_rows(entity, watermark)

    # MySQL devuelve BOOLEAN como TINYINT (1/0): se convierte a bool para el esquema Parquet.
    bool_cols = [c.name for c in entity.columns if c.arrow == pa.bool_()]
    rows = []
    for r in rows_iter:
        for col in bool_cols:
            if r.get(col) is not None:
                r[col] = bool(r[col])
        r["_batch_id"] = batch_id
        r["_extracted_at"] = extracted_at
        r["_source"] = entity.source_label
        rows.append(r)

    keys = [r[entity.key] for r in rows if r.get(entity.key) is not None]
    manifest = {
        "status": "OK",
        "batch_id": batch_id,
        "source": entity.source,
        "entity": entity.name,
        "mode": effective_mode,
        "watermark_from": watermark,
        "max_key": max(keys) if keys else watermark,
        "rows": len(rows),
        "extracted_at_utc": extracted_at.isoformat() + "Z",
        "s3_key": None,
        "reused": False,
    }
    if rows:
        table = pa.Table.from_pylist(rows, schema=entity.arrow_schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        key = prefix + "part-00000.parquet"
        try:
            s3.put_bytes(key, buf.getvalue(), "application/vnd.apache.parquet", if_none_match=True)
        except AlreadyExists:
            # Un intento anterior alcanzó a escribir el Parquet pero no el manifiesto:
            # se conserva el archivo original y se toman sus conteos.
            old = pq.read_table(io.BytesIO(s3.get_bytes(key)))
            manifest["rows"] = old.num_rows
            old_keys = [k for k in old.column(entity.key).to_pylist() if k is not None]
            manifest["max_key"] = max(old_keys) if old_keys else watermark
            log.warning("[batch=%s][entity=%s] Parquet ya existía; se conserva", batch_id, entity.name)
        manifest["s3_key"] = key
    s3.put_json(manifest_key, manifest)
    log.info("[batch=%s][entity=%s] modo=%s marca=%s filas=%s", batch_id, entity.name,
             effective_mode, watermark, manifest["rows"])
    return manifest
