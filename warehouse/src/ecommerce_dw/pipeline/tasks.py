"""Pasos del pipeline del data warehouse. El DAG `dw_pipeline` llama exactamente estas funciones.

Identificadores:
  batch_id      -> lote de extracción (carpeta batch_id=<id> en S3 y columna _batch_id en raw).
  processing_id -> la corrida del DAG. En una corrida normal batch_id == processing_id.
                   Al reprocesar (param reprocess_batch_id) se reutiliza un batch_id existente:
                   NO se consultan las fuentes y se recarga el mismo lote desde S3.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone

from ..connections import connect_mongo, connect_mysql, connect_warehouse, mongo_database
from . import extract, load, reconcile, state
from .entities import BY_NAME, ENTITIES
from .s3io import S3

log = logging.getLogger("pipeline")


def sanitize(run_id: str) -> str:
    rid = run_id.replace("+00:00", "Z")
    return re.sub(r"[^A-Za-z0-9_]", "", rid.replace("-", "").replace(":", ""))[:120]


def build_ctx(run_id: str, reprocess_batch_id: str | None = None, extraction_mode: str = "incremental") -> dict:
    processing_id = sanitize(run_id)
    reprocess = (reprocess_batch_id or "").strip()
    if extraction_mode not in ("incremental", "full"):
        raise ValueError("extraction_mode debe ser 'incremental' o 'full'")
    return {
        "processing_id": processing_id,
        "batch_id": reprocess or processing_id,
        "mode": "reprocess" if reprocess else "extract",
        "extraction_mode": extraction_mode,
    }


def get_s3() -> S3:
    return S3(os.environ["DW_BUCKET"], os.getenv("AWS_REGION"))


# 1 ------------------------------------------------------------------------------------------
def validate_connections(ctx: dict, s3: S3 | None = None) -> dict:
    """Valida parámetros y acceso a S3, RDS, DocumentDB y Redshift antes de mover datos."""
    s3 = s3 or get_s3()
    report = {"batch_id": ctx["batch_id"], "mode": ctx["mode"], "extraction_mode": ctx["extraction_mode"]}
    s3.client.head_bucket(Bucket=s3.bucket)
    if os.getenv("WAREHOUSE_ENGINE", "redshift") == "redshift" and not os.getenv("REDSHIFT_IAM_ROLE_ARN"):
        raise RuntimeError("Falta REDSHIFT_IAM_ROLE_ARN (rol que usa COPY para leer S3)")
    if ctx["mode"] == "reprocess":
        manifest = s3.get_json_or_none(state.batch_prefix(ctx["batch_id"]) + "manifest.json")
        if manifest is None:
            raise RuntimeError(f"El lote {ctx['batch_id']} no existe en S3; no se puede reprocesar")
        report["reprocess_manifest_rows"] = {k: v["rows"] for k, v in manifest["entities"].items()}
    else:
        conn = connect_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM customers")
                report["mysql_customers"] = cur.fetchone()[0]
        finally:
            conn.close()
        client = connect_mongo()
        try:
            report["documentdb_reviews"] = mongo_database(client)["reviews"].estimated_document_count()
        finally:
            client.close()
    w = connect_warehouse()
    try:
        with w.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            report["warehouse"] = list(cur.fetchone())
    finally:
        w.close()
    log.info("[batch=%s][task=validate_connections] %s", ctx["batch_id"], report)
    return report


# 2 ------------------------------------------------------------------------------------------
def extract_entity(ctx: dict, entity_name: str, s3: S3 | None = None, row_source=None) -> dict:
    s3 = s3 or get_s3()
    entity = BY_NAME[entity_name]
    if ctx["mode"] == "reprocess":
        m = s3.get_json(entity.s3_prefix(ctx["batch_id"]) + "_manifest.json")
        log.info("[batch=%s][entity=%s] reprocesamiento: NO se consulta la fuente", ctx["batch_id"], entity_name)
        return m
    wm = state.read_watermarks(s3)["watermarks"]
    return extract.extract_entity(s3, entity, ctx["batch_id"], ctx["extraction_mode"], wm, row_source)


# 3 ------------------------------------------------------------------------------------------
def register_batch(ctx: dict, s3: S3 | None = None) -> dict:
    """Junta los manifiestos de las 6 entidades en el manifiesto del lote."""
    s3 = s3 or get_s3()
    key = state.batch_prefix(ctx["batch_id"]) + "manifest.json"
    if ctx["mode"] == "reprocess":
        manifest = s3.get_json(key)
    else:
        entities = {e.name: s3.get_json(e.s3_prefix(ctx["batch_id"]) + "_manifest.json") for e in ENTITIES}
        manifest = {"batch_id": ctx["batch_id"], "extraction_mode": ctx["extraction_mode"],
                    "registered_at_utc": datetime.now(timezone.utc).isoformat(), "entities": entities}
        s3.put_json(key, manifest)
    state.write_status(s3, ctx["batch_id"], "EXTRACTED", processing_id=ctx["processing_id"],
                       rows={k: v["rows"] for k, v in manifest["entities"].items()})
    return {k: v["rows"] for k, v in manifest["entities"].items()}


# 4 ------------------------------------------------------------------------------------------
def load_raw(ctx: dict, s3: S3 | None = None, conn=None) -> dict:
    s3 = s3 or get_s3()
    manifest = s3.get_json(state.batch_prefix(ctx["batch_id"]) + "manifest.json")
    result = load.load_batch(s3, manifest, conn=conn)
    state.write_status(s3, ctx["batch_id"], "LOADED", processing_id=ctx["processing_id"], loaded=result)
    return result


# 5 (dbt) ------------------------------------------------------------------------------------
def dbt(ctx: dict, command: str, selector: str | None = None) -> None:
    from .dbt_runner import run_dbt
    run_dbt(command, selector, batch_id=ctx["batch_id"])


# 6 ------------------------------------------------------------------------------------------
def reconcile_kpi(ctx: dict, s3: S3 | None = None, source_fn=None, conn=None) -> dict:
    s3 = s3 or get_s3()
    res = reconcile.reconcile(source_fn or reconcile.source_sales_totals, conn=conn)
    res.update(batch_id=ctx["batch_id"], processing_id=ctx["processing_id"],
               at_utc=datetime.now(timezone.utc).isoformat())
    s3.put_json(state.batch_prefix(ctx["batch_id"]) + "reconciliation.json", res)
    log.info("[batch=%s][task=reconcile] %s", ctx["batch_id"], res)
    if not res["ok"]:
        raise RuntimeError(f"Conciliación de ventas no coincide: {res}")
    return res


# 7 ------------------------------------------------------------------------------------------
def refresh_quicksight(ctx: dict) -> str:
    """Si el dataset de QuickSight usa SPICE, lanza su actualización al terminar el lote."""
    dataset_id = os.getenv("QUICKSIGHT_DATASET_ID", "").strip()
    if not dataset_id:
        log.info("[batch=%s] QUICKSIGHT_DATASET_ID vacío: dataset en DirectQuery o refresco manual", ctx["batch_id"])
        return "skipped"
    import boto3
    qs = boto3.client("quicksight", region_name=os.getenv("AWS_REGION"))
    ingestion_id = ("b" + ctx["processing_id"])[:60]
    qs.create_ingestion(AwsAccountId=os.environ["AWS_ACCOUNT_ID"], DataSetId=dataset_id, IngestionId=ingestion_id)
    log.info("[batch=%s] refresco SPICE solicitado: %s", ctx["batch_id"], ingestion_id)
    return ingestion_id


# 8 ------------------------------------------------------------------------------------------
def record_batch(ctx: dict, failed_tasks: list[str], s3: S3 | None = None) -> dict:
    """Marca de avance: PROCESSED + marcas de agua sólo si todo terminó bien."""
    s3 = s3 or get_s3()
    if failed_tasks:
        return state.write_status(s3, ctx["batch_id"], "FAILED", processing_id=ctx["processing_id"],
                                  failed_tasks=failed_tasks)
    changed = {}
    if ctx["mode"] == "extract":
        manifest = s3.get_json(state.batch_prefix(ctx["batch_id"]) + "manifest.json")
        changed = state.commit_watermarks(s3, ctx["batch_id"], manifest)
    return state.write_status(s3, ctx["batch_id"], "PROCESSED", processing_id=ctx["processing_id"],
                              failed_tasks=[], watermarks_advanced=changed)
