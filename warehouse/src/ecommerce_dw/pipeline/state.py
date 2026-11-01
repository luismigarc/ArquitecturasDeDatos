"""Estado del pipeline en S3: manifiestos de lote, estado de cada corrida y marcas de agua.

    control/batches/batch_id=<id>/manifest.json   lo que se extrajo (por entidad)
    control/batches/batch_id=<id>/status.json     EXTRACTED -> LOADED -> PROCESSED | FAILED
    control/batches/batch_id=<id>/reconciliation.json
    control/state/watermarks.json                 última llave procesada por entidad incremental

Un lote se considera PROCESADO sólo cuando la carga a Redshift, dbt run/snapshot y dbt test
terminaron bien. Sólo en ese momento se avanza la marca de agua (record_batch). Si algo falla
antes, la marca no se mueve: el siguiente lote vuelve a extraer desde el mismo punto y la
deduplicación por llave en staging evita duplicados.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .s3io import S3

WATERMARKS_KEY = "control/state/watermarks.json"


def batch_prefix(batch_id: str) -> str:
    return f"control/batches/batch_id={batch_id}/"


def read_watermarks(s3: S3) -> dict:
    return s3.get_json_or_none(WATERMARKS_KEY) or {"watermarks": {}, "history": []}


def commit_watermarks(s3: S3, batch_id: str, manifest: dict) -> dict:
    """Avanza la marca de agua de cada entidad incremental con el máximo extraído en el lote."""
    state = read_watermarks(s3)
    wms = state["watermarks"]
    changed = {}
    for name, m in manifest["entities"].items():
        if m.get("mode") != "incremental" or m.get("max_key") is None:
            continue
        old = wms.get(name, 0)
        new = max(old, int(m["max_key"]))
        if new != old:
            changed[name] = {"from": old, "to": new}
        wms[name] = new
    state["history"] = (state.get("history") or [])[-49:] + [{
        "batch_id": batch_id, "at_utc": datetime.now(timezone.utc).isoformat(), "changed": changed}]
    s3.put_json(WATERMARKS_KEY, state)
    return changed


def write_status(s3: S3, batch_id: str, status: str, **extra) -> dict:
    key = batch_prefix(batch_id) + "status.json"
    current = s3.get_json_or_none(key) or {"batch_id": batch_id, "history": []}
    now = datetime.now(timezone.utc).isoformat()
    current.update(extra)
    current["status"] = status
    current["updated_at_utc"] = now
    current["history"].append({"status": status, "at_utc": now, **{k: v for k, v in extra.items()
                                                                   if k in ("processing_id", "failed_tasks")}})
    s3.put_json(key, current)
    return current
