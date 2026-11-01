"""
### dw_pipeline — Data warehouse de e-commerce en AWS

`validate_connections → extract_<fuente>_<entidad> (x6) → register_batch → load_raw_redshift
 → dbt_deps → dbt_seed → dbt_run_intermediate → dbt_run_quarantine → dbt_snapshot → dbt_run_marts
 → dbt_test → reconcile_sales_kpi → refresh_quicksight → record_batch`

* Extracción: RDS MySQL y DocumentDB → Parquet en S3 (`raw/<fuente>/<entidad>/batch_id=<id>/`).
* Carga: `COPY ... FORMAT AS PARQUET` a `raw.*` en Redshift con un rol IAM.
* Transformación y pruebas: dbt-redshift (el SQL se ejecuta en Redshift).

**Parámetros (Trigger DAG w/ config):**
* `reprocess_batch_id`: recarga y transforma un lote existente desde S3 sin consultar las fuentes.
* `extraction_mode`: `incremental` (por defecto, usa marcas de agua) o `full` (copia completa).

**Fallo controlado:** Variable `dw_fail_at` = nombre de una tarea (p. ej. `dbt_run_marts`).
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.getenv("PIPELINE_HOME", "/opt/airflow/src"))

from airflow.decorators import dag, task  # noqa: E402
from airflow.exceptions import AirflowFailException  # noqa: E402
from airflow.models import Variable  # noqa: E402
from airflow.models.param import Param  # noqa: E402
from airflow.operators.python import get_current_context  # noqa: E402
from airflow.utils.state import TaskInstanceState  # noqa: E402
from airflow.utils.trigger_rule import TriggerRule  # noqa: E402

from ecommerce_dw.pipeline import tasks as P  # noqa: E402
from ecommerce_dw.pipeline.entities import ENTITIES  # noqa: E402

log = logging.getLogger("pipeline")


def _ctx() -> dict:
    c = get_current_context()
    p = c["params"]
    return P.build_ctx(c["dag_run"].run_id, p.get("reprocess_batch_id"), p.get("extraction_mode") or "incremental")


def _maybe_fail(task_id: str) -> None:
    if Variable.get("dw_fail_at", default_var="") == task_id:
        raise AirflowFailException(f"Fallo controlado en {task_id} (Variable dw_fail_at)")


@dag(
    dag_id="dw_pipeline",
    description="RDS + DocumentDB -> S3 -> Redshift (COPY) -> dbt -> QuickSight",
    schedule=os.getenv("DW_SCHEDULE") or None,  # vacío = sólo manual
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,                           # una corrida a la vez sobre el warehouse
    default_args={
        "owner": "ingdatos",
        "retries": 2,                            # reintentos acotados
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "reprocess_batch_id": Param(None, type=["null", "string"],
                                    description="Vacío = lote nuevo. batch_id existente = reprocesar desde S3"),
        "extraction_mode": Param("incremental", enum=["incremental", "full"]),
    },
    tags=["datawarehouse", "redshift", "dbt"],
    doc_md=__doc__,
)
def dw_pipeline():

    @task
    def validate_connections():
        _maybe_fail("validate_connections")
        return P.validate_connections(_ctx())

    def make_extract(entity):
        tid = f"extract_{entity.source}_{entity.name}"

        @task(task_id=tid)
        def _extract():
            _maybe_fail(tid)
            m = P.extract_entity(_ctx(), entity.name)
            return {"rows": m["rows"], "mode": m["mode"], "watermark_from": m.get("watermark_from"),
                    "reused": m.get("reused", False)}
        return _extract()

    @task
    def register_batch():
        _maybe_fail("register_batch")
        return P.register_batch(_ctx())

    @task
    def load_raw_redshift():
        _maybe_fail("load_raw_redshift")
        return P.load_raw(_ctx())

    def dbt_task(task_id, command, selector=None, retries=2):
        @task(task_id=task_id, retries=retries)
        def _run():
            _maybe_fail(task_id)
            P.dbt(_ctx(), command, selector)
        return _run()

    @task
    def reconcile_sales_kpi():
        _maybe_fail("reconcile_sales_kpi")
        return P.reconcile_kpi(_ctx())

    @task(retries=1)
    def refresh_quicksight():
        _maybe_fail("refresh_quicksight")
        return P.refresh_quicksight(_ctx())

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def record_batch():
        c = get_current_context()
        failed = [ti.task_id for ti in c["dag_run"].get_task_instances(
            state=[TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]) if ti.task_id != "record_batch"]
        st = P.record_batch(_ctx(), failed)
        if st["status"] == "FAILED":
            raise AirflowFailException(f"Lote fallido; tareas: {failed}")
        return st["status"]

    v = validate_connections()
    extracts = [make_extract(e) for e in ENTITIES]
    reg = register_batch()
    v >> extracts >> reg
    (reg
     >> load_raw_redshift()
     >> dbt_task("dbt_deps", "deps")
     >> dbt_task("dbt_seed", "seed")
     >> dbt_task("dbt_run_intermediate", "run", "path:models/intermediate")
     >> dbt_task("dbt_run_quarantine", "run", "path:models/quarantine")
     >> dbt_task("dbt_snapshot", "snapshot")
     >> dbt_task("dbt_run_marts", "run", "path:models/marts")
     >> dbt_task("dbt_test", "test", retries=0)
     >> reconcile_sales_kpi()
     >> refresh_quicksight()
     >> record_batch())


dw_pipeline()
