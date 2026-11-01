"""Ejecuta comandos de dbt (dbt-redshift). El SQL se ejecuta en Redshift; aquí sólo se orquesta."""
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt"))
log = logging.getLogger("pipeline")


def run_dbt(command: str, selector: str | None = None, extra: list[str] | None = None,
            batch_id: str | None = None) -> None:
    args = [DBT_EXECUTABLE, command, "--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT),
            "--target", os.getenv("DBT_TARGET", "redshift")]
    if selector:
        args += ["--select", selector]
    if batch_id and command not in ("deps", "seed"):
        args += ["--vars", f"{{batch_id: '{batch_id}'}}"]
    args += extra or []
    log.info("[batch=%s] dbt: %s", batch_id, " ".join(args))
    proc = subprocess.run(args, text=True, capture_output=True)
    for line in (proc.stdout or "").splitlines():
        log.info("[batch=%s][dbt %s] %s", batch_id, command, line)
    if proc.returncode != 0:
        for line in (proc.stderr or "").splitlines():
            log.error("[batch=%s][dbt %s] %s", batch_id, command, line)
        raise RuntimeError(f"dbt {command} falló (código {proc.returncode}); revise el log de la tarea")
