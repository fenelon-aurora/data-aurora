"""Healthcheck do data-aurora: confirma que o Airflow carrega o pacote e alcança o People.

Rode manualmente depois de cada deploy ou mudança de configuração.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# O sidecar sincroniza s3://<bucket>/data-aurora/ em /opt/airflow/dags/data-aurora/{dags,src}.
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from airflow.decorators import dag, task  # noqa: E402

DEFAULT_ARGS = {"owner": "data-aurora", "retries": 1, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="data_aurora_healthcheck",
    description="Confere pacote data_aurora, configuração e acesso ao People",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-aurora", "infra"],
)
def data_aurora_healthcheck():
    @task
    def verificar_servicos() -> list[dict]:
        from data_aurora.composition.container import build_container

        results = build_container().healthcheck.execute()
        return [{"service": r.service, "ok": r.ok, "detail": r.detail} for r in results]

    verificar_servicos()


data_aurora_healthcheck()
