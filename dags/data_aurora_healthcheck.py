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


def _contexto_execucao() -> dict:
    """Identificação da execução para a primeira linha do log da task."""
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    ti = ctx["ti"]
    return {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ctx["run_id"],
        "data_logica": ctx["logical_date"].isoformat(),
        "tentativa": ti.try_number,
        "max_tentativas": ti.max_tries + 1,
    }


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
        from data_aurora.container import build_container
        from data_aurora.shared.logging import get_logger, log_event, log_execucao

        logger = get_logger("dags.healthcheck")
        log_execucao(logger, _contexto_execucao())
        results = build_container().healthcheck.execute()
        resumo = [{"service": r.service, "ok": r.ok, "detail": r.detail} for r in results]
        log_event(logger, "resumo", servicos=[f"{r['service']}:{'ok' if r['ok'] else 'falha'}" for r in resumo])
        return resumo

    verificar_servicos()


data_aurora_healthcheck()
