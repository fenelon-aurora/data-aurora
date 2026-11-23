"""Encerra no People as coletas de ciclo com prazo vencido (D58, fase 3 do plano de prazo).

Depende da rota interna `POST /internal/cycles/close-expired-collections` do People, que ainda não
existe: a DAG nasce pausada (padrão do Airflow) e só deve ser ativada depois dela.
A operação é idempotente no People; rodar de novo não encerra nada a mais.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from airflow.decorators import dag, task  # noqa: E402
from airflow.models.param import Param  # noqa: E402


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


def _avisar(context: dict, status: str) -> None:
    """Callback de sucesso/falha: e-mail SES para o time (data_aurora_notify_*). Nunca falha a task."""
    try:
        from data_aurora.container import build_notifier
        from data_aurora.shared.run_report import run_report_from_context
        from data_aurora.shared.settings import load_settings

        resumo = context["ti"].xcom_pull(task_ids=context["ti"].task_id) if status == "sucesso" else None
        resumo = resumo if isinstance(resumo, dict) else {}
        linhas = {
            "Ciclos encerrados": resumo.get("closed", "—"),
            "Dry run": resumo.get("dry_run", "—"),
            "Ciclos": ", ".join(resumo.get("cycle_ids", [])) or "—",
        } if status == "sucesso" else {}
        report = run_report_from_context(context, status, load_settings().airflow_base_url, linhas)
        build_notifier().execute(report, changed=bool(resumo.get("closed")))
    except Exception as exc:  # noqa: BLE001
        import logging

        logging.getLogger("airflow.task.data_aurora.dags").error("aviso.erro_callback | erro=%s", type(exc).__name__)


def _avisar_sucesso(context: dict) -> None:
    _avisar(context, "sucesso")


def _avisar_falha(context: dict) -> None:
    _avisar(context, "falha")


DEFAULT_ARGS = {
    "owner": "data-aurora",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Falha: só depois da última tentativa. Sucesso: conforme data_aurora_notify_success.
    "on_failure_callback": _avisar_falha,
    "on_success_callback": _avisar_sucesso,
}


@dag(
    dag_id="people_close_expired_collections",
    description="Encerra coletas de ciclo com prazo vencido (People, D58)",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={"dry_run": Param(False, type="boolean", description="Só lista o que seria encerrado")},
    tags=["data-aurora", "people", "ciclo"],
)
def people_close_expired_collections():
    @task
    def encerrar(params: dict | None = None) -> dict:
        from data_aurora.container import build_container
        from data_aurora.shared.logging import get_logger, log_event, log_execucao

        logger = get_logger("dags.people_close_expired_collections")
        dry_run = bool((params or {}).get("dry_run", False))
        log_execucao(logger, {**_contexto_execucao(), "dry_run": dry_run})
        result = build_container().close_expired_collections.execute(dry_run=dry_run)
        resumo = {"closed": result.closed, "cycle_ids": list(result.closed_cycle_ids), "dry_run": result.dry_run}
        log_event(logger, "resumo", encerrados=result.closed, ignorados=result.skipped, dry_run=result.dry_run,
                  ciclos=result.closed_cycle_ids)
        return resumo

    encerrar()


people_close_expired_collections()
