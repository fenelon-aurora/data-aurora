"""Lembrete automático de prazo da coleta: todo dia nos últimos 3 dias, a quem ainda não concluiu a conversa.

Chama `POST /internal/cycles/send-deadline-reminders` do People, que escolhe os ciclos em coleta com
prazo (fim do ciclo ou da janela) daqui a até `days_before` dias de calendário em Brasília e envia o
lembrete (e-mails E-08 colaborador / E-09 gestor) a quem tem conversa não concluída. Um envio por dia
da contagem: faltam 3, 2, 1 e, no dia do prazo, "É hoje". A marca do prazo e do dia fica no log do ciclo,
então rodar de novo no mesmo dia não repete; prorrogar o prazo recomeça a contagem perto do novo prazo.
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
            "Lembretes enviados": resumo.get("sent", "—"),
            "Pessoas pendentes": resumo.get("pending", "—"),
            "Dias antes do prazo": resumo.get("days_before", "—"),
            "Dry run": resumo.get("dry_run", "—"),
            "Ciclos": ", ".join(resumo.get("cycle_ids", [])) or "—",
        } if status == "sucesso" else {}
        report = run_report_from_context(context, status, load_settings().airflow_base_url, linhas)
        build_notifier().execute(report, changed=bool(resumo.get("sent")))
    except Exception as exc:  # noqa: BLE001
        import logging

        logging.getLogger("airflow.task.data_aurora.dags").error("aviso.erro_callback | erro=%s", type(exc).__name__)


def _avisar_sucesso(context: dict) -> None:
    _avisar(context, "sucesso")


def _avisar_falha(context: dict) -> None:
    _avisar(context, "falha")


DEFAULT_ARGS = {
    "owner": "data-aurora",
    # Uma retentativa só: o People é idempotente por prazo, mas e-mail é efeito externo.
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": _avisar_falha,
    "on_success_callback": _avisar_sucesso,
}


@dag(
    dag_id="people_send_deadline_reminders",
    description="Lembrete de prazo da coleta 3 dias antes do fim, a quem não concluiu (People, D58)",
    # 12:00 UTC = 09:00 em Brasília: e-mail chega em horário comercial.
    schedule="0 12 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={
        "dry_run": Param(False, type="boolean", description="Só lista quem seria lembrado, sem enviar"),
        "days_before": Param(3, type="integer", minimum=1, maximum=30, description="Lembra todo dia a partir de N dias antes do prazo, até o dia do prazo"),
    },
    tags=["data-aurora", "people", "ciclo"],
)
def people_send_deadline_reminders():
    @task
    def lembrar(params: dict | None = None) -> dict:
        from airflow.exceptions import AirflowFailException

        from data_aurora.container import build_container
        from data_aurora.shared.domain.errors import ConfigError
        from data_aurora.shared.logging import get_logger, log_event, log_execucao

        logger = get_logger("dags.people_send_deadline_reminders")
        params = params or {}
        dry_run = bool(params.get("dry_run", False))
        days_before = int(params.get("days_before", 3))
        log_execucao(logger, {**_contexto_execucao(), "dry_run": dry_run, "dias_antes": days_before})
        try:
            result = build_container().send_deadline_reminders.execute(dry_run=dry_run, days_before=days_before)
        except ConfigError as exc:
            log_event(logger, "falha_sem_retentativa", motivo="configuracao", mensagem=str(exc))
            raise AirflowFailException(f"Configuração: {exc}") from exc
        resumo = {
            "sent": result.sent,
            "pending": result.pending,
            "days_before": result.days_before,
            "dry_run": result.dry_run,
            "cycle_ids": [c.cycle_id for c in result.cycles if not c.already_reminded],
        }
        log_event(logger, "resumo", enviados=result.sent, pendentes=result.pending, ciclos=len(result.cycles),
                  dias_antes=result.days_before, dry_run=result.dry_run)
        return resumo

    lembrar()


people_send_deadline_reminders()
