"""Monta o RunReport a partir do contexto que o Airflow passa aos callbacks.

Só lê chaves do dicionário de contexto (sem importar Airflow), para ser testável sem ele.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

from data_aurora.shared.domain.entities import RunReport


def run_report_from_context(
    context: dict[str, Any],
    status: str,
    base_url: str,
    summary: dict[str, Any] | None = None,
) -> RunReport:
    ti = context["ti"]
    run_id = str(context.get("run_id") or getattr(ti, "run_id", ""))
    logical_date = context.get("logical_date")
    exception = context.get("exception")
    duration = getattr(ti, "duration", None)
    url = (
        f"{base_url}/dags/{quote(ti.dag_id)}/grid?dag_run_id={quote(run_id)}&task_id={quote(ti.task_id)}&tab=logs"
        if base_url
        else ""
    )
    return RunReport(
        status=status,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=run_id,
        logical_date=logical_date.isoformat() if hasattr(logical_date, "isoformat") else str(logical_date or ""),
        try_number=int(getattr(ti, "try_number", 0) or 0),
        max_tries=int(getattr(ti, "max_tries", 0) or 0) + 1,
        duration_seconds=float(duration) if duration is not None else None,
        error_type=type(exception).__name__ if exception else "",
        error_message=str(exception)[:1000] if exception else "",
        summary=tuple((str(k), str(v)) for k, v in (summary or {}).items()),
        url=url,
    )
