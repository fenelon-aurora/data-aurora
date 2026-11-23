"""Estruturas comuns a todos os domínios."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceHealth:
    service: str
    ok: bool
    detail: str = ""


@dataclass(frozen=True)
class EmailMessage:
    to: tuple[str, ...]
    subject: str
    text: str
    html: str = ""


@dataclass(frozen=True)
class RunReport:
    """O que aconteceu numa execução de task, para avisar o time."""

    status: str  # "sucesso" | "falha"
    dag_id: str
    task_id: str
    run_id: str
    logical_date: str
    try_number: int
    max_tries: int
    duration_seconds: float | None = None
    error_type: str = ""
    error_message: str = ""
    summary: tuple[tuple[str, str], ...] = ()
    url: str = ""
