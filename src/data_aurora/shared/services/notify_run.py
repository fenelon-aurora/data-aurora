"""Caso de uso: avisa o time por e-mail sobre o resultado de uma execução de DAG.

Falha sempre gera aviso. Sucesso segue `notify_success`: `sempre`, `com_mudancas` (só quando a
execução mudou algo, por exemplo encerrou ciclos) ou `nunca`. Erro ao avisar é registrado e
engolido: o aviso nunca derruba nem muda o estado da task.
"""
from __future__ import annotations

import html
import logging

from data_aurora.shared.domain.entities import EmailMessage, RunReport
from data_aurora.shared.domain.ports import EmailPort
from data_aurora.shared.logging import get_logger, log_event

logger = get_logger(__name__)

MODOS_SUCESSO = ("sempre", "com_mudancas", "nunca")


class NotifyRun:
    def __init__(self, email: EmailPort, recipients: tuple[str, ...], success_mode: str, environment: str) -> None:
        self._email = email
        self._recipients = recipients
        self._success_mode = success_mode if success_mode in MODOS_SUCESSO else "com_mudancas"
        self._environment = environment

    def should_notify(self, report: RunReport, changed: bool) -> bool:
        if report.status != "sucesso":
            return True
        return self._success_mode == "sempre" or (self._success_mode == "com_mudancas" and changed)

    def execute(self, report: RunReport, *, changed: bool = False) -> bool:
        if not self._recipients:
            log_event(logger, "aviso.ignorado", logging.WARNING, motivo="sem destinatários", dag_id=report.dag_id)
            return False
        if not self.should_notify(report, changed):
            log_event(logger, "aviso.ignorado", motivo=f"sucesso com modo {self._success_mode}", dag_id=report.dag_id)
            return False
        try:
            message_id = self._email.send(self.build_message(report))
            log_event(logger, "aviso.enviado", status=report.status, dag_id=report.dag_id, run_id=report.run_id,
                      destinatarios=len(self._recipients), message_id=message_id)
            return True
        except Exception as exc:  # noqa: BLE001 — aviso não pode derrubar a task
            log_event(logger, "aviso.erro", logging.ERROR, status=report.status, dag_id=report.dag_id,
                      erro=type(exc).__name__, mensagem=str(exc)[:300])
            return False

    def build_message(self, report: RunReport) -> EmailMessage:
        falha = report.status != "sucesso"
        marca = "FALHA" if falha else "OK"
        quando = report.logical_date[:16]
        subject = f"[Airflow {self._environment}] {marca} · {report.dag_id}.{report.task_id} · {quando}"
        linhas = [
            ("Status", report.status),
            ("DAG", report.dag_id),
            ("Task", report.task_id),
            ("Execução (run_id)", report.run_id),
            ("Data lógica", report.logical_date),
            ("Tentativa", f"{report.try_number} de {report.max_tries}"),
        ]
        if report.duration_seconds is not None:
            linhas.append(("Duração", f"{report.duration_seconds:.1f} s"))
        if falha:
            linhas += [("Erro", report.error_type or "—"), ("Mensagem", report.error_message or "—")]
        linhas += list(report.summary)
        if report.url:
            linhas.append(("Log da execução", report.url))
        text = "\n".join(f"{k}: {v}" for k, v in linhas)
        cor = "#b42318" if falha else "#067647"
        rows = "".join(
            f'<tr><td style="padding:4px 12px 4px 0;color:#555">{html.escape(k)}</td>'
            f'<td style="padding:4px 0"><b>{html.escape(str(v))}</b></td></tr>'
            for k, v in linhas
        )
        link = f'<p><a href="{html.escape(report.url)}">Abrir no Airflow</a></p>' if report.url else ""
        body_html = (
            f'<div style="font-family:Arial,sans-serif;font-size:14px">'
            f'<h2 style="color:{cor};margin:0 0 12px">{marca} · {html.escape(report.dag_id)}</h2>'
            f"<table>{rows}</table>{link}</div>"
        )
        return EmailMessage(to=self._recipients, subject=subject, text=text, html=body_html)
