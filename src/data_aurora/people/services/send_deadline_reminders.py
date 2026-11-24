"""Caso de uso: lembrete automático N dias antes do fim da coleta, a quem não concluiu a conversa.

Quem decide quem recebe é o People (ciclos em coleta com prazo perto, conversas não concluídas, um
envio por prazo). Este caso de uso só aciona a operação idempotente no horário e registra o resultado.
"""
from __future__ import annotations

from data_aurora.people.domain.entities import DeadlineRemindersResult
from data_aurora.people.domain.ports import PeoplePort
from data_aurora.shared.logging import etapa, get_logger, log_event

logger = get_logger(__name__)

DEFAULT_DAYS_BEFORE = 3


class SendDeadlineReminders:
    def __init__(self, people: PeoplePort) -> None:
        self._people = people

    def execute(self, *, dry_run: bool = False, days_before: int = DEFAULT_DAYS_BEFORE) -> DeadlineRemindersResult:
        with etapa(logger, "send_deadline_reminders", dry_run=dry_run, dias_antes=days_before) as fim:
            result = self._people.send_deadline_reminders(dry_run=dry_run, days_before=days_before)
            for c in result.cycles:
                acao = "lembrado"
                if c.already_reminded:
                    acao = "ja_lembrado"
                elif result.dry_run:
                    acao = "listado (dry_run)"
                log_event(
                    logger, "send_deadline_reminders.ciclo", ciclo_id=c.cycle_id, empresa_id=c.company_id,
                    prazo=c.deadline_at, colaboradores=c.collaborators, gestores=c.managers, enviados=c.sent,
                    falhas=c.skipped, acao=acao,
                )
            fim.update(ciclos=len(result.cycles), enviados=result.sent, pendentes=result.pending,
                       dry_run_servidor=result.dry_run)
            return result
