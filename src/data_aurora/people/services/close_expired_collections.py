"""Caso de uso: encerra as coletas de ciclo com prazo vencido (D58, fase 3 do plano de prazo).

Quem decide o que está vencido é o People: este caso de uso só aciona a operação idempotente
do People no horário e registra o resultado. Rodar duas vezes não encerra nada a mais.
"""
from __future__ import annotations

from data_aurora.people.domain.entities import CloseExpiredResult
from data_aurora.people.domain.ports import PeoplePort
from data_aurora.shared.logging import etapa, get_logger, log_event

logger = get_logger(__name__)


class CloseExpiredCollections:
    def __init__(self, people: PeoplePort) -> None:
        self._people = people

    def execute(self, *, dry_run: bool = False) -> CloseExpiredResult:
        with etapa(logger, "close_expired_collections", dry_run=dry_run) as fim:
            result = self._people.close_expired_collections(dry_run=dry_run)
            for cycle_id in result.closed_cycle_ids:
                log_event(logger, "close_expired_collections.ciclo", ciclo_id=cycle_id,
                          acao="listado (dry_run)" if result.dry_run else "encerrado")
            fim.update(encerrados=result.closed, ignorados=result.skipped, dry_run_servidor=result.dry_run)
            return result
