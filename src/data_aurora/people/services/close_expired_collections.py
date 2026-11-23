"""Caso de uso: encerra as coletas de ciclo com prazo vencido (D58, fase 3 do plano de prazo).

Quem decide o que está vencido é o People: este caso de uso só aciona a operação idempotente
do People no horário e registra o resultado. Rodar duas vezes não encerra nada a mais.
"""
from __future__ import annotations

from data_aurora.people.domain.entities import CloseExpiredResult
from data_aurora.people.domain.ports import PeoplePort
from data_aurora.shared.logging import get_logger

logger = get_logger(__name__)


class CloseExpiredCollections:
    def __init__(self, people: PeoplePort) -> None:
        self._people = people

    def execute(self, *, dry_run: bool = False) -> CloseExpiredResult:
        logger.info("Starting CloseExpiredCollections dry_run=%s", dry_run)
        try:
            result = self._people.close_expired_collections(dry_run=dry_run)
            logger.info(
                "Completed CloseExpiredCollections closed=%s skipped=%s dry_run=%s",
                result.closed,
                result.skipped,
                result.dry_run,
            )
            return result
        except Exception as exc:
            logger.error("Error in CloseExpiredCollections: %s", exc)
            raise
