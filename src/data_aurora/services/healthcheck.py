"""Caso de uso: confere se o ambiente do Airflow alcança os serviços que as DAGs usam."""
from __future__ import annotations

from data_aurora.domain.entities import ServiceHealth
from data_aurora.domain.errors import UpstreamError
from data_aurora.domain.ports import PeoplePort
from data_aurora.utils.logging import get_logger

logger = get_logger(__name__)


class HealthCheck:
    def __init__(self, people: PeoplePort) -> None:
        self._people = people

    def execute(self) -> list[ServiceHealth]:
        logger.info("Starting HealthCheck")
        try:
            results = [self._people.health()]
            failed = [r for r in results if not r.ok]
            if failed:
                raise UpstreamError(failed[0].service, failed[0].detail or "health falhou")
            logger.info("Completed HealthCheck: %s", [r.service for r in results])
            return results
        except Exception as exc:
            logger.error("Error in HealthCheck: %s", exc)
            raise
