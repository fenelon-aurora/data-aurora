"""Caso de uso: confere se o ambiente do Airflow alcança os serviços que as DAGs usam."""
from __future__ import annotations

import logging
from collections.abc import Sequence

from data_aurora.shared.domain.entities import ServiceHealth
from data_aurora.shared.domain.errors import UpstreamError
from data_aurora.shared.domain.ports import HealthPort
from data_aurora.shared.logging import etapa, get_logger, log_event

logger = get_logger(__name__)


class HealthCheck:
    def __init__(self, services: Sequence[HealthPort]) -> None:
        self._services = tuple(services)

    def execute(self) -> list[ServiceHealth]:
        with etapa(logger, "healthcheck", servicos=len(self._services)) as fim:
            results = [service.health() for service in self._services]
            for r in results:
                log_event(logger, "healthcheck.servico", logging.INFO if r.ok else logging.ERROR,
                          servico=r.service, ok=r.ok, detalhe=r.detail or None)
            failed = [r for r in results if not r.ok]
            fim.update(ok=len(results) - len(failed), falhas=len(failed))
            if failed:
                raise UpstreamError(failed[0].service, failed[0].detail or "health falhou")
            return results
