"""Cliente HTTP do People (serviço-a-serviço).

Rotas internas do People exigem o header `x-aurora-service-key` (mesmo padrão do
`x-aurora-provisioner-key` de `/internal/account-access/resolve`). A chave vem do Secrets Manager.
"""
from __future__ import annotations

from collections.abc import Callable

import requests

from data_aurora.domain.entities import CloseExpiredResult, ServiceHealth
from data_aurora.domain.errors import UpstreamError
from data_aurora.utils.logging import get_logger

logger = get_logger(__name__)

SERVICE = "people"
SERVICE_KEY_HEADER = "x-aurora-service-key"
CLOSE_EXPIRED_PATH = "/internal/cycles/close-expired-collections"


class PeopleHttpAdapter:
    def __init__(
        self,
        base_url: str,
        service_key: Callable[[], str],
        timeout_seconds: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._service_key = service_key
        self._timeout = timeout_seconds
        self._session = session or requests.Session()

    def health(self) -> ServiceHealth:
        logger.info("Starting people.health")
        try:
            resp = self._session.get(f"{self._base_url}/health", timeout=self._timeout)
            ok = resp.status_code == 200
            logger.info("Completed people.health status=%s", resp.status_code)
            return ServiceHealth(SERVICE, ok, "" if ok else f"HTTP {resp.status_code}")
        except requests.RequestException as exc:
            logger.error("Error in people.health: %s", type(exc).__name__)
            return ServiceHealth(SERVICE, False, type(exc).__name__)

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult:
        logger.info("Starting people.close_expired_collections dry_run=%s", dry_run)
        try:
            resp = self._session.post(
                f"{self._base_url}{CLOSE_EXPIRED_PATH}",
                json={"dry_run": dry_run},
                headers={SERVICE_KEY_HEADER: self._service_key()},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            logger.error("Error in people.close_expired_collections: %s", type(exc).__name__)
            raise UpstreamError(SERVICE, type(exc).__name__) from exc
        if resp.status_code != 200:
            logger.error("Error in people.close_expired_collections: HTTP %s", resp.status_code)
            raise UpstreamError(SERVICE, "close-expired-collections recusado", resp.status_code)
        body = resp.json()
        result = CloseExpiredResult(
            closed_cycle_ids=tuple(str(i) for i in body.get("closed_cycle_ids", [])),
            skipped=int(body.get("skipped", 0)),
            dry_run=bool(body.get("dry_run", dry_run)),
        )
        logger.info("Completed people.close_expired_collections closed=%s", result.closed)
        return result
