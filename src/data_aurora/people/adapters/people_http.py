"""Cliente HTTP do People (serviço-a-serviço).

Rotas `/internal/*` do People exigem a chave de serviço no header `x-aurora-provisioner-key` (a mesma
que o Grant usa). A chave vem do Secrets Manager (`grant/<env>/people-provisioner`).
Cada chamada gera uma linha `people.http` com método, rota, status e latência.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from urllib.parse import urlparse

import requests

from data_aurora.people.domain.entities import CloseExpiredResult, DeadlineReminderCycle, DeadlineRemindersResult
from data_aurora.shared.domain.entities import ServiceHealth
from data_aurora.shared.domain.errors import UpstreamError
from data_aurora.shared.logging import get_logger, log_event

logger = get_logger(__name__)

SERVICE = "people"
SERVICE_KEY_HEADER = "x-aurora-provisioner-key"
CLOSE_EXPIRED_PATH = "/internal/cycles/close-expired-collections"
DEADLINE_REMINDERS_PATH = "/internal/cycles/send-deadline-reminders"


class PeopleHttpAdapter:
    def __init__(
        self,
        base_url: str,
        service_key: Callable[[], str],
        timeout_seconds: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._host = urlparse(self._base_url).netloc or self._base_url
        self._service_key = service_key
        self._timeout = timeout_seconds
        self._session = session or requests.Session()

    def _call(self, method: str, path: str, **kwargs) -> requests.Response:
        inicio = time.monotonic()
        try:
            resp = getattr(self._session, method.lower())(
                f"{self._base_url}{path}", timeout=self._timeout, **kwargs
            )
        except requests.RequestException as exc:
            log_event(
                logger, "people.http", logging.ERROR, metodo=method, host=self._host, rota=path,
                erro=type(exc).__name__, latencia_ms=(time.monotonic() - inicio) * 1000, timeout_s=self._timeout,
            )
            raise
        log_event(
            logger, "people.http", logging.INFO if resp.status_code < 400 else logging.ERROR,
            metodo=method, host=self._host, rota=path, status=resp.status_code,
            latencia_ms=(time.monotonic() - inicio) * 1000,
        )
        return resp

    def health(self) -> ServiceHealth:
        try:
            resp = self._call("GET", "/health")
        except requests.RequestException as exc:
            return ServiceHealth(SERVICE, False, type(exc).__name__)
        ok = resp.status_code == 200
        return ServiceHealth(SERVICE, ok, "" if ok else f"HTTP {resp.status_code}")

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult:
        try:
            resp = self._call(
                "POST", CLOSE_EXPIRED_PATH, json={"dry_run": dry_run}, headers={SERVICE_KEY_HEADER: self._service_key()}
            )
        except requests.RequestException as exc:
            raise UpstreamError(SERVICE, type(exc).__name__) from exc
        if resp.status_code != 200:
            raise UpstreamError(SERVICE, f"{CLOSE_EXPIRED_PATH} recusado", resp.status_code)
        body = resp.json()
        return CloseExpiredResult(
            closed_cycle_ids=tuple(str(i) for i in body.get("closed_cycle_ids", [])),
            skipped=int(body.get("skipped", 0)),
            dry_run=bool(body.get("dry_run", dry_run)),
        )

    def send_deadline_reminders(self, *, dry_run: bool, days_before: int) -> DeadlineRemindersResult:
        try:
            resp = self._call(
                "POST", DEADLINE_REMINDERS_PATH, json={"dry_run": dry_run, "days_before": days_before},
                headers={SERVICE_KEY_HEADER: self._service_key()},
            )
        except requests.RequestException as exc:
            raise UpstreamError(SERVICE, type(exc).__name__) from exc
        if resp.status_code != 200:
            raise UpstreamError(SERVICE, f"{DEADLINE_REMINDERS_PATH} recusado", resp.status_code)
        body = resp.json()
        return DeadlineRemindersResult(
            cycles=tuple(
                DeadlineReminderCycle(
                    cycle_id=str(c.get("cycle_id", "")),
                    company_id=str(c.get("company_id", "")),
                    deadline_at=str(c.get("deadline_at", "")),
                    collaborators=int(c.get("collaborators", 0)),
                    managers=int(c.get("managers", 0)),
                    sent=int(c.get("sent", 0)),
                    skipped=int(c.get("skipped", 0)),
                    already_reminded=bool(c.get("already_reminded", False)),
                )
                for c in body.get("cycles", [])
            ),
            days_before=int(body.get("days_before", days_before)),
            dry_run=bool(body.get("dry_run", dry_run)),
        )
