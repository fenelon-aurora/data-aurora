"""Portas (interfaces) que os casos de uso usam. Implementações ficam em adapters/outbound."""
from __future__ import annotations

from typing import Protocol

from data_aurora.domain.entities import CloseExpiredResult, ServiceHealth


class SecretsPort(Protocol):
    def get_json(self, secret_id: str) -> dict: ...


class PeoplePort(Protocol):
    def health(self) -> ServiceHealth: ...

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult: ...
