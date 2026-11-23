"""Porta do People: o que os casos de uso precisam da API, sem saber que é HTTP."""
from __future__ import annotations

from typing import Protocol

from data_aurora.people.domain.entities import CloseExpiredResult
from data_aurora.shared.domain.entities import ServiceHealth


class PeoplePort(Protocol):
    def health(self) -> ServiceHealth: ...

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult: ...
