from __future__ import annotations

import pytest

from data_aurora.domain.entities import CloseExpiredResult, ServiceHealth
from data_aurora.domain.errors import UpstreamError
from data_aurora.services.close_expired_collections import CloseExpiredCollections
from data_aurora.services.healthcheck import HealthCheck


class FakePeople:
    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.dry_runs: list[bool] = []

    def health(self) -> ServiceHealth:
        return ServiceHealth("people", self.healthy, "" if self.healthy else "HTTP 503")

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult:
        self.dry_runs.append(dry_run)
        return CloseExpiredResult(("c1",), skipped=0, dry_run=dry_run)


def test_close_expired_repassa_dry_run():
    people = FakePeople()
    result = CloseExpiredCollections(people).execute(dry_run=True)
    assert people.dry_runs == [True]
    assert result.closed == 1 and result.dry_run is True


def test_healthcheck_falha_quando_servico_cai():
    assert HealthCheck(FakePeople()).execute()[0].ok is True
    with pytest.raises(UpstreamError):
        HealthCheck(FakePeople(healthy=False)).execute()
