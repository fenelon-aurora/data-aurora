from __future__ import annotations

import pytest

from data_aurora.people.domain.entities import CloseExpiredResult, DeadlineReminderCycle, DeadlineRemindersResult
from data_aurora.people.services.close_expired_collections import CloseExpiredCollections
from data_aurora.people.services.send_deadline_reminders import SendDeadlineReminders
from data_aurora.shared.domain.entities import ServiceHealth
from data_aurora.shared.domain.errors import UpstreamError
from data_aurora.shared.services.healthcheck import HealthCheck


class FakePeople:
    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.dry_runs: list[bool] = []

    def health(self) -> ServiceHealth:
        return ServiceHealth("people", self.healthy, "" if self.healthy else "HTTP 503")

    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult:
        self.dry_runs.append(dry_run)
        return CloseExpiredResult(("c1",), skipped=0, dry_run=dry_run)

    def send_deadline_reminders(self, *, dry_run: bool, days_before: int) -> DeadlineRemindersResult:
        self.dry_runs.append(dry_run)
        return DeadlineRemindersResult(
            cycles=(
                DeadlineReminderCycle("c1", "e1", "2026-11-25T02:59:59.999Z", 2, 1, 0 if dry_run else 3),
                DeadlineReminderCycle("c2", "e1", "2026-11-25T02:59:59.999Z", 4, 0, already_reminded=True),
            ),
            days_before=days_before,
            dry_run=dry_run,
        )


def test_close_expired_repassa_dry_run():
    people = FakePeople()
    result = CloseExpiredCollections(people).execute(dry_run=True)
    assert people.dry_runs == [True]
    assert result.closed == 1 and result.dry_run is True


def test_healthcheck_falha_quando_servico_cai():
    assert HealthCheck([FakePeople()]).execute()[0].ok is True
    with pytest.raises(UpstreamError):
        HealthCheck([FakePeople(healthy=False)]).execute()


def test_lembrete_de_prazo_repassa_parametros_e_soma():
    people = FakePeople()
    result = SendDeadlineReminders(people).execute(dry_run=False, days_before=3)
    assert people.dry_runs == [False]
    assert result.sent == 3 and result.pending == 3 and result.days_before == 3
