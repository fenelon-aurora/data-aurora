from __future__ import annotations

import pytest
import requests

from data_aurora.people.adapters.people_http import (
    CLOSE_EXPIRED_PATH,
    DEADLINE_REMINDERS_PATH,
    SERVICE_KEY_HEADER,
    PeopleHttpAdapter,
)
from data_aurora.shared.domain.errors import UpstreamError


class FakeResponse:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body or {}

    def json(self) -> dict:
        return self._body


class FakeSession:
    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.calls: list[tuple[str, str, dict]] = []

    def _reply(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def get(self, url, **kwargs):
        return self._reply("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._reply("POST", url, **kwargs)


def adapter(session: FakeSession) -> PeopleHttpAdapter:
    return PeopleHttpAdapter("https://people.example/api/v1/", lambda: "segredo", 5, session=session)


def test_close_expired_envia_chave_e_le_resultado():
    session = FakeSession(FakeResponse(200, {"closed_cycle_ids": ["c1", "c2"], "skipped": 3, "dry_run": True}))
    result = adapter(session).close_expired_collections(dry_run=True)
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", f"https://people.example/api/v1{CLOSE_EXPIRED_PATH}")
    assert kwargs["headers"] == {SERVICE_KEY_HEADER: "segredo"}
    assert kwargs["json"] == {"dry_run": True}
    assert result.closed == 2 and result.skipped == 3 and result.dry_run is True


def test_close_expired_erro_http_vira_upstream_error():
    with pytest.raises(UpstreamError) as err:
        adapter(FakeSession(FakeResponse(404))).close_expired_collections(dry_run=False)
    assert err.value.status == 404


def test_close_expired_falha_de_rede_vira_upstream_error():
    with pytest.raises(UpstreamError):
        adapter(FakeSession(requests.ConnectionError("x"))).close_expired_collections(dry_run=False)


def test_health_nao_lanca_e_informa_falha():
    assert adapter(FakeSession(FakeResponse(200))).health().ok is True
    falha = adapter(FakeSession(requests.Timeout("x"))).health()
    assert falha.ok is False and falha.detail == "Timeout"


def test_lembrete_de_prazo_envia_parametros_e_le_ciclos():
    body = {
        "cycles": [{
            "cycle_id": "c1", "company_id": "e1", "deadline_at": "2026-11-25T02:59:59.999Z",
            "collaborators": 2, "managers": 1, "sent": 3, "skipped": 0, "already_reminded": False,
        }],
        "days_before": 3, "dry_run": False,
    }
    session = FakeSession(FakeResponse(200, body))
    result = adapter(session).send_deadline_reminders(dry_run=False, days_before=3)
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", f"https://people.example/api/v1{DEADLINE_REMINDERS_PATH}")
    assert kwargs["headers"] == {SERVICE_KEY_HEADER: "segredo"}
    assert kwargs["json"] == {"dry_run": False, "days_before": 3}
    assert result.sent == 3 and result.cycles[0].managers == 1 and result.dry_run is False


def test_lembrete_de_prazo_erro_http_vira_upstream_error():
    with pytest.raises(UpstreamError) as err:
        adapter(FakeSession(FakeResponse(500))).send_deadline_reminders(dry_run=False, days_before=3)
    assert err.value.status == 500
