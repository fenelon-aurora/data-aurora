from __future__ import annotations

import pytest

from data_aurora.composition.container import build_container
from data_aurora.composition.settings import load_settings
from data_aurora.domain.errors import ConfigError


def test_settings_por_ambiente(monkeypatch):
    monkeypatch.setenv("DATA_ENVIRONMENT", "DEV")
    monkeypatch.setenv("PEOPLE_API_URL", " https://people.example/api/v1 ")
    settings = load_settings()
    assert settings.environment == "dev"
    assert settings.people_api_url == "https://people.example/api/v1"
    assert settings.http_timeout_seconds == 30.0


def test_ambiente_invalido(monkeypatch):
    monkeypatch.setenv("DATA_ENVIRONMENT", "staging")
    with pytest.raises(ConfigError):
        load_settings()


def test_container_exige_url_do_people():
    with pytest.raises(ConfigError):
        build_container()


def test_container_monta_sem_ler_segredo(monkeypatch):
    # O segredo só é lido quando a rota interna é chamada, não ao montar o container.
    monkeypatch.setenv("PEOPLE_API_URL", "https://people.example/api/v1")
    container = build_container()
    assert container.settings.people_service_key_secret_id == ""
