from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(autouse=True)
def _limpa_settings(monkeypatch):
    from data_aurora.composition.settings import load_settings

    for key in ("DATA_ENVIRONMENT", "PEOPLE_API_URL", "PEOPLE_SERVICE_KEY_SECRET_ID", "HTTP_TIMEOUT_SECONDS"):
        monkeypatch.delenv(key, raising=False)
    load_settings.cache_clear()
    yield
    load_settings.cache_clear()
