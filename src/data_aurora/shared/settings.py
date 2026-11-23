"""Configuração do pacote, lida uma vez por processo.

Ordem de leitura de cada chave: variável de ambiente (task definition do ECS, via iac-aurora) e,
se ausente, Airflow Variable `data_aurora_<chave em minúsculas>` (editável na UI, Admin → Variables).
Nunca guarde segredo em Variable: só o ARN/nome do segredo.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from data_aurora.shared.domain.errors import ConfigError
from data_aurora.shared.logging import get_logger

logger = get_logger(__name__)

ENVIRONMENTS = ("dev", "demo", "prod")


def _airflow_variable(name: str) -> str | None:
    try:
        from airflow.models import Variable  # import tardio: o pacote não depende do Airflow
    except ImportError:
        return None
    try:
        return Variable.get(name, default_var=None)
    except Exception as exc:  # noqa: BLE001 — sem metadata DB (testes, parse local)
        logger.error("Error reading Airflow Variable %s: %s", name, type(exc).__name__)
        return None


def read_config(key: str, default: str = "") -> str:
    value = os.getenv(key)
    if value is None or not value.strip():
        value = _airflow_variable(f"data_aurora_{key.lower()}")
    return (value or default).strip()


@dataclass(frozen=True)
class Settings:
    environment: str
    aws_region: str
    people_api_url: str
    people_service_key_secret_id: str
    http_timeout_seconds: float

    def require(self, *fields: str) -> None:
        missing = [f for f in fields if not getattr(self, f)]
        if missing:
            raise ConfigError(f"configuração ausente: {', '.join(missing)}")


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    logger.info("Starting load_settings")
    try:
        environment = read_config("DATA_ENVIRONMENT", "prod").lower()
        if environment not in ENVIRONMENTS:
            raise ConfigError(f"DATA_ENVIRONMENT inválido: {environment}")
        settings = Settings(
            environment=environment,
            aws_region=read_config("AWS_REGION", "us-east-1"),
            people_api_url=read_config("PEOPLE_API_URL"),
            people_service_key_secret_id=read_config("PEOPLE_SERVICE_KEY_SECRET_ID"),
            http_timeout_seconds=float(read_config("HTTP_TIMEOUT_SECONDS", "30")),
        )
        logger.info("Completed load_settings environment=%s", settings.environment)
        return settings
    except Exception as exc:
        logger.error("Error in load_settings: %s", exc)
        raise
