"""Configuração do pacote, lida uma vez por processo.

Ordem de leitura de cada chave: variável de ambiente (task definition do ECS, via iac-aurora) e,
se ausente, Airflow Variable `data_aurora_<chave em minúsculas>` (editável na UI, Admin → Variables).
Nunca guarde segredo em Variable: só o ARN/nome do segredo.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse

from data_aurora.shared.domain.errors import ConfigError
from data_aurora.shared.logging import get_logger, log_event

logger = get_logger(__name__)

ENVIRONMENTS = ("dev", "demo", "prod")
DEFAULT_RECIPIENTS = "tech@aurorapeople.ai,lucas.fenelon@aurorapeople.ai,thais.plewka@aurorapeople.ai"


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
    value, fonte = os.getenv(key), "env"
    if value is None or not value.strip():
        value, fonte = _airflow_variable(f"data_aurora_{key.lower()}"), "airflow_variable"
    if value is None or not str(value).strip():
        value, fonte = default, "padrao" if default else "ausente"
    log_event(logger, "settings.fonte", chave=key, fonte=fonte)
    return str(value).strip()


@dataclass(frozen=True)
class Settings:
    environment: str
    aws_region: str
    people_api_url: str
    people_service_key_secret_id: str
    http_timeout_seconds: float
    notify_recipients: tuple[str, ...]
    notify_sender: str
    notify_success: str
    airflow_base_url: str

    def require(self, *fields: str) -> None:
        missing = [f for f in fields if not getattr(self, f)]
        if missing:
            raise ConfigError(f"configuração ausente: {', '.join(missing)}")


@lru_cache(maxsize=1)
def load_settings() -> Settings:
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
            notify_recipients=tuple(
                e.strip() for e in read_config("NOTIFY_RECIPIENTS", DEFAULT_RECIPIENTS).split(",") if e.strip()
            ),
            notify_sender=read_config("NOTIFY_SENDER", "Aurora Pipeline <noreply@aurorapeople.ai>"),
            notify_success=read_config("NOTIFY_SUCCESS", "com_mudancas").lower(),
            airflow_base_url=read_config("AIRFLOW_BASE_URL", "https://pipeline.aurorapeople.ai").rstrip("/"),
        )
        log_event(
            logger,
            "settings.carregadas",
            ambiente=settings.environment,
            regiao=settings.aws_region,
            people_host=urlparse(settings.people_api_url).netloc or None,
            people_segredo_configurado=bool(settings.people_service_key_secret_id),
            timeout_s=settings.http_timeout_seconds,
            avisos_destinatarios=len(settings.notify_recipients),
            avisos_sucesso=settings.notify_success,
        )
        return settings
    except Exception as exc:
        log_event(logger, "settings.erro", logging.ERROR, erro=type(exc).__name__, mensagem=str(exc))
        raise
