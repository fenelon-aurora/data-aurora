"""Injeção de dependência: único lugar que conhece as implementações concretas."""
from __future__ import annotations

from dataclasses import dataclass

from data_aurora.people.adapters.people_http import PeopleHttpAdapter
from data_aurora.people.services.close_expired_collections import CloseExpiredCollections
from data_aurora.shared.adapters.secrets_manager import SecretsManagerAdapter
from data_aurora.shared.domain.errors import ConfigError
from data_aurora.shared.logging import get_logger
from data_aurora.shared.services.healthcheck import HealthCheck
from data_aurora.shared.settings import Settings, load_settings

logger = get_logger(__name__)


@dataclass(frozen=True)
class Container:
    settings: Settings
    healthcheck: HealthCheck
    close_expired_collections: CloseExpiredCollections


def build_container(settings: Settings | None = None) -> Container:
    logger.info("Starting build_container")
    try:
        settings = settings or load_settings()
        settings.require("people_api_url")
        secrets = SecretsManagerAdapter(settings.aws_region)

        def people_service_key() -> str:
            settings.require("people_service_key_secret_id")
            key = str(secrets.get_json(settings.people_service_key_secret_id).get("key", ""))
            if not key:
                raise ConfigError("segredo da chave de serviço do People sem o campo 'key'")
            return key

        people = PeopleHttpAdapter(settings.people_api_url, people_service_key, settings.http_timeout_seconds)
        container = Container(
            settings=settings,
            healthcheck=HealthCheck([people]),
            close_expired_collections=CloseExpiredCollections(people),
        )
        logger.info("Completed build_container")
        return container
    except Exception as exc:
        logger.error("Error in build_container: %s", exc)
        raise
