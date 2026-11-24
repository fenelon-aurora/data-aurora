"""Injeção de dependência: único lugar que conhece as implementações concretas."""
from __future__ import annotations

from dataclasses import dataclass

from data_aurora.people.adapters.people_http import PeopleHttpAdapter
from data_aurora.people.services.close_expired_collections import CloseExpiredCollections
from data_aurora.people.services.send_deadline_reminders import SendDeadlineReminders
from data_aurora.shared.adapters.secrets_manager import SecretsManagerAdapter
from data_aurora.shared.adapters.ses_email import SesEmailAdapter
from data_aurora.shared.domain.errors import ConfigError
from data_aurora.shared.logging import get_logger
from data_aurora.shared.services.healthcheck import HealthCheck
from data_aurora.shared.services.notify_run import NotifyRun
from data_aurora.shared.settings import Settings, load_settings

logger = get_logger(__name__)


@dataclass(frozen=True)
class Container:
    settings: Settings
    healthcheck: HealthCheck
    close_expired_collections: CloseExpiredCollections
    send_deadline_reminders: SendDeadlineReminders


def build_container(settings: Settings | None = None) -> Container:
    logger.info("Starting build_container")
    try:
        settings = settings or load_settings()
        settings.require("people_api_url")
        secrets = SecretsManagerAdapter(settings.aws_region)

        def people_service_key() -> str:
            settings.require("people_service_key_secret_id")
            secret = secrets.get_json(settings.people_service_key_secret_id)
            key = str(secret.get("provisioner_key") or secret.get("key") or "")
            if not key:
                raise ConfigError("segredo da chave de serviço do People sem o campo 'provisioner_key'")
            return key

        people = PeopleHttpAdapter(settings.people_api_url, people_service_key, settings.http_timeout_seconds)
        container = Container(
            settings=settings,
            healthcheck=HealthCheck([people]),
            close_expired_collections=CloseExpiredCollections(people),
            send_deadline_reminders=SendDeadlineReminders(people),
        )
        logger.info("Completed build_container")
        return container
    except Exception as exc:
        logger.error("Error in build_container: %s", exc)
        raise


def build_notifier(settings: Settings | None = None) -> NotifyRun:
    """Só o aviso por e-mail: não exige a configuração do People (usado nos callbacks da DAG)."""
    settings = settings or load_settings()
    return NotifyRun(
        SesEmailAdapter(settings.aws_region, settings.notify_sender),
        settings.notify_recipients,
        settings.notify_success,
        settings.environment,
    )
