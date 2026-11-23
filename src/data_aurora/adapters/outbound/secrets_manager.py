"""Leitura de segredos JSON no Secrets Manager (role da task do Airflow)."""
from __future__ import annotations

import json

from data_aurora.domain.errors import ConfigError
from data_aurora.utils.logging import get_logger

logger = get_logger(__name__)


class SecretsManagerAdapter:
    def __init__(self, region: str) -> None:
        self._region = region
        self._cache: dict[str, dict] = {}  # vale a vida da task; segredo não vai para disco

    def get_json(self, secret_id: str) -> dict:
        logger.info("Starting get_json")
        if secret_id in self._cache:
            return self._cache[secret_id]
        try:
            import boto3

            raw = boto3.client("secretsmanager", region_name=self._region).get_secret_value(SecretId=secret_id)
            data = json.loads(raw["SecretString"])
            if not isinstance(data, dict):
                raise ConfigError("segredo não é um objeto JSON")
            self._cache[secret_id] = data
            logger.info("Completed get_json")
            return data
        except ConfigError:
            raise
        except Exception as exc:
            # Nunca logar o valor do segredo; só o tipo do erro.
            logger.error("Error in get_json: %s", type(exc).__name__)
            raise ConfigError(f"não foi possível ler o segredo ({type(exc).__name__})") from exc
