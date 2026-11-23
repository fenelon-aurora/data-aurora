"""Portas comuns: segredos e health de qualquer serviço que as DAGs chamam."""
from __future__ import annotations

from typing import Protocol

from data_aurora.shared.domain.entities import EmailMessage, ServiceHealth


class SecretsPort(Protocol):
    def get_json(self, secret_id: str) -> dict: ...


class HealthPort(Protocol):
    def health(self) -> ServiceHealth: ...


class EmailPort(Protocol):
    def send(self, message: EmailMessage) -> str: ...
