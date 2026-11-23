"""Erros de domínio. Adaptadores traduzem falhas de infraestrutura para estes tipos."""
from __future__ import annotations


class DataAuroraError(Exception):
    """Base dos erros do pacote."""


class ConfigError(DataAuroraError):
    """Configuração obrigatória ausente ou inválida."""


class UpstreamError(DataAuroraError):
    """Serviço externo (People, IA, Grant) respondeu com erro ou não respondeu."""

    def __init__(self, service: str, message: str, status: int | None = None) -> None:
        super().__init__(f"{service}: {message}" + (f" (HTTP {status})" if status is not None else ""))
        self.service = service
        self.status = status
