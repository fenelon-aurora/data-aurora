"""Estruturas comuns a todos os domínios."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceHealth:
    service: str
    ok: bool
    detail: str = ""
