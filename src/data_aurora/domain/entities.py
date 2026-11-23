"""Estruturas imutáveis trocadas entre casos de uso e portas."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ServiceHealth:
    service: str
    ok: bool
    detail: str = ""


@dataclass(frozen=True)
class CloseExpiredResult:
    """Resultado do encerramento automático de coletas vencidas (fase 3 do plano de prazo, D58)."""

    closed_cycle_ids: tuple[str, ...] = field(default_factory=tuple)
    skipped: int = 0
    dry_run: bool = False

    @property
    def closed(self) -> int:
        return len(self.closed_cycle_ids)
