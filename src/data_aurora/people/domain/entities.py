"""Estruturas do domínio People."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CloseExpiredResult:
    """Resultado do encerramento automático de coletas vencidas (fase 3 do plano de prazo, D58)."""

    closed_cycle_ids: tuple[str, ...] = field(default_factory=tuple)
    skipped: int = 0
    dry_run: bool = False

    @property
    def closed(self) -> int:
        return len(self.closed_cycle_ids)


@dataclass(frozen=True)
class DeadlineReminderCycle:
    """Um ciclo (e prazo) com lembrete automático: quantos pendentes e quantos e-mails saíram."""

    cycle_id: str
    company_id: str
    deadline_at: str
    collaborators: int = 0
    managers: int = 0
    sent: int = 0
    skipped: int = 0
    already_reminded: bool = False


@dataclass(frozen=True)
class DeadlineRemindersResult:
    """Resultado do lembrete automático de prazo (N dias antes do fim da coleta)."""

    cycles: tuple[DeadlineReminderCycle, ...] = field(default_factory=tuple)
    days_before: int = 3
    dry_run: bool = False

    @property
    def sent(self) -> int:
        return sum(c.sent for c in self.cycles)

    @property
    def pending(self) -> int:
        return sum(c.collaborators + c.managers for c in self.cycles if not c.already_reminded)
