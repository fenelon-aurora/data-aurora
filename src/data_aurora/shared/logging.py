"""Logs do pacote, pensados para leitura na aba Logs de cada task do Airflow.

- Os loggers ficam sob `airflow.task`, então tudo aparece no log da task (e no S3 de logs remotos),
  sem depender de como o Airflow configura o logger raiz.
- `log_event` grava uma linha por evento no formato `evento | chave=valor ...`, fácil de ler e de
  filtrar (CloudWatch/S3). Valores sensíveis são mascarados pelo nome da chave.
- `etapa` mede e registra início, fim (com duração) e falha (com tipo do erro) de um bloco.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_RAIZ = "airflow.task.data_aurora"
_SENSIVEIS = ("key", "token", "secret", "password", "senha", "authorization")


def get_logger(name: str) -> logging.Logger:
    sufixo = name.removeprefix("data_aurora").lstrip(".")
    return logging.getLogger(f"{_RAIZ}.{sufixo}" if sufixo else _RAIZ)


def _formatar(valor: Any) -> str:
    if isinstance(valor, float):
        return f"{valor:.1f}"
    if isinstance(valor, (list, tuple, set)):
        itens = list(valor)
        if len(itens) > 20:
            return "[" + ",".join(map(str, itens[:20])) + f",…+{len(itens) - 20}]"
        return "[" + ",".join(map(str, itens)) + "]"
    texto = str(valor)
    return f'"{texto}"' if " " in texto else texto


def log_event(logger: logging.Logger, evento: str, level: int = logging.INFO, **campos: Any) -> None:
    partes = []
    for chave, valor in campos.items():
        if valor is None:
            continue
        if any(s in chave.lower() for s in _SENSIVEIS) and not chave.lower().endswith(("_configurado", "_id")):
            valor = "***"
        partes.append(f"{chave}={_formatar(valor)}")
    logger.log(level, "%s | %s", evento, " ".join(partes)) if partes else logger.log(level, "%s", evento)


@contextmanager
def etapa(logger: logging.Logger, nome: str, **campos: Any) -> Iterator[dict]:
    """Registra `nome.inicio`, `nome.fim` (duracao_ms) ou `nome.erro` (erro, duracao_ms).

    O dicionário devolvido aceita campos extras que entram na linha de fim.
    """
    extras: dict[str, Any] = {}
    inicio = time.monotonic()
    log_event(logger, f"{nome}.inicio", **campos)
    try:
        yield extras
    except Exception as exc:
        log_event(
            logger,
            f"{nome}.erro",
            logging.ERROR,
            **campos,
            erro=type(exc).__name__,
            mensagem=str(exc)[:300],
            duracao_ms=(time.monotonic() - inicio) * 1000,
        )
        raise
    log_event(logger, f"{nome}.fim", **campos, **extras, duracao_ms=(time.monotonic() - inicio) * 1000)


def log_execucao(logger: logging.Logger, contexto: dict[str, Any]) -> None:
    """Primeira linha de toda task: identifica a execução (dag, run, data lógica, tentativa, params)."""
    log_event(logger, "execucao", **contexto)
