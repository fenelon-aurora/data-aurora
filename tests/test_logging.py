from __future__ import annotations

import logging

import pytest

from data_aurora.people.domain.entities import CloseExpiredResult
from data_aurora.people.services.close_expired_collections import CloseExpiredCollections
from data_aurora.shared.logging import etapa, get_logger, log_event


class FakePeople:
    def close_expired_collections(self, *, dry_run: bool) -> CloseExpiredResult:
        return CloseExpiredResult(("c1", "c2"), skipped=1, dry_run=dry_run)


def test_logger_fica_sob_airflow_task():
    assert get_logger("data_aurora.people.services.x").name == "airflow.task.data_aurora.people.services.x"


def test_caso_de_uso_registra_inicio_ciclos_e_fim(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task.data_aurora")
    CloseExpiredCollections(FakePeople()).execute(dry_run=False)
    linhas = [r.getMessage() for r in caplog.records]
    assert linhas[0] == "close_expired_collections.inicio | dry_run=False"
    assert "close_expired_collections.ciclo | ciclo_id=c1 acao=encerrado" in linhas
    fim = linhas[-1]
    assert fim.startswith("close_expired_collections.fim | dry_run=False encerrados=2 ignorados=1")
    assert "duracao_ms=" in fim


def test_erro_registra_tipo_e_repropaga(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task.data_aurora")
    logger = get_logger("data_aurora.teste")
    with pytest.raises(ValueError), etapa(logger, "passo", alvo="x"):
        raise ValueError("falhou")
    erro = caplog.records[-1]
    assert erro.levelno == logging.ERROR
    assert erro.getMessage().startswith("passo.erro | alvo=x erro=ValueError mensagem=falhou")


def test_mascara_campos_sensiveis(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task.data_aurora")
    log_event(get_logger("data_aurora.teste"), "evento", api_key="abc", segredo_configurado=True, secret_id="data/x")
    assert caplog.records[-1].getMessage() == "evento | api_key=*** segredo_configurado=True secret_id=data/x"
