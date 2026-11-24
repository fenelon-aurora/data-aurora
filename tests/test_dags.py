"""Carrega as DAGs como o scheduler faz. Só roda onde o Airflow está instalado (CI de deploy)."""
from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_dags_carregam_sem_erro():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    assert {
        "data_aurora_healthcheck", "people_close_expired_collections", "people_send_deadline_reminders",
    } <= set(bag.dag_ids)
    fechar = bag.get_dag("people_close_expired_collections")
    assert fechar.max_active_runs == 1 and fechar.catchup is False
    tarefa = fechar.get_task("encerrar")
    assert tarefa.on_failure_callback and tarefa.on_success_callback
    lembrete = bag.get_dag("people_send_deadline_reminders")
    assert lembrete.max_active_runs == 1 and lembrete.catchup is False
    assert lembrete.params["days_before"] == 3
    assert lembrete.get_task("lembrar").on_failure_callback
