from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from data_aurora.shared.adapters.ses_email import SesEmailAdapter
from data_aurora.shared.domain.entities import EmailMessage, RunReport
from data_aurora.shared.run_report import run_report_from_context
from data_aurora.shared.services.notify_run import NotifyRun
from data_aurora.shared.settings import load_settings

TIME = ("tech@aurorapeople.ai", "lucas.fenelon@aurorapeople.ai", "thais.plewka@aurorapeople.ai")


class FakeEmail:
    def __init__(self, falhar: bool = False) -> None:
        self.enviados: list[EmailMessage] = []
        self.falhar = falhar

    def send(self, message: EmailMessage) -> str:
        if self.falhar:
            raise RuntimeError("SES fora")
        self.enviados.append(message)
        return "msg-1"


def relatorio(status: str = "falha") -> RunReport:
    return RunReport(
        status=status, dag_id="people_close_expired_collections", task_id="encerrar", run_id="scheduled__x",
        logical_date="2026-11-23T21:15:00+00:00", try_number=3, max_tries=3, duration_seconds=1.5,
        error_type="UpstreamError" if status == "falha" else "", error_message="people: HTTP 500",
        summary=(("Ciclos encerrados", "2"),), url="https://pipeline.aurorapeople.ai/dags/x/grid",
    )


def test_destinatarios_padrao_sao_os_tres():
    assert load_settings().notify_recipients == TIME
    assert load_settings().notify_success == "com_mudancas"


def test_falha_sempre_avisa_com_erro_e_link():
    email = FakeEmail()
    assert NotifyRun(email, TIME, "nunca", "prod").execute(relatorio("falha")) is True
    msg = email.enviados[0]
    assert msg.to == TIME
    assert msg.subject.startswith("[Airflow prod] FALHA · people_close_expired_collections.encerrar")
    assert "Tentativa: 3 de 3" in msg.text and "Erro: UpstreamError" in msg.text
    assert "Mensagem: people: HTTP 500" in msg.text and "https://pipeline.aurorapeople.ai/dags/x/grid" in msg.html


def test_sucesso_segue_o_modo():
    email = FakeEmail()
    assert NotifyRun(email, TIME, "com_mudancas", "prod").execute(relatorio("sucesso"), changed=False) is False
    assert NotifyRun(email, TIME, "com_mudancas", "prod").execute(relatorio("sucesso"), changed=True) is True
    assert NotifyRun(email, TIME, "sempre", "prod").execute(relatorio("sucesso")) is True
    assert NotifyRun(email, TIME, "nunca", "prod").execute(relatorio("sucesso"), changed=True) is False
    assert len(email.enviados) == 2 and email.enviados[0].subject.startswith("[Airflow prod] OK")


def test_erro_no_envio_nao_propaga():
    assert NotifyRun(FakeEmail(falhar=True), TIME, "sempre", "prod").execute(relatorio()) is False


def test_ses_monta_a_chamada():
    chamadas = []
    client = SimpleNamespace(send_email=lambda **kw: chamadas.append(kw) or {"MessageId": "abc"})
    adapter = SesEmailAdapter("us-east-1", "Aurora Pipeline <noreply@aurorapeople.ai>", client=client)
    assert adapter.send(EmailMessage(TIME, "assunto", "texto", "<b>html</b>")) == "abc"
    kw = chamadas[0]
    assert kw["FromEmailAddress"] == "Aurora Pipeline <noreply@aurorapeople.ai>"
    assert kw["Destination"] == {"ToAddresses": list(TIME)}
    assert kw["Content"]["Simple"]["Body"]["Html"]["Data"] == "<b>html</b>"


def test_relatorio_a_partir_do_contexto_do_airflow():
    ti = SimpleNamespace(dag_id="d", task_id="t", run_id="r 1", try_number=2, max_tries=2, duration=3.25)
    ctx = {"ti": ti, "run_id": "r 1", "logical_date": datetime(2026, 11, 23, 21, 15, tzinfo=UTC),
           "exception": ValueError("quebrou")}
    rep = run_report_from_context(ctx, "falha", "https://pipeline.aurorapeople.ai")
    assert rep.max_tries == 3 and rep.error_type == "ValueError" and rep.error_message == "quebrou"
    assert rep.url == "https://pipeline.aurorapeople.ai/dags/d/grid?dag_run_id=r%201&task_id=t&tab=logs"
