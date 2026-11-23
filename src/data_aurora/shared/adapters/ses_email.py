"""Envio de e-mail pelo Amazon SES (v2), com o domínio verificado aurorapeople.ai."""
from __future__ import annotations

import logging
import time

from data_aurora.shared.domain.entities import EmailMessage
from data_aurora.shared.logging import get_logger, log_event

logger = get_logger(__name__)


class SesEmailAdapter:
    def __init__(self, region: str, sender: str, client=None) -> None:
        self._region = region
        self._sender = sender
        self._client = client

    def _ses(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("sesv2", region_name=self._region)
        return self._client

    def send(self, message: EmailMessage) -> str:
        inicio = time.monotonic()
        body = {"Text": {"Data": message.text, "Charset": "UTF-8"}}
        if message.html:
            body["Html"] = {"Data": message.html, "Charset": "UTF-8"}
        try:
            resp = self._ses().send_email(
                FromEmailAddress=self._sender,
                Destination={"ToAddresses": list(message.to)},
                Content={"Simple": {"Subject": {"Data": message.subject, "Charset": "UTF-8"}, "Body": body}},
            )
        except Exception as exc:
            log_event(logger, "ses.envio", logging.ERROR, destinatarios=len(message.to), erro=type(exc).__name__,
                      mensagem=str(exc)[:300], latencia_ms=(time.monotonic() - inicio) * 1000)
            raise
        message_id = str(resp.get("MessageId", ""))
        log_event(logger, "ses.envio", destinatarios=len(message.to), message_id=message_id,
                  latencia_ms=(time.monotonic() - inicio) * 1000)
        return message_id
