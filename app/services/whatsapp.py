"""
WhatsApp Sender — HU-5.1

Sends messages via Twilio WhatsApp API or Meta Cloud API.
Abstracted behind a common interface so the orchestrator doesn't care
which provider is configured.

Usage:
    sender = WhatsAppSender.from_env()
    result = sender.send("whatsapp:+521234567890", "Hello!")
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class WhatsAppProvider(str, Enum):
    TWILIO = "twilio"
    META = "meta"
    MOCK = "mock"       # for testing


@dataclass
class SendResult:
    """Result of a WhatsApp send attempt."""
    success: bool
    provider: str
    recipient: str
    message_sid: Optional[str] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class WhatsAppConfig:
    provider: WhatsAppProvider = WhatsAppProvider.MOCK
    # Twilio
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from: str = "whatsapp:+14155238886"   # Twilio sandbox
    # Meta
    meta_phone_number_id: str = ""
    meta_access_token: str = ""
    # Recipients
    josue_whatsapp: str = ""
    tecnico_whatsapp: str = ""

    @classmethod
    def from_env(cls) -> WhatsAppConfig:
        return cls(
            provider=WhatsAppProvider(os.getenv("WHATSAPP_PROVIDER", "mock")),
            twilio_account_sid=os.getenv("TWILIO_ACCOUNT_SID", ""),
            twilio_auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
            twilio_from=os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886"),
            meta_phone_number_id=os.getenv("META_PHONE_NUMBER_ID", ""),
            meta_access_token=os.getenv("META_ACCESS_TOKEN", ""),
            josue_whatsapp=os.getenv("JOSUE_WHATSAPP", ""),
            tecnico_whatsapp=os.getenv("TECNICO_WHATSAPP", ""),
        )


# ============================================================
# Recipient resolution
# ============================================================

RECIPIENT_ALIASES = {
    "josue": "josue_whatsapp",
    "tecnico": "tecnico_whatsapp",
    "odoo_mantto": "tecnico_whatsapp",  # mantto goes to tecnico for now
}


class WhatsAppSender:
    """
    Send WhatsApp messages via configured provider.
    Supports Twilio, Meta Cloud API, and mock (for testing/dev).
    """

    def __init__(self, config: WhatsAppConfig):
        self.config = config
        self._send_log: list[SendResult] = []

    @classmethod
    def from_env(cls) -> WhatsAppSender:
        return cls(WhatsAppConfig.from_env())

    @property
    def send_log(self) -> list[SendResult]:
        return list(self._send_log)

    @property
    def total_sent(self) -> int:
        return sum(1 for r in self._send_log if r.success)

    @property
    def total_failed(self) -> int:
        return sum(1 for r in self._send_log if not r.success)

    def resolve_recipient(self, alias_or_number: str) -> str:
        """
        Resolve alias ('josue', 'tecnico') to phone number from config.
        If already a phone number (starts with 'whatsapp:'), return as-is.
        """
        if alias_or_number.startswith("whatsapp:") or alias_or_number.startswith("+"):
            return alias_or_number

        attr = RECIPIENT_ALIASES.get(alias_or_number.lower())
        if attr:
            number = getattr(self.config, attr, "")
            if number:
                return number

        logger.warning(f"Cannot resolve recipient: {alias_or_number}")
        return alias_or_number

    def send(self, recipient: str, message: str) -> SendResult:
        """Send a WhatsApp message. recipient can be alias or phone number."""
        resolved = self.resolve_recipient(recipient)

        if self.config.provider == WhatsAppProvider.TWILIO:
            result = self._send_twilio(resolved, message)
        elif self.config.provider == WhatsAppProvider.META:
            result = self._send_meta(resolved, message)
        else:
            result = self._send_mock(resolved, message)

        self._send_log.append(result)
        return result

    def send_to_recipients(self, recipients: list[str], message: str) -> list[SendResult]:
        """Send the same message to multiple recipients."""
        results = []
        seen = set()
        for r in recipients:
            resolved = self.resolve_recipient(r)
            if resolved in seen:
                continue
            seen.add(resolved)
            results.append(self.send(resolved, message))
        return results

    # ---- Provider implementations ----

    def _send_twilio(self, to: str, body: str) -> SendResult:
        """Send via Twilio WhatsApp API."""
        try:
            from twilio.rest import Client

            client = Client(
                self.config.twilio_account_sid,
                self.config.twilio_auth_token,
            )
            message = client.messages.create(
                from_=self.config.twilio_from,
                to=to,
                body=body,
            )
            logger.info(f"WhatsApp sent via Twilio: SID={message.sid} to={to}")
            return SendResult(
                success=True,
                provider="twilio",
                recipient=to,
                message_sid=message.sid,
            )
        except ImportError:
            logger.error("twilio package not installed — pip install twilio")
            return SendResult(
                success=False, provider="twilio", recipient=to,
                error="twilio package not installed",
            )
        except Exception as e:
            logger.error(f"Twilio send failed: {e}")
            return SendResult(
                success=False, provider="twilio", recipient=to,
                error=str(e),
            )

    def _send_meta(self, to: str, body: str) -> SendResult:
        """Send via Meta Cloud API."""
        try:
            import urllib.request
            import json

            # Strip whatsapp: prefix if present
            phone = to.replace("whatsapp:", "").replace("+", "")

            url = f"https://graph.facebook.com/v18.0/{self.config.meta_phone_number_id}/messages"
            payload = json.dumps({
                "messaging_product": "whatsapp",
                "to": phone,
                "type": "text",
                "text": {"body": body},
            }).encode()

            req = urllib.request.Request(
                url, data=payload,
                headers={
                    "Authorization": f"Bearer {self.config.meta_access_token}",
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                msg_id = data.get("messages", [{}])[0].get("id", "")

            logger.info(f"WhatsApp sent via Meta: id={msg_id} to={to}")
            return SendResult(
                success=True, provider="meta", recipient=to,
                message_sid=msg_id,
            )
        except Exception as e:
            logger.error(f"Meta send failed: {e}")
            return SendResult(
                success=False, provider="meta", recipient=to,
                error=str(e),
            )

    def _send_mock(self, to: str, body: str) -> SendResult:
        """Mock sender for testing — logs but doesn't send."""
        logger.info(f"[MOCK WhatsApp] to={to} body_len={len(body)}")
        return SendResult(
            success=True,
            provider="mock",
            recipient=to,
            message_sid=f"MOCK_{datetime.now(timezone.utc).strftime('%H%M%S')}",
        )
