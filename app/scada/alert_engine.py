"""
Alert Engine — HU-4.4

Processes SCADA readings and alarm events, applies:
  - Threshold checks on analog values
  - Cooldown logic (15 min per variable+station+severity)
  - Escalation (WARNING unresolved 30 min → CRITICAL)
  - Recurrence tracking (>3/week → preventive maintenance order)

Outputs AlertAction objects for the WhatsApp/Odoo layer to handle.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.scada.tags import (
    AnalogTag,
    AlarmTag,
    Severity,
    COMPRESSOR_ANALOG_TAGS,
    COMPRESSOR_ALARM_TAGS,
)
from app.scada.plc_reader import ScadaReading, AlarmEvent

logger = logging.getLogger(__name__)

COOLDOWN_MINUTES = 15
ESCALATION_MINUTES = 30
RECURRENCE_THRESHOLD = 3   # >3 per week → maintenance order
RECURRENCE_WINDOW_DAYS = 7


@dataclass
class AlertAction:
    """Action to be taken by the notification layer."""
    alert_id: str
    station_id: int
    tag_id: str
    tag_name: str
    severity: Severity
    value: Optional[float] = None
    threshold: Optional[float] = None
    unit: str = ""
    message: str = ""
    recipients: list[str] = field(default_factory=list)   # "josue", "tecnico", "odoo_mantto"
    is_escalation: bool = False
    is_recurrence: bool = False
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_whatsapp_message(self) -> str:
        icon = {
            Severity.INFO: "ℹ️",
            Severity.WARNING: "⚠️",
            Severity.CRITICAL: "🚨",
            Severity.EMERGENCY: "🔴",
        }.get(self.severity, "❓")

        lines = [f"{icon} *ALERTA {self.severity.value}*"]
        if self.is_escalation:
            lines[0] += " (ESCALADA)"
        if self.is_recurrence:
            lines[0] += " ⟳ RECURRENTE"

        lines.append(f"📍 Estación {self.station_id}")
        lines.append(f"🔧 {self.tag_name}")

        if self.value is not None and self.threshold is not None:
            lines.append(f"📊 Valor: {self.value} {self.unit} (umbral: {self.threshold} {self.unit})")
        lines.append(f"💬 {self.message}")
        lines.append(f"⏰ {self.timestamp.strftime('%H:%M:%S UTC')}")

        return "\n".join(lines)


@dataclass
class _CooldownEntry:
    """Tracks cooldown and escalation state per alert."""
    tag_id: str
    severity: Severity
    station_id: int
    first_triggered: datetime
    last_triggered: datetime
    cooldown_until: datetime
    escalated: bool = False
    occurrence_count: int = 1


class AlertEngine:
    """
    Stateful alert engine that processes readings and events.

    Maintains cooldown state per tag+station+severity.
    Tracks escalation timers and recurrence windows.
    """

    def __init__(self, station_id: int = 3):
        self.station_id = station_id
        self._cooldowns: dict[str, _CooldownEntry] = {}   # key = tag_id:severity
        self._history: list[tuple[str, datetime]] = []      # (tag_id, timestamp) for recurrence
        self._tag_lookup = {t.tag_id: t for t in COMPRESSOR_ANALOG_TAGS}
        self._alarm_lookup = {t.tag_id: t for t in COMPRESSOR_ALARM_TAGS}

    def _cooldown_key(self, tag_id: str, severity: Severity) -> str:
        return f"{self.station_id}:{tag_id}:{severity.value}"

    def _is_cooled_down(self, tag_id: str, severity: Severity, now: datetime) -> bool:
        key = self._cooldown_key(tag_id, severity)
        entry = self._cooldowns.get(key)
        if entry is None:
            return False  # No cooldown = can fire
        return now < entry.cooldown_until

    def _set_cooldown(self, tag_id: str, severity: Severity, now: datetime):
        key = self._cooldown_key(tag_id, severity)
        existing = self._cooldowns.get(key)
        if existing:
            existing.last_triggered = now
            existing.cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
            existing.occurrence_count += 1
        else:
            self._cooldowns[key] = _CooldownEntry(
                tag_id=tag_id, severity=severity, station_id=self.station_id,
                first_triggered=now, last_triggered=now,
                cooldown_until=now + timedelta(minutes=COOLDOWN_MINUTES),
            )
        self._history.append((tag_id, now))

    def _check_escalation(self, tag_id: str, now: datetime) -> Optional[AlertAction]:
        """Check if a WARNING has been active >30 min without resolution."""
        warn_key = self._cooldown_key(tag_id, Severity.WARNING)
        entry = self._cooldowns.get(warn_key)
        if entry and not entry.escalated:
            elapsed = (now - entry.first_triggered).total_seconds() / 60
            if elapsed >= ESCALATION_MINUTES:
                entry.escalated = True
                tag_def = self._alarm_lookup.get(tag_id) or self._tag_lookup.get(tag_id)
                name = tag_def.name if tag_def else tag_id
                desc = tag_def.description if hasattr(tag_def, 'description') else f"{name} sin resolver"

                return AlertAction(
                    alert_id=f"ESC_{tag_id}_{now.strftime('%H%M%S')}",
                    station_id=self.station_id,
                    tag_id=tag_id, tag_name=name,
                    severity=Severity.CRITICAL,
                    message=f"⬆️ Escalada: {desc} — sin resolver por {int(elapsed)} min",
                    recipients=["josue", "tecnico"],
                    is_escalation=True,
                    timestamp=now,
                )
        return None

    def _check_recurrence(self, tag_id: str, now: datetime) -> bool:
        """Check if tag has triggered >3 times in past 7 days."""
        cutoff = now - timedelta(days=RECURRENCE_WINDOW_DAYS)
        count = sum(1 for tid, ts in self._history if tid == tag_id and ts >= cutoff)
        return count > RECURRENCE_THRESHOLD

    def _recipients_for_severity(self, severity: Severity) -> list[str]:
        if severity == Severity.EMERGENCY:
            return ["josue", "tecnico", "odoo_mantto"]
        elif severity == Severity.CRITICAL:
            return ["josue", "tecnico"]
        elif severity == Severity.WARNING:
            return ["josue"]
        return []

    # ============================================================
    # Process analog readings
    # ============================================================

    def process_readings(self, readings: list[ScadaReading]) -> list[AlertAction]:
        """Check analog readings against thresholds."""
        actions = []
        now = datetime.now(timezone.utc)

        for reading in readings:
            tag = self._tag_lookup.get(reading.tag_id)
            if not tag:
                continue

            val = reading.scaled_value

            # Check CRITICAL thresholds first
            severity = None
            threshold = None

            if tag.crit_high is not None and val >= tag.crit_high:
                severity = Severity.CRITICAL
                threshold = tag.crit_high
            elif tag.crit_low is not None and val <= tag.crit_low:
                severity = Severity.CRITICAL
                threshold = tag.crit_low
            elif tag.warn_high is not None and val >= tag.warn_high:
                severity = Severity.WARNING
                threshold = tag.warn_high
            elif tag.warn_low is not None and val <= tag.warn_low:
                severity = Severity.WARNING
                threshold = tag.warn_low

            if severity and not self._is_cooled_down(reading.tag_id, severity, now):
                is_recurrent = self._check_recurrence(reading.tag_id, now)
                self._set_cooldown(reading.tag_id, severity, now)

                direction = "alta" if (threshold and val >= threshold) else "baja"
                actions.append(AlertAction(
                    alert_id=f"ANA_{reading.tag_id}_{now.strftime('%H%M%S')}",
                    station_id=self.station_id,
                    tag_id=reading.tag_id, tag_name=tag.name,
                    severity=severity, value=val, threshold=threshold,
                    unit=tag.unit,
                    message=f"{tag.name} {direction}: {val} {tag.unit} (umbral: {threshold} {tag.unit})",
                    recipients=self._recipients_for_severity(severity),
                    is_recurrence=is_recurrent,
                    timestamp=now,
                ))

        # Check escalations
        for tag in COMPRESSOR_ANALOG_TAGS:
            esc = self._check_escalation(tag.tag_id, now)
            if esc:
                actions.append(esc)

        return actions

    # ============================================================
    # Process alarm events
    # ============================================================

    def process_alarms(self, events: list[AlarmEvent]) -> list[AlertAction]:
        """Convert alarm state changes to alert actions."""
        actions = []
        now = datetime.now(timezone.utc)

        for event in events:
            if not event.active:
                # Alarm cleared — could send "resolved" notification
                logger.info(f"Alarm cleared: {event.tag_id}")
                continue

            tag = self._alarm_lookup.get(event.tag_id)
            severity = event.severity
            if tag and self._is_cooled_down(event.tag_id, severity, now):
                continue

            is_recurrent = self._check_recurrence(event.tag_id, now)
            self._set_cooldown(event.tag_id, severity, now)

            recipients = self._recipients_for_severity(severity)
            if is_recurrent:
                recipients = list(set(recipients + ["odoo_mantto"]))

            actions.append(AlertAction(
                alert_id=f"ALM_{event.tag_id}_{now.strftime('%H%M%S')}",
                station_id=self.station_id,
                tag_id=event.tag_id, tag_name=event.tag_name,
                severity=severity,
                message=event.description,
                recipients=recipients,
                is_recurrence=is_recurrent,
                timestamp=now,
            ))

        # Check escalations for alarm tags
        for tag in COMPRESSOR_ALARM_TAGS:
            if tag.escalation_minutes > 0:
                esc = self._check_escalation(tag.tag_id, now)
                if esc:
                    actions.append(esc)

        return actions

    # ============================================================
    # Stats
    # ============================================================

    @property
    def active_cooldowns(self) -> int:
        now = datetime.now(timezone.utc)
        return sum(1 for e in self._cooldowns.values() if now < e.cooldown_until)

    @property
    def total_alerts_fired(self) -> int:
        return len(self._history)

    def get_recurrence_report(self) -> dict[str, int]:
        """Get count of alerts per tag in the recurrence window."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=RECURRENCE_WINDOW_DAYS)
        counts: dict[str, int] = defaultdict(int)
        for tag_id, ts in self._history:
            if ts >= cutoff:
                counts[tag_id] += 1
        return dict(sorted(counts.items(), key=lambda x: -x[1]))
