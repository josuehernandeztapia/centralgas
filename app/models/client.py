"""
Client Retention & Inactivity Models — HU-6.5

Data models for tracking client behavior, detecting churn risk,
and generating retention actions.

The 331+ vagonetas of Grupo Estación Central Gas are the core business.
Losing one vehicle ≈ $5,000-$8,000 MXN/month in revenue.

Segmentation:
  VAGONETA  — camionetas de carga, 80%+ de clientes
  TAXI      — taxis convertidos a GNC
  PARTICULAR — vehículos particulares

Tendencia (trend classification):
  NUEVO_2025     — first charge ≤90 days ago
  CRECIENDO      — monthly litros increasing >10% MoM
  ESTABLE        — within ±10% of 3-month average
  BAJANDO        — monthly litros decreasing >20% MoM
  PERDIDO_2025   — no charge in 30+ days (active client) or 60+ days (any)

Churn stages:
  GREEN   — healthy, charging regularly
  YELLOW  — at risk: dias_sin_cargar > threshold or consumption dropping
  ORANGE  — high risk: significant inactivity or sharp consumption drop
  RED     — churned: no activity beyond recovery window

Alert types:
  INACTIVITY     — days without charge exceeds threshold
  CONSUMPTION_DROP — monthly volume drop exceeds threshold
  FREQUENCY_DROP — charging frequency declining
  RECOVERY       — inactive client returns (positive alert)
  NEW_CLIENT     — first charge detected
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional


# ============================================================
# Enums
# ============================================================

class Segmento(str, Enum):
    VAGONETA = "VAGONETA"
    TAXI = "TAXI"
    PARTICULAR = "PARTICULAR"


class Tendencia(str, Enum):
    NUEVO_2025 = "NUEVO_2025"
    CRECIENDO = "CRECIENDO"
    ESTABLE = "ESTABLE"
    BAJANDO = "BAJANDO"
    PERDIDO_2025 = "PERDIDO_2025"


class ChurnStage(str, Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ORANGE = "ORANGE"
    RED = "RED"


class RetentionAlertType(str, Enum):
    INACTIVITY = "INACTIVITY"
    CONSUMPTION_DROP = "CONSUMPTION_DROP"
    FREQUENCY_DROP = "FREQUENCY_DROP"
    RECOVERY = "RECOVERY"
    NEW_CLIENT = "NEW_CLIENT"


# ============================================================
# Thresholds — configurable per-segment
# ============================================================

@dataclass
class RetentionThresholds:
    """
    Configurable thresholds for churn detection.
    Calibrated from real Central Gas data:
      - Average vagoneta charges every 3-4 days
      - Taxi charges every 1-2 days
      - Particular charges every 5-7 days
    """
    # Inactivity (days without charge)
    yellow_days: int = 7          # 1 week — unusual
    orange_days: int = 15         # 2 weeks — at risk
    red_days: int = 30            # 1 month — likely churned

    # Consumption drop (% month-over-month)
    consumption_drop_warn: float = 20.0    # 20% drop → YELLOW
    consumption_drop_crit: float = 50.0    # 50% drop → ORANGE

    # Frequency drop (charges per week)
    frequency_drop_warn: float = 30.0      # 30% fewer charges
    frequency_drop_crit: float = 60.0      # 60% fewer charges

    # New client window (days since first charge)
    new_client_days: int = 90

    # Recovery window: client returns after being RED
    recovery_min_days: int = 30   # was inactive ≥30 days

    @classmethod
    def for_segment(cls, segmento: Segmento) -> RetentionThresholds:
        """Segment-specific thresholds."""
        if segmento == Segmento.TAXI:
            # Taxis charge more frequently
            return cls(
                yellow_days=4,
                orange_days=7,
                red_days=14,
                consumption_drop_warn=15.0,
                consumption_drop_crit=40.0,
            )
        elif segmento == Segmento.PARTICULAR:
            # Particulares charge less frequently
            return cls(
                yellow_days=14,
                orange_days=21,
                red_days=45,
                consumption_drop_warn=30.0,
                consumption_drop_crit=60.0,
            )
        else:
            # Vagoneta — default
            return cls()


# ============================================================
# Client Profile — in-memory snapshot for analysis
# ============================================================

@dataclass
class MonthlyStats:
    """Monthly aggregation for one client at one station."""
    year: int
    month: int
    num_cargas: int = 0
    total_litros: Decimal = Decimal("0")
    total_mxn: Decimal = Decimal("0")
    station_ids: set = field(default_factory=set)
    avg_litros_per_carga: Decimal = Decimal("0")

    @property
    def period_key(self) -> str:
        return f"{self.year}-{self.month:02d}"


@dataclass
class ClientProfile:
    """
    In-memory snapshot of a client's behavior.
    Built from transaction history for analysis.
    """
    placa: str
    segmento: Segmento = Segmento.VAGONETA
    estatus: str = "ACTIVO"

    # First / last activity
    primera_carga: Optional[datetime] = None
    ultima_carga: Optional[datetime] = None

    # Current state
    dias_sin_cargar: int = 0
    tendencia: Optional[Tendencia] = None
    churn_stage: ChurnStage = ChurnStage.GREEN

    # Aggregates
    total_cargas: int = 0
    total_litros: Decimal = Decimal("0")
    total_mxn: Decimal = Decimal("0")

    # Monthly history (last N months)
    monthly_stats: list[MonthlyStats] = field(default_factory=list)

    # Station affinity
    eds_principal: Optional[str] = None
    station_distribution: dict[int, int] = field(default_factory=dict)

    # Previous churn stage (for detecting transitions)
    prev_churn_stage: Optional[ChurnStage] = None

    @property
    def consumo_prom_lt(self) -> Decimal:
        """Average monthly liters (last 3 months or available)."""
        recent = self.monthly_stats[-3:] if self.monthly_stats else []
        if not recent:
            return Decimal("0")
        total = sum(m.total_litros for m in recent)
        return (total / len(recent)).quantize(Decimal("0.1"))

    @property
    def freq_cargas_semana(self) -> float:
        """Average charges per week (last 30 days)."""
        recent = self.monthly_stats[-1:] if self.monthly_stats else []
        if not recent:
            return 0.0
        cargas = recent[0].num_cargas
        return round(cargas * 7 / 30, 1)

    @property
    def is_new(self) -> bool:
        """Client with first charge ≤90 days ago."""
        if not self.primera_carga:
            return False
        delta = (datetime.now(timezone.utc) - self.primera_carga).days
        return delta <= 90

    @property
    def mom_litros_change_pct(self) -> Optional[float]:
        """Month-over-month liters change (%). None if <2 months."""
        if len(self.monthly_stats) < 2:
            return None
        current = float(self.monthly_stats[-1].total_litros)
        previous = float(self.monthly_stats[-2].total_litros)
        if previous == 0:
            return None
        return round((current - previous) / previous * 100, 1)


# ============================================================
# Retention Alert
# ============================================================

@dataclass
class RetentionAlert:
    """
    An alert generated by the retention engine.
    Can trigger WhatsApp messages, Odoo tasks, or both.
    """
    alert_type: RetentionAlertType
    placa: str
    client_name: str = ""
    segmento: Segmento = Segmento.VAGONETA
    churn_stage: ChurnStage = ChurnStage.GREEN
    station_id: Optional[int] = None

    # Details
    dias_sin_cargar: int = 0
    consumo_actual: Decimal = Decimal("0")
    consumo_anterior: Decimal = Decimal("0")
    drop_pct: float = 0.0

    message: str = ""
    recommended_action: str = ""
    priority: int = 3   # 1=highest, 5=lowest

    # Notification targets
    whatsapp_josue: bool = True
    whatsapp_client: bool = False
    create_odoo_task: bool = False

    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_whatsapp_message(self) -> str:
        """Format alert for WhatsApp notification to Josue."""
        stage_icon = {
            ChurnStage.GREEN: "🟢",
            ChurnStage.YELLOW: "🟡",
            ChurnStage.ORANGE: "🟠",
            ChurnStage.RED: "🔴",
        }
        type_icon = {
            RetentionAlertType.INACTIVITY: "⏰",
            RetentionAlertType.CONSUMPTION_DROP: "📉",
            RetentionAlertType.FREQUENCY_DROP: "📊",
            RetentionAlertType.RECOVERY: "🎉",
            RetentionAlertType.NEW_CLIENT: "🆕",
        }

        icon = type_icon.get(self.alert_type, "📋")
        stage = stage_icon.get(self.churn_stage, "⚪")

        lines = [
            f"{icon} *ALERTA RETENCIÓN*",
            f"{stage} {self.churn_stage.value} — {self.alert_type.value}",
            f"",
            f"🚗 *{self.placa}*" + (f" — {self.client_name}" if self.client_name else ""),
            f"📍 {self.segmento.value}",
        ]

        if self.alert_type == RetentionAlertType.INACTIVITY:
            lines.append(f"⏱ {self.dias_sin_cargar} días sin cargar")
        elif self.alert_type in (
            RetentionAlertType.CONSUMPTION_DROP,
            RetentionAlertType.FREQUENCY_DROP,
        ):
            lines.append(f"📉 Bajó {abs(self.drop_pct):.0f}%: {self.consumo_anterior}→{self.consumo_actual} lt/mes")
        elif self.alert_type == RetentionAlertType.RECOVERY:
            lines.append(f"🎉 Regresó después de {self.dias_sin_cargar} días")
        elif self.alert_type == RetentionAlertType.NEW_CLIENT:
            lines.append(f"🆕 Primera carga detectada")

        if self.recommended_action:
            lines.append(f"")
            lines.append(f"💡 *Acción:* {self.recommended_action}")

        return "\n".join(lines)

    def to_client_whatsapp(self) -> Optional[str]:
        """
        Optional WhatsApp message TO the client (e.g., promo offer).
        Only for YELLOW/ORANGE stages with known WhatsApp number.
        """
        if self.alert_type == RetentionAlertType.INACTIVITY:
            if self.churn_stage == ChurnStage.YELLOW:
                return (
                    f"Hola, te extrañamos en Central Gas ⛽\n"
                    f"Hace {self.dias_sin_cargar} días que no nos visitas.\n"
                    f"¡Te esperamos con el mejor precio en GNC! 🚗💨"
                )
            elif self.churn_stage == ChurnStage.ORANGE:
                return (
                    f"Hola, notamos que dejaste de cargar GNC.\n"
                    f"¿Todo bien con tu vehículo? Si necesitas apoyo "
                    f"técnico, estamos para ayudarte.\n"
                    f"Central Gas — Tu estación de confianza ⛽"
                )
        return None
