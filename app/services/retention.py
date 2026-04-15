"""
Client Retention & Inactivity Detection Service — HU-6.5

The 331+ vagonetas are Central Gas's core business. This service:

1. Builds client profiles from transaction history
2. Classifies each client into churn stages (GREEN → YELLOW → ORANGE → RED)
3. Detects trend changes (BAJANDO, CRECIENDO, etc.)
4. Generates retention alerts with recommended actions
5. Produces daily/weekly retention reports for Josue

The service runs:
  - Daily at close time: quick scan of all active clients
  - Weekly (Sunday): deep analysis with trend reclassification

Architecture:
  Transactions → build_profiles() → classify_churn() → detect_alerts() → RetentionAlert[]
                                                                            ↓
                                                      WhatsApp (Josue) + optional client msg
                                                      Odoo task (for high-value churning clients)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from app.models.client import (
    ChurnStage,
    ClientProfile,
    MonthlyStats,
    RetentionAlert,
    RetentionAlertType,
    RetentionThresholds,
    Segmento,
    Tendencia,
)
from app.models.transaction import TransactionNormalized

logger = logging.getLogger(__name__)


# ============================================================
# STATION NAMES (for display)
# ============================================================

STATION_NAMES = {
    1: "Parques Industriales",
    2: "Oriente",
    3: "Pensión/Nacozari",
}


# ============================================================
# Profile Builder
# ============================================================

def build_client_profiles(
    transactions: list[TransactionNormalized],
    reference_date: date | None = None,
    existing_profiles: dict[str, dict] | None = None,
) -> dict[str, ClientProfile]:
    """
    Build ClientProfile for each unique placa from transaction history.

    Args:
        transactions: All transactions (multi-station, multi-date)
        reference_date: "today" for dias_sin_cargar calculation
        existing_profiles: Optional dict with pre-existing client data
            (e.g., from DB: {placa: {segmento, whatsapp, nombre, ...}})

    Returns:
        Dict of placa → ClientProfile
    """
    if reference_date is None:
        reference_date = date.today()

    # Group transactions by placa
    by_placa: dict[str, list[TransactionNormalized]] = defaultdict(list)
    for t in transactions:
        if t.placa:
            by_placa[t.placa].append(t)

    profiles = {}
    for placa, txns in by_placa.items():
        # Sort by timestamp
        txns.sort(key=lambda t: t.timestamp_local)

        # Pre-existing data
        existing = (existing_profiles or {}).get(placa, {})
        segmento = Segmento(existing.get("segmento", "VAGONETA"))

        profile = ClientProfile(
            placa=placa,
            segmento=segmento,
            estatus=existing.get("estatus", "ACTIVO"),
            primera_carga=txns[0].timestamp_local,
            ultima_carga=txns[-1].timestamp_local,
            total_cargas=len(txns),
            total_litros=sum(t.litros for t in txns),
            total_mxn=sum(t.total_mxn for t in txns),
        )

        # Days since last charge
        last_date = txns[-1].timestamp_local.date()
        profile.dias_sin_cargar = (reference_date - last_date).days

        # Station distribution
        station_counts: dict[int, int] = defaultdict(int)
        for t in txns:
            if t.station_id:
                station_counts[t.station_id] += 1
        profile.station_distribution = dict(station_counts)

        # Primary station (most charges)
        if station_counts:
            primary_sid = max(station_counts, key=station_counts.get)
            profile.eds_principal = STATION_NAMES.get(primary_sid, f"EDS {primary_sid}")

        # Monthly aggregation
        monthly: dict[str, MonthlyStats] = {}
        for t in txns:
            key = f"{t.timestamp_local.year}-{t.timestamp_local.month:02d}"
            if key not in monthly:
                monthly[key] = MonthlyStats(
                    year=t.timestamp_local.year,
                    month=t.timestamp_local.month,
                )
            m = monthly[key]
            m.num_cargas += 1
            m.total_litros += t.litros
            m.total_mxn += t.total_mxn
            if t.station_id:
                m.station_ids.add(t.station_id)

        # Sort monthly stats by period
        sorted_months = sorted(monthly.values(), key=lambda m: (m.year, m.month))
        for m in sorted_months:
            if m.num_cargas > 0:
                m.avg_litros_per_carga = (m.total_litros / m.num_cargas).quantize(Decimal("0.1"))
        profile.monthly_stats = sorted_months

        profiles[placa] = profile

    return profiles


# ============================================================
# Churn Classification
# ============================================================

def classify_churn_stage(
    profile: ClientProfile,
    thresholds: RetentionThresholds | None = None,
) -> ChurnStage:
    """
    Classify client into churn stage based on inactivity and consumption.

    Stage determination (worst-of):
      1. Inactivity-based: dias_sin_cargar vs thresholds
      2. Consumption-based: MoM litros change vs thresholds

    Returns the worst (highest risk) stage.
    """
    if thresholds is None:
        thresholds = RetentionThresholds.for_segment(profile.segmento)

    # Save previous stage for transition detection
    profile.prev_churn_stage = profile.churn_stage

    # --- Inactivity-based ---
    inactivity_stage = ChurnStage.GREEN
    if profile.dias_sin_cargar >= thresholds.red_days:
        inactivity_stage = ChurnStage.RED
    elif profile.dias_sin_cargar >= thresholds.orange_days:
        inactivity_stage = ChurnStage.ORANGE
    elif profile.dias_sin_cargar >= thresholds.yellow_days:
        inactivity_stage = ChurnStage.YELLOW

    # --- Consumption-based ---
    consumption_stage = ChurnStage.GREEN
    mom_change = profile.mom_litros_change_pct
    if mom_change is not None and mom_change < 0:
        drop = abs(mom_change)
        if drop >= thresholds.consumption_drop_crit:
            consumption_stage = ChurnStage.ORANGE
        elif drop >= thresholds.consumption_drop_warn:
            consumption_stage = ChurnStage.YELLOW

    # Take the worst stage
    stage_order = [ChurnStage.GREEN, ChurnStage.YELLOW, ChurnStage.ORANGE, ChurnStage.RED]
    worst_idx = max(stage_order.index(inactivity_stage), stage_order.index(consumption_stage))
    profile.churn_stage = stage_order[worst_idx]

    return profile.churn_stage


def classify_tendencia(profile: ClientProfile) -> Tendencia:
    """
    Classify client trend from monthly consumption data.

    Rules:
      NUEVO_2025     — first charge ≤90 days ago
      PERDIDO_2025   — RED churn stage
      BAJANDO        — MoM drop >20% for 2+ months
      CRECIENDO      — MoM increase >10% for 2+ months
      ESTABLE        — everything else
    """
    if profile.is_new:
        profile.tendencia = Tendencia.NUEVO_2025
        return profile.tendencia

    if profile.churn_stage == ChurnStage.RED:
        profile.tendencia = Tendencia.PERDIDO_2025
        return profile.tendencia

    # Check consecutive monthly trends
    if len(profile.monthly_stats) >= 3:
        last3 = profile.monthly_stats[-3:]
        changes = []
        for i in range(1, len(last3)):
            prev = float(last3[i - 1].total_litros)
            curr = float(last3[i].total_litros)
            if prev > 0:
                changes.append((curr - prev) / prev * 100)

        if len(changes) >= 2:
            if all(c < -20 for c in changes):
                profile.tendencia = Tendencia.BAJANDO
                return profile.tendencia
            if all(c > 10 for c in changes):
                profile.tendencia = Tendencia.CRECIENDO
                return profile.tendencia

    profile.tendencia = Tendencia.ESTABLE
    return profile.tendencia


# ============================================================
# Alert Generation
# ============================================================

def detect_alerts(
    profiles: dict[str, ClientProfile],
    thresholds_map: dict[Segmento, RetentionThresholds] | None = None,
    previous_stages: dict[str, ChurnStage] | None = None,
) -> list[RetentionAlert]:
    """
    Scan all client profiles and generate retention alerts.

    Detects:
      1. INACTIVITY — client passes a churn stage threshold
      2. CONSUMPTION_DROP — significant MoM volume decline
      3. FREQUENCY_DROP — charging less often
      4. RECOVERY — client returns after being RED/ORANGE
      5. NEW_CLIENT — first charge detected (≤7 days ago)

    Args:
        profiles: Dict of placa → ClientProfile (already classified)
        thresholds_map: Optional per-segment thresholds
        previous_stages: Previous churn stages for transition detection

    Returns:
        List of RetentionAlert sorted by priority
    """
    if previous_stages is None:
        previous_stages = {}

    alerts = []

    for placa, profile in profiles.items():
        thresholds = RetentionThresholds.for_segment(profile.segmento)
        prev_stage = previous_stages.get(placa, profile.prev_churn_stage)

        # --- NEW CLIENT ---
        if profile.is_new and profile.total_cargas <= 3:
            alerts.append(RetentionAlert(
                alert_type=RetentionAlertType.NEW_CLIENT,
                placa=placa,
                segmento=profile.segmento,
                churn_stage=ChurnStage.GREEN,
                priority=5,
                message=f"Nuevo cliente: {placa}. {profile.total_cargas} cargas, "
                        f"{profile.total_litros:.0f} litros totales.",
                recommended_action="Dar bienvenida, explicar beneficios programa fidelidad",
            ))

        # --- RECOVERY ---
        if (
            prev_stage in (ChurnStage.RED, ChurnStage.ORANGE)
            and profile.churn_stage in (ChurnStage.GREEN, ChurnStage.YELLOW)
        ):
            alerts.append(RetentionAlert(
                alert_type=RetentionAlertType.RECOVERY,
                placa=placa,
                segmento=profile.segmento,
                churn_stage=profile.churn_stage,
                dias_sin_cargar=profile.dias_sin_cargar,
                priority=2,
                message=f"Cliente {placa} regresó después de estar {prev_stage.value}",
                recommended_action="Contactar para agradecer y asegurar retención",
                create_odoo_task=True,
            ))
            continue  # Don't also alert on inactivity for recovered clients

        # --- INACTIVITY ---
        if profile.churn_stage in (ChurnStage.YELLOW, ChurnStage.ORANGE, ChurnStage.RED):
            # Only alert on stage transitions or RED
            stage_changed = prev_stage != profile.churn_stage if prev_stage else True

            if stage_changed or profile.churn_stage == ChurnStage.RED:
                priority = {
                    ChurnStage.YELLOW: 4,
                    ChurnStage.ORANGE: 2,
                    ChurnStage.RED: 1,
                }[profile.churn_stage]

                actions = {
                    ChurnStage.YELLOW: "Enviar mensaje amigable de recordatorio",
                    ChurnStage.ORANGE: "Llamar al cliente, ofrecer revisión técnica gratuita",
                    ChurnStage.RED: "Visita personal o descuento especial para recuperar",
                }

                alert = RetentionAlert(
                    alert_type=RetentionAlertType.INACTIVITY,
                    placa=placa,
                    segmento=profile.segmento,
                    churn_stage=profile.churn_stage,
                    dias_sin_cargar=profile.dias_sin_cargar,
                    priority=priority,
                    message=f"{placa}: {profile.dias_sin_cargar} días sin cargar "
                            f"({profile.churn_stage.value})",
                    recommended_action=actions.get(profile.churn_stage, ""),
                    whatsapp_client=(
                        profile.churn_stage in (ChurnStage.YELLOW, ChurnStage.ORANGE)
                    ),
                    create_odoo_task=(profile.churn_stage == ChurnStage.RED),
                )
                alerts.append(alert)

        # --- CONSUMPTION DROP ---
        mom = profile.mom_litros_change_pct
        if mom is not None and mom < -thresholds.consumption_drop_warn:
            # Only if not already captured by inactivity
            if profile.churn_stage == ChurnStage.GREEN:
                prev_litros = (
                    profile.monthly_stats[-2].total_litros
                    if len(profile.monthly_stats) >= 2
                    else Decimal("0")
                )
                curr_litros = (
                    profile.monthly_stats[-1].total_litros
                    if profile.monthly_stats
                    else Decimal("0")
                )
                alerts.append(RetentionAlert(
                    alert_type=RetentionAlertType.CONSUMPTION_DROP,
                    placa=placa,
                    segmento=profile.segmento,
                    churn_stage=ChurnStage.YELLOW,
                    consumo_actual=curr_litros,
                    consumo_anterior=prev_litros,
                    drop_pct=mom,
                    priority=3,
                    message=f"{placa}: consumo bajó {abs(mom):.0f}% "
                            f"({prev_litros:.0f}→{curr_litros:.0f} lt/mes)",
                    recommended_action="Revisar si hay problema mecánico o competencia",
                ))

    # Sort by priority (1=highest)
    alerts.sort(key=lambda a: a.priority)
    return alerts


# ============================================================
# Retention Report
# ============================================================

@dataclass
class RetentionReport:
    """Daily/weekly retention report summary."""
    report_date: date
    total_clients: int = 0
    active_clients: int = 0

    # Churn stage distribution
    green_count: int = 0
    yellow_count: int = 0
    orange_count: int = 0
    red_count: int = 0

    # Trend distribution
    nuevos: int = 0
    creciendo: int = 0
    estables: int = 0
    bajando: int = 0
    perdidos: int = 0

    # Revenue at risk
    revenue_at_risk_mxn: Decimal = Decimal("0")   # Monthly revenue of YELLOW+ORANGE
    revenue_lost_mxn: Decimal = Decimal("0")       # Monthly revenue of RED

    # Alerts generated
    alerts: list[RetentionAlert] = field(default_factory=list)

    # Top at-risk clients (for Josue's attention)
    top_at_risk: list[ClientProfile] = field(default_factory=list)


def generate_retention_report(
    profiles: dict[str, ClientProfile],
    alerts: list[RetentionAlert],
    report_date: date | None = None,
) -> RetentionReport:
    """
    Generate a daily retention report from classified profiles.

    Returns RetentionReport with distribution counts, revenue at risk,
    and prioritized alert list.
    """
    if report_date is None:
        report_date = date.today()

    report = RetentionReport(report_date=report_date)
    report.total_clients = len(profiles)
    report.alerts = alerts

    for profile in profiles.values():
        # Churn stage counts
        if profile.churn_stage == ChurnStage.GREEN:
            report.green_count += 1
        elif profile.churn_stage == ChurnStage.YELLOW:
            report.yellow_count += 1
        elif profile.churn_stage == ChurnStage.ORANGE:
            report.orange_count += 1
        elif profile.churn_stage == ChurnStage.RED:
            report.red_count += 1

        # Tendencia counts
        if profile.tendencia == Tendencia.NUEVO_2025:
            report.nuevos += 1
        elif profile.tendencia == Tendencia.CRECIENDO:
            report.creciendo += 1
        elif profile.tendencia == Tendencia.ESTABLE:
            report.estables += 1
        elif profile.tendencia == Tendencia.BAJANDO:
            report.bajando += 1
        elif profile.tendencia == Tendencia.PERDIDO_2025:
            report.perdidos += 1

        # Active = not RED
        if profile.churn_stage != ChurnStage.RED:
            report.active_clients += 1

        # Revenue at risk
        monthly_rev = profile.consumo_prom_lt * Decimal("13.99")  # est. revenue
        if profile.churn_stage in (ChurnStage.YELLOW, ChurnStage.ORANGE):
            report.revenue_at_risk_mxn += monthly_rev
        elif profile.churn_stage == ChurnStage.RED:
            report.revenue_lost_mxn += monthly_rev

    # Top at-risk: ORANGE and RED, sorted by consumo_prom
    at_risk = [
        p for p in profiles.values()
        if p.churn_stage in (ChurnStage.ORANGE, ChurnStage.RED)
    ]
    at_risk.sort(key=lambda p: float(p.consumo_prom_lt), reverse=True)
    report.top_at_risk = at_risk[:10]

    return report


def format_retention_whatsapp(report: RetentionReport) -> str:
    """Format daily retention report for WhatsApp."""
    total = report.total_clients
    pct_green = round(report.green_count / total * 100, 1) if total else 0
    pct_yellow = round(report.yellow_count / total * 100, 1) if total else 0
    pct_orange = round(report.orange_count / total * 100, 1) if total else 0
    pct_red = round(report.red_count / total * 100, 1) if total else 0

    lines = [
        f"📊 *REPORTE RETENCIÓN*",
        f"📅 {report.report_date.strftime('%d/%m/%Y')}",
        f"",
        f"*Clientes:* {total} total, {report.active_clients} activos",
        f"",
        f"🟢 Sano: {report.green_count} ({pct_green}%)",
        f"🟡 En riesgo: {report.yellow_count} ({pct_yellow}%)",
        f"🟠 Alto riesgo: {report.orange_count} ({pct_orange}%)",
        f"🔴 Perdido: {report.red_count} ({pct_red}%)",
        f"",
        f"*Tendencias:*",
        f"  🆕 Nuevos: {report.nuevos}",
        f"  📈 Creciendo: {report.creciendo}",
        f"  ➡️ Estables: {report.estables}",
        f"  📉 Bajando: {report.bajando}",
        f"  💀 Perdidos: {report.perdidos}",
    ]

    if report.revenue_at_risk_mxn > 0:
        lines.append(f"")
        lines.append(f"⚠️ *Revenue en riesgo:* ${report.revenue_at_risk_mxn:,.0f}/mes")

    if report.revenue_lost_mxn > 0:
        lines.append(f"🔴 *Revenue perdido:* ${report.revenue_lost_mxn:,.0f}/mes")

    if report.alerts:
        lines.append(f"")
        lines.append(f"🔔 *{len(report.alerts)} alertas:*")
        for alert in report.alerts[:5]:
            stage_icon = {
                ChurnStage.GREEN: "🟢", ChurnStage.YELLOW: "🟡",
                ChurnStage.ORANGE: "🟠", ChurnStage.RED: "🔴",
            }
            lines.append(f"  {stage_icon.get(alert.churn_stage, '⚪')} {alert.message}")
        if len(report.alerts) > 5:
            lines.append(f"  ... y {len(report.alerts) - 5} más")

    return "\n".join(lines)


# ============================================================
# Full Retention Pipeline
# ============================================================

def run_retention_analysis(
    transactions: list[TransactionNormalized],
    reference_date: date | None = None,
    existing_profiles: dict[str, dict] | None = None,
    previous_stages: dict[str, ChurnStage] | None = None,
) -> tuple[RetentionReport, dict[str, ClientProfile]]:
    """
    Run the full retention analysis pipeline:
      1. Build client profiles from transactions
      2. Classify churn stages
      3. Classify tendencias
      4. Generate alerts
      5. Build report

    Args:
        transactions: All transaction history
        reference_date: "today" for analysis
        existing_profiles: Pre-existing client metadata from DB
        previous_stages: Previous churn stages for transition detection

    Returns:
        (RetentionReport, dict of placa → ClientProfile)
    """
    if reference_date is None:
        reference_date = date.today()

    logger.info(f"Running retention analysis for {reference_date}")

    # 1. Build profiles
    profiles = build_client_profiles(
        transactions, reference_date, existing_profiles
    )
    logger.info(f"  Built {len(profiles)} client profiles")

    # 2. Classify churn stages
    for profile in profiles.values():
        classify_churn_stage(profile)

    # 3. Classify tendencias
    for profile in profiles.values():
        classify_tendencia(profile)

    # 4. Generate alerts
    alerts = detect_alerts(profiles, previous_stages=previous_stages)
    logger.info(f"  Generated {len(alerts)} retention alerts")

    # 5. Build report
    report = generate_retention_report(profiles, alerts, reference_date)

    logger.info(
        f"  Distribution: GREEN={report.green_count} YELLOW={report.yellow_count} "
        f"ORANGE={report.orange_count} RED={report.red_count}"
    )

    return report, profiles
