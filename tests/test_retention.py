"""
Client Retention & Inactivity Detection Tests — HU-6.5

Tests the full retention pipeline:
  - Client profile building from transactions
  - Churn stage classification (GREEN → YELLOW → ORANGE → RED)
  - Tendencia classification (NUEVO → CRECIENDO → ESTABLE → BAJANDO → PERDIDO)
  - Alert generation (inactivity, consumption drop, recovery, new client)
  - Retention report generation
  - WhatsApp message formatting
  - Integration with real 388K transaction data
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, "/sessions/cool-focused-pasteur/mnt/Downloads/central-gas-agent")

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
from app.models.transaction import TransactionNormalized, MedioPago, SchemaVersion
from app.services.retention import (
    build_client_profiles,
    classify_churn_stage,
    classify_tendencia,
    detect_alerts,
    generate_retention_report,
    format_retention_whatsapp,
    run_retention_analysis,
)


# ============================================================
# Helpers
# ============================================================

CST = timezone(timedelta(hours=-6))
REF_DATE = date(2026, 1, 31)  # "today" for tests


def make_txn(
    placa: str = "ABC-123",
    station_id: int = 3,
    local_date: date = date(2026, 1, 15),
    litros: float = 40.0,
    total: float = 559.60,
    pvp: float = 13.99,
    medio: MedioPago = MedioPago.EFECTIVO,
) -> TransactionNormalized:
    local_dt = datetime(local_date.year, local_date.month, local_date.day, 10, 0, tzinfo=CST)
    utc_dt = local_dt.astimezone(timezone.utc)
    total_d = Decimal(str(total))
    litros_d = Decimal(str(litros))
    ingreso_neto = (total_d / Decimal("1.16")).quantize(Decimal("0.01"))
    iva = total_d - ingreso_neto
    kg = (litros_d * Decimal("0.717")).quantize(Decimal("0.0001"))

    return TransactionNormalized(
        source_file="test.csv",
        source_row=1,
        schema_version=SchemaVersion.PRE_2023,
        station_id=station_id,
        station_natgas="EDS Nacozari",
        plaza="AGUASCALIENTES",
        timestamp_utc=utc_dt,
        timestamp_local=local_dt,
        placa=placa,
        litros=litros_d,
        pvp=Decimal(str(pvp)),
        total_mxn=total_d,
        medio_pago=medio,
        kg=kg,
        nm3=litros_d,
        ingreso_neto=ingreso_neto,
        iva=iva,
    )


def make_monthly_txns(
    placa: str,
    year: int,
    month: int,
    num_cargas: int = 8,
    litros_per: float = 40.0,
    station_id: int = 3,
) -> list[TransactionNormalized]:
    """Generate transactions spread across a month."""
    txns = []
    for i in range(num_cargas):
        day = min(1 + i * (28 // max(num_cargas, 1)), 28)
        txns.append(make_txn(
            placa=placa,
            station_id=station_id,
            local_date=date(year, month, day),
            litros=litros_per,
            total=round(litros_per * 13.99, 2),
        ))
    return txns


# ============================================================
# PROFILE BUILDER TESTS
# ============================================================

class TestBuildProfiles:
    def test_single_client(self):
        txns = [
            make_txn(placa="PLT-001", local_date=date(2026, 1, 10)),
            make_txn(placa="PLT-001", local_date=date(2026, 1, 20)),
        ]
        profiles = build_client_profiles(txns, REF_DATE)
        assert "PLT-001" in profiles
        p = profiles["PLT-001"]
        assert p.total_cargas == 2
        assert p.dias_sin_cargar == 11  # Jan 31 - Jan 20

    def test_multiple_clients(self):
        txns = [
            make_txn(placa="A-001", local_date=date(2026, 1, 15)),
            make_txn(placa="A-002", local_date=date(2026, 1, 15)),
            make_txn(placa="A-003", local_date=date(2026, 1, 15)),
        ]
        profiles = build_client_profiles(txns, REF_DATE)
        assert len(profiles) == 3

    def test_station_distribution(self):
        txns = [
            make_txn(placa="PLT-001", station_id=3, local_date=date(2026, 1, 5)),
            make_txn(placa="PLT-001", station_id=3, local_date=date(2026, 1, 10)),
            make_txn(placa="PLT-001", station_id=1, local_date=date(2026, 1, 15)),
        ]
        profiles = build_client_profiles(txns, REF_DATE)
        p = profiles["PLT-001"]
        assert p.station_distribution == {3: 2, 1: 1}
        assert p.eds_principal == "Pensión/Nacozari"

    def test_monthly_stats(self):
        txns = (
            make_monthly_txns("PLT-001", 2025, 11, num_cargas=6)
            + make_monthly_txns("PLT-001", 2025, 12, num_cargas=8)
            + make_monthly_txns("PLT-001", 2026, 1, num_cargas=10)
        )
        profiles = build_client_profiles(txns, REF_DATE)
        p = profiles["PLT-001"]
        assert len(p.monthly_stats) == 3
        assert p.monthly_stats[-1].num_cargas == 10  # Jan 2026

    def test_consumo_prom_lt(self):
        txns = (
            make_monthly_txns("PLT-001", 2025, 11, num_cargas=8, litros_per=40)
            + make_monthly_txns("PLT-001", 2025, 12, num_cargas=8, litros_per=40)
            + make_monthly_txns("PLT-001", 2026, 1, num_cargas=8, litros_per=40)
        )
        profiles = build_client_profiles(txns, REF_DATE)
        p = profiles["PLT-001"]
        # 8 charges × 40L = 320L/month average
        assert p.consumo_prom_lt == Decimal("320.0")

    def test_existing_profile_data(self):
        txns = [make_txn(placa="TAXI-001", local_date=date(2026, 1, 25))]
        existing = {"TAXI-001": {"segmento": "TAXI", "estatus": "ACTIVO"}}
        profiles = build_client_profiles(txns, REF_DATE, existing)
        assert profiles["TAXI-001"].segmento == Segmento.TAXI

    def test_empty_transactions(self):
        profiles = build_client_profiles([], REF_DATE)
        assert len(profiles) == 0


# ============================================================
# CHURN CLASSIFICATION TESTS
# ============================================================

class TestChurnClassification:
    def test_green_active_client(self):
        """Recent charge → GREEN."""
        p = ClientProfile(placa="PLT-001", dias_sin_cargar=2)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.GREEN

    def test_yellow_7_days(self):
        """7 days inactive → YELLOW for vagoneta."""
        p = ClientProfile(placa="PLT-001", dias_sin_cargar=8)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.YELLOW

    def test_orange_15_days(self):
        """15 days inactive → ORANGE."""
        p = ClientProfile(placa="PLT-001", dias_sin_cargar=16)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.ORANGE

    def test_red_30_days(self):
        """30 days inactive → RED."""
        p = ClientProfile(placa="PLT-001", dias_sin_cargar=35)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.RED

    def test_taxi_stricter_thresholds(self):
        """Taxi with 5 days inactive → YELLOW (stricter than vagoneta)."""
        p = ClientProfile(placa="TAXI-001", segmento=Segmento.TAXI, dias_sin_cargar=5)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.YELLOW

    def test_particular_more_lenient(self):
        """Particular with 10 days → still GREEN (more lenient)."""
        p = ClientProfile(placa="PART-001", segmento=Segmento.PARTICULAR, dias_sin_cargar=10)
        stage = classify_churn_stage(p)
        assert stage == ChurnStage.GREEN

    def test_consumption_drop_triggers_yellow(self):
        """20%+ MoM consumption drop → YELLOW even if active."""
        p = ClientProfile(
            placa="PLT-001",
            dias_sin_cargar=2,
            monthly_stats=[
                MonthlyStats(year=2025, month=12, num_cargas=8, total_litros=Decimal("400")),
                MonthlyStats(year=2026, month=1, num_cargas=5, total_litros=Decimal("300")),
            ],
        )
        stage = classify_churn_stage(p)
        # 25% drop → YELLOW
        assert stage == ChurnStage.YELLOW

    def test_consumption_drop_50pct_orange(self):
        """50%+ MoM drop → ORANGE."""
        p = ClientProfile(
            placa="PLT-001",
            dias_sin_cargar=2,
            monthly_stats=[
                MonthlyStats(year=2025, month=12, num_cargas=8, total_litros=Decimal("400")),
                MonthlyStats(year=2026, month=1, num_cargas=2, total_litros=Decimal("180")),
            ],
        )
        stage = classify_churn_stage(p)
        # 55% drop → ORANGE
        assert stage == ChurnStage.ORANGE

    def test_worst_of_inactivity_and_consumption(self):
        """Takes the worst between inactivity stage and consumption stage."""
        p = ClientProfile(
            placa="PLT-001",
            dias_sin_cargar=10,  # YELLOW by inactivity
            monthly_stats=[
                MonthlyStats(year=2025, month=12, num_cargas=8, total_litros=Decimal("400")),
                MonthlyStats(year=2026, month=1, num_cargas=2, total_litros=Decimal("150")),
            ],
        )
        stage = classify_churn_stage(p)
        # Inactivity=YELLOW, Consumption=ORANGE(62.5% drop) → ORANGE
        assert stage == ChurnStage.ORANGE

    def test_prev_stage_saved(self):
        """Previous churn stage is saved for transition detection."""
        p = ClientProfile(placa="PLT-001", dias_sin_cargar=2, churn_stage=ChurnStage.YELLOW)
        classify_churn_stage(p)
        assert p.prev_churn_stage == ChurnStage.YELLOW
        assert p.churn_stage == ChurnStage.GREEN


# ============================================================
# TENDENCIA TESTS
# ============================================================

class TestTendencia:
    def test_nuevo_client(self):
        now = datetime.now(timezone.utc)
        p = ClientProfile(
            placa="NEW-001",
            primera_carga=now - timedelta(days=30),
        )
        t = classify_tendencia(p)
        assert t == Tendencia.NUEVO_2025

    def test_perdido_red_stage(self):
        p = ClientProfile(
            placa="LOST-001",
            churn_stage=ChurnStage.RED,
            primera_carga=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        t = classify_tendencia(p)
        assert t == Tendencia.PERDIDO_2025

    def test_bajando_consecutive_drops(self):
        p = ClientProfile(
            placa="DOWN-001",
            primera_carga=datetime(2024, 6, 1, tzinfo=timezone.utc),
            monthly_stats=[
                MonthlyStats(year=2025, month=10, total_litros=Decimal("400")),
                MonthlyStats(year=2025, month=11, total_litros=Decimal("300")),  # -25%
                MonthlyStats(year=2025, month=12, total_litros=Decimal("200")),  # -33%
            ],
        )
        t = classify_tendencia(p)
        assert t == Tendencia.BAJANDO

    def test_creciendo_consecutive_growth(self):
        p = ClientProfile(
            placa="UP-001",
            primera_carga=datetime(2024, 6, 1, tzinfo=timezone.utc),
            monthly_stats=[
                MonthlyStats(year=2025, month=10, total_litros=Decimal("200")),
                MonthlyStats(year=2025, month=11, total_litros=Decimal("250")),  # +25%
                MonthlyStats(year=2025, month=12, total_litros=Decimal("310")),  # +24%
            ],
        )
        t = classify_tendencia(p)
        assert t == Tendencia.CRECIENDO

    def test_estable_default(self):
        p = ClientProfile(
            placa="STABLE-001",
            primera_carga=datetime(2024, 6, 1, tzinfo=timezone.utc),
            monthly_stats=[
                MonthlyStats(year=2025, month=10, total_litros=Decimal("300")),
                MonthlyStats(year=2025, month=11, total_litros=Decimal("310")),  # +3%
                MonthlyStats(year=2025, month=12, total_litros=Decimal("295")),  # -5%
            ],
        )
        t = classify_tendencia(p)
        assert t == Tendencia.ESTABLE


# ============================================================
# ALERT GENERATION TESTS
# ============================================================

class TestAlertDetection:
    def test_inactivity_alert_yellow(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001",
                churn_stage=ChurnStage.YELLOW,
                dias_sin_cargar=10,
            ),
        }
        alerts = detect_alerts(profiles)
        assert len(alerts) == 1
        assert alerts[0].alert_type == RetentionAlertType.INACTIVITY
        assert alerts[0].churn_stage == ChurnStage.YELLOW

    def test_inactivity_alert_red(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001",
                churn_stage=ChurnStage.RED,
                dias_sin_cargar=35,
            ),
        }
        alerts = detect_alerts(profiles)
        assert len(alerts) == 1
        assert alerts[0].priority == 1  # highest
        assert alerts[0].create_odoo_task is True

    def test_recovery_alert(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001",
                churn_stage=ChurnStage.GREEN,
                prev_churn_stage=ChurnStage.RED,
                dias_sin_cargar=2,
            ),
        }
        previous = {"PLT-001": ChurnStage.RED}
        alerts = detect_alerts(profiles, previous_stages=previous)
        assert len(alerts) == 1
        assert alerts[0].alert_type == RetentionAlertType.RECOVERY

    def test_new_client_alert(self):
        now = datetime.now(timezone.utc)
        profiles = {
            "NEW-001": ClientProfile(
                placa="NEW-001",
                primera_carga=now - timedelta(days=3),
                total_cargas=2,
                total_litros=Decimal("80"),
                churn_stage=ChurnStage.GREEN,
            ),
        }
        alerts = detect_alerts(profiles)
        assert len(alerts) == 1
        assert alerts[0].alert_type == RetentionAlertType.NEW_CLIENT
        assert alerts[0].priority == 5  # lowest priority

    def test_consumption_drop_alert(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001",
                churn_stage=ChurnStage.GREEN,
                dias_sin_cargar=2,
                primera_carga=datetime(2024, 1, 1, tzinfo=timezone.utc),
                monthly_stats=[
                    MonthlyStats(year=2025, month=12, total_litros=Decimal("400")),
                    MonthlyStats(year=2026, month=1, total_litros=Decimal("300")),  # -25%
                ],
            ),
        }
        alerts = detect_alerts(profiles)
        assert len(alerts) == 1
        assert alerts[0].alert_type == RetentionAlertType.CONSUMPTION_DROP
        assert abs(alerts[0].drop_pct) == 25.0

    def test_no_alerts_for_green_healthy(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001",
                churn_stage=ChurnStage.GREEN,
                dias_sin_cargar=2,
                primera_carga=datetime(2024, 1, 1, tzinfo=timezone.utc),
                total_cargas=100,
                monthly_stats=[
                    MonthlyStats(year=2025, month=12, total_litros=Decimal("300")),
                    MonthlyStats(year=2026, month=1, total_litros=Decimal("310")),
                ],
            ),
        }
        alerts = detect_alerts(profiles)
        assert len(alerts) == 0

    def test_alerts_sorted_by_priority(self):
        now = datetime.now(timezone.utc)
        profiles = {
            "RED-001": ClientProfile(
                placa="RED-001", churn_stage=ChurnStage.RED, dias_sin_cargar=40,
            ),
            "YELLOW-001": ClientProfile(
                placa="YELLOW-001", churn_stage=ChurnStage.YELLOW, dias_sin_cargar=10,
            ),
            "NEW-001": ClientProfile(
                placa="NEW-001", churn_stage=ChurnStage.GREEN,
                primera_carga=now - timedelta(days=2), total_cargas=1,
                total_litros=Decimal("40"),
            ),
        }
        alerts = detect_alerts(profiles)
        # RED(1) → YELLOW(4) → NEW(5)
        assert alerts[0].priority <= alerts[-1].priority

    def test_yellow_alert_offers_client_whatsapp(self):
        profiles = {
            "PLT-001": ClientProfile(
                placa="PLT-001", churn_stage=ChurnStage.YELLOW, dias_sin_cargar=10,
            ),
        }
        alerts = detect_alerts(profiles)
        assert alerts[0].whatsapp_client is True


# ============================================================
# WHATSAPP MESSAGE TESTS
# ============================================================

class TestWhatsAppMessages:
    def test_inactivity_alert_message(self):
        alert = RetentionAlert(
            alert_type=RetentionAlertType.INACTIVITY,
            placa="PLT-001",
            client_name="Carlos Ramirez",
            churn_stage=ChurnStage.ORANGE,
            dias_sin_cargar=18,
        )
        msg = alert.to_whatsapp_message()
        assert "ALERTA RETENCIÓN" in msg
        assert "PLT-001" in msg
        assert "18 días sin cargar" in msg
        assert "🟠" in msg

    def test_recovery_alert_message(self):
        alert = RetentionAlert(
            alert_type=RetentionAlertType.RECOVERY,
            placa="PLT-001",
            churn_stage=ChurnStage.GREEN,
            dias_sin_cargar=5,
        )
        msg = alert.to_whatsapp_message()
        assert "Regresó" in msg
        assert "🎉" in msg

    def test_client_whatsapp_yellow(self):
        alert = RetentionAlert(
            alert_type=RetentionAlertType.INACTIVITY,
            placa="PLT-001",
            churn_stage=ChurnStage.YELLOW,
            dias_sin_cargar=10,
        )
        msg = alert.to_client_whatsapp()
        assert msg is not None
        assert "Central Gas" in msg
        assert "10 días" in msg

    def test_client_whatsapp_orange(self):
        alert = RetentionAlert(
            alert_type=RetentionAlertType.INACTIVITY,
            placa="PLT-001",
            churn_stage=ChurnStage.ORANGE,
            dias_sin_cargar=20,
        )
        msg = alert.to_client_whatsapp()
        assert msg is not None
        assert "técnico" in msg.lower() or "apoyo" in msg.lower()

    def test_client_whatsapp_none_for_green(self):
        alert = RetentionAlert(
            alert_type=RetentionAlertType.INACTIVITY,
            placa="PLT-001",
            churn_stage=ChurnStage.GREEN,
        )
        assert alert.to_client_whatsapp() is None


# ============================================================
# RETENTION REPORT TESTS
# ============================================================

class TestRetentionReport:
    def test_report_counts(self):
        profiles = {
            "G1": ClientProfile(placa="G1", churn_stage=ChurnStage.GREEN, tendencia=Tendencia.ESTABLE),
            "G2": ClientProfile(placa="G2", churn_stage=ChurnStage.GREEN, tendencia=Tendencia.CRECIENDO),
            "Y1": ClientProfile(placa="Y1", churn_stage=ChurnStage.YELLOW, tendencia=Tendencia.BAJANDO,
                                monthly_stats=[MonthlyStats(2026, 1, total_litros=Decimal("200"))]),
            "R1": ClientProfile(placa="R1", churn_stage=ChurnStage.RED, tendencia=Tendencia.PERDIDO_2025,
                                monthly_stats=[MonthlyStats(2025, 11, total_litros=Decimal("300"))]),
        }
        report = generate_retention_report(profiles, [], REF_DATE)
        assert report.total_clients == 4
        assert report.active_clients == 3  # not RED
        assert report.green_count == 2
        assert report.yellow_count == 1
        assert report.red_count == 1
        assert report.estables == 1
        assert report.creciendo == 1
        assert report.bajando == 1
        assert report.perdidos == 1

    def test_revenue_at_risk(self):
        profiles = {
            "Y1": ClientProfile(
                placa="Y1", churn_stage=ChurnStage.YELLOW,
                tendencia=Tendencia.BAJANDO,
                monthly_stats=[
                    MonthlyStats(2025, 11, total_litros=Decimal("300")),
                    MonthlyStats(2025, 12, total_litros=Decimal("300")),
                    MonthlyStats(2026, 1, total_litros=Decimal("300")),
                ],
            ),
        }
        report = generate_retention_report(profiles, [], REF_DATE)
        # consumo_prom = 300 lt, revenue = 300 * 13.99 = $4,197
        assert report.revenue_at_risk_mxn == Decimal("300.0") * Decimal("13.99")

    def test_whatsapp_report_format(self):
        profiles = {
            "G1": ClientProfile(placa="G1", churn_stage=ChurnStage.GREEN, tendencia=Tendencia.ESTABLE),
            "Y1": ClientProfile(placa="Y1", churn_stage=ChurnStage.YELLOW, tendencia=Tendencia.BAJANDO),
        }
        report = generate_retention_report(profiles, [], REF_DATE)
        msg = format_retention_whatsapp(report)
        assert "REPORTE RETENCIÓN" in msg
        assert "2 total" in msg
        assert "🟢" in msg
        assert "🟡" in msg

    def test_top_at_risk_sorted(self):
        profiles = {
            "O1": ClientProfile(
                placa="O1", churn_stage=ChurnStage.ORANGE,
                monthly_stats=[
                    MonthlyStats(2025, 11, total_litros=Decimal("200")),
                    MonthlyStats(2025, 12, total_litros=Decimal("200")),
                    MonthlyStats(2026, 1, total_litros=Decimal("200")),
                ],
            ),
            "R1": ClientProfile(
                placa="R1", churn_stage=ChurnStage.RED,
                monthly_stats=[
                    MonthlyStats(2025, 11, total_litros=Decimal("500")),
                    MonthlyStats(2025, 12, total_litros=Decimal("500")),
                    MonthlyStats(2026, 1, total_litros=Decimal("500")),
                ],
            ),
        }
        report = generate_retention_report(profiles, [], REF_DATE)
        assert len(report.top_at_risk) == 2
        # R1 has higher consumo → first
        assert report.top_at_risk[0].placa == "R1"


# ============================================================
# FULL PIPELINE INTEGRATION
# ============================================================

class TestFullPipeline:
    def test_run_retention_analysis(self):
        """Full pipeline with synthetic multi-month data."""
        # 3 clients with different patterns
        txns = (
            # Active client — charges regularly, including very recent
            make_monthly_txns("ACTIVE-001", 2025, 11, 8, 40)
            + make_monthly_txns("ACTIVE-001", 2025, 12, 8, 40)
            + make_monthly_txns("ACTIVE-001", 2026, 1, 8, 40)
            + [make_txn(placa="ACTIVE-001", local_date=date(2026, 1, 29), litros=40)]
            # Declining client — fewer charges each month
            + make_monthly_txns("DECLINE-001", 2025, 11, 8, 40)
            + make_monthly_txns("DECLINE-001", 2025, 12, 5, 30)
            + make_monthly_txns("DECLINE-001", 2026, 1, 2, 20)
            # Churned client — no charges in January
            + make_monthly_txns("LOST-001", 2025, 11, 8, 40)
            + make_monthly_txns("LOST-001", 2025, 12, 4, 30)
            # No Jan 2026 charges → 31 days inactive as of Jan 31
        )

        report, profiles = run_retention_analysis(txns, REF_DATE)

        assert report.total_clients == 3

        # Active should be GREEN
        assert profiles["ACTIVE-001"].churn_stage == ChurnStage.GREEN

        # Declining should be YELLOW or ORANGE (consumption drop)
        assert profiles["DECLINE-001"].churn_stage in (ChurnStage.YELLOW, ChurnStage.ORANGE)

        # Lost should be RED (31 days inactive)
        assert profiles["LOST-001"].churn_stage == ChurnStage.RED
        assert profiles["LOST-001"].tendencia == Tendencia.PERDIDO_2025

        # Should have alerts
        assert len(report.alerts) > 0
        alert_placas = {a.placa for a in report.alerts}
        assert "LOST-001" in alert_placas

    def test_pipeline_with_real_data(self):
        """Integration with real 388K GasUp transactions."""
        csv_dir = Path("/sessions/cool-focused-pasteur/mnt/Downloads/central-gas-agent/data/gasup")
        if not csv_dir.exists():
            pytest.skip("Real CSV data not available")

        from app.parsers.gasup import parse_directory

        results = parse_directory(csv_dir)
        all_txns = []
        for pr in results:
            all_txns.extend(pr.transactions)

        if not all_txns:
            pytest.skip("No transactions parsed")

        # Run analysis as of Jan 31, 2026
        report, profiles = run_retention_analysis(all_txns, date(2026, 1, 31))

        # Should have 100+ unique clients
        assert report.total_clients >= 100

        # Should have some distribution across stages
        assert report.green_count > 0
        assert report.total_clients == (
            report.green_count + report.yellow_count
            + report.orange_count + report.red_count
        )

        # Verify profiles have monthly stats
        sample = list(profiles.values())[0]
        assert len(sample.monthly_stats) >= 1

        # Print summary for manual inspection
        print(f"\nReal data retention analysis:")
        print(f"  Total clients: {report.total_clients}")
        print(f"  GREEN: {report.green_count}")
        print(f"  YELLOW: {report.yellow_count}")
        print(f"  ORANGE: {report.orange_count}")
        print(f"  RED: {report.red_count}")
        print(f"  Alerts: {len(report.alerts)}")
        print(f"  Revenue at risk: ${report.revenue_at_risk_mxn:,.0f}/mes")
        print(f"  Revenue lost: ${report.revenue_lost_mxn:,.0f}/mes")


# ============================================================
# THRESHOLDS TESTS
# ============================================================

class TestThresholds:
    def test_vagoneta_defaults(self):
        t = RetentionThresholds()
        assert t.yellow_days == 7
        assert t.red_days == 30

    def test_taxi_stricter(self):
        t = RetentionThresholds.for_segment(Segmento.TAXI)
        assert t.yellow_days == 4
        assert t.red_days == 14

    def test_particular_more_lenient(self):
        t = RetentionThresholds.for_segment(Segmento.PARTICULAR)
        assert t.yellow_days == 14
        assert t.red_days == 45

    def test_segment_specific(self):
        for seg in Segmento:
            t = RetentionThresholds.for_segment(seg)
            assert t.yellow_days < t.orange_days < t.red_days
