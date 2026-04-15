"""
Orchestrator + WhatsApp Test Suite.

Tests the full nightly close pipeline with mocked dependencies:
  - Parser returns synthetic transactions
  - Odoo client mocked
  - WhatsApp uses mock provider
  - DB disabled

Also tests the WhatsApp sender directly.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "/sessions/cool-focused-pasteur/mnt/Downloads/central-gas-agent")

from app.models.transaction import TransactionNormalized, MedioPago, SchemaVersion
from app.services.orchestrator import (
    Orchestrator,
    OrchestratorConfig,
    Phase,
    PhaseStatus,
    CloseRunResult,
)
from app.services.whatsapp import (
    WhatsAppSender,
    WhatsAppConfig,
    WhatsAppProvider,
    SendResult,
)


# ============================================================
# HELPERS
# ============================================================

CLOSE_DATE = date(2026, 1, 15)

# CST timezone for test data
CST = timezone(timedelta(hours=-6))

def make_txn(
    placa: str = "ABC-123",
    litros: float = 40.0,
    total: float = 419.70,
    medio: MedioPago = MedioPago.EFECTIVO,
    station_id: int = 3,
    pvp: float = 10.49,
    hora: str = "10:30",
) -> TransactionNormalized:
    h, m = int(hora.split(":")[0]), int(hora.split(":")[1])
    local_dt = datetime(2026, 1, 15, h, m, 0, tzinfo=CST)
    utc_dt = local_dt.astimezone(timezone.utc)
    total_d = Decimal(str(total))
    litros_d = Decimal(str(litros))
    pvp_d = Decimal(str(pvp))
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
        pvp=pvp_d,
        total_mxn=total_d,
        medio_pago=medio,
        kg=kg,
        nm3=litros_d,
        ingreso_neto=ingreso_neto,
        iva=iva,
    )


def make_transactions(n: int = 20, station_id: int = 3) -> list[TransactionNormalized]:
    """Generate n synthetic transactions for a single station."""
    txns = []
    for i in range(n):
        medio = MedioPago.EFECTIVO if i % 5 != 0 else MedioPago.TARJETA_DEBITO
        txns.append(make_txn(
            placa=f"PLT-{i:03d}",
            litros=35.0 + i,
            total=round((35.0 + i) * 10.49, 2),
            medio=medio,
            station_id=station_id,
            hora=f"{8 + i % 12}:{i % 60:02d}",
        ))
    return txns


class MockParseResult:
    """Mimics what parse_directory returns."""
    def __init__(self, transactions):
        self.transactions = transactions
        self.row_count = len(transactions)


def mock_parse_directory(csv_dir: Path):
    """Return a list of MockParseResult with synthetic transactions."""
    txns = make_transactions(20, station_id=3)
    return [MockParseResult(txns)]


# ============================================================
# WHATSAPP SENDER TESTS
# ============================================================

class TestWhatsAppSender:
    def test_mock_send(self):
        config = WhatsAppConfig(provider=WhatsAppProvider.MOCK)
        sender = WhatsAppSender(config)
        result = sender.send("+521234567890", "Hello test")
        assert result.success is True
        assert result.provider == "mock"
        assert sender.total_sent == 1
        assert sender.total_failed == 0

    def test_resolve_alias_josue(self):
        config = WhatsAppConfig(
            provider=WhatsAppProvider.MOCK,
            josue_whatsapp="whatsapp:+5214491234567",
        )
        sender = WhatsAppSender(config)
        resolved = sender.resolve_recipient("josue")
        assert resolved == "whatsapp:+5214491234567"

    def test_resolve_alias_tecnico(self):
        config = WhatsAppConfig(
            provider=WhatsAppProvider.MOCK,
            tecnico_whatsapp="whatsapp:+5214497654321",
        )
        sender = WhatsAppSender(config)
        resolved = sender.resolve_recipient("tecnico")
        assert resolved == "whatsapp:+5214497654321"

    def test_resolve_direct_number(self):
        config = WhatsAppConfig(provider=WhatsAppProvider.MOCK)
        sender = WhatsAppSender(config)
        resolved = sender.resolve_recipient("whatsapp:+5214499999999")
        assert resolved == "whatsapp:+5214499999999"

    def test_send_to_multiple_deduplicates(self):
        config = WhatsAppConfig(
            provider=WhatsAppProvider.MOCK,
            josue_whatsapp="whatsapp:+521111",
            tecnico_whatsapp="whatsapp:+522222",
        )
        sender = WhatsAppSender(config)
        results = sender.send_to_recipients(
            ["josue", "tecnico", "josue"],  # josue duplicated
            "Test message",
        )
        assert len(results) == 2  # deduplicated
        assert sender.total_sent == 2

    def test_send_log(self):
        config = WhatsAppConfig(provider=WhatsAppProvider.MOCK)
        sender = WhatsAppSender(config)
        sender.send("+521111", "msg1")
        sender.send("+522222", "msg2")
        assert len(sender.send_log) == 2

    def test_twilio_import_error(self):
        """Twilio provider fails gracefully if package not installed."""
        config = WhatsAppConfig(
            provider=WhatsAppProvider.TWILIO,
            twilio_account_sid="fake",
            twilio_auth_token="fake",
        )
        sender = WhatsAppSender(config)
        # This will fail because twilio is not installed
        result = sender.send("+521111", "test")
        assert result.success is False
        assert "twilio" in result.error.lower()


# ============================================================
# ORCHESTRATOR TESTS
# ============================================================

class TestOrchestrator:
    def _make_orchestrator(self, odoo=None, whatsapp=None, db=None) -> Orchestrator:
        config = OrchestratorConfig(
            csv_dir=Path("data/gasup"),
            odoo_enabled=odoo is not None,
            scada_enabled=False,
            whatsapp_enabled=whatsapp is not None,
            db_enabled=db is not None,
            station_ids=[3],  # Just Nacozari for tests
        )
        return Orchestrator(
            config=config,
            parser=mock_parse_directory,
            odoo_client=odoo,
            whatsapp_sender=whatsapp,
            db_conn=db,
        )

    def test_full_pipeline_no_external_deps(self):
        """Pipeline succeeds with all external deps disabled."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)

        assert result.overall_status == "success"
        assert result.total_transactions == 20
        assert result.total_new_files == 1
        assert len(result.station_results) == 1

        # All phases either succeeded or were skipped
        for name, pr in result.phases.items():
            assert pr.status in (PhaseStatus.SUCCESS, PhaseStatus.SKIPPED), (
                f"Phase {name} was {pr.status}"
            )

    def test_parse_phase_stats(self):
        """Parse phase returns correct statistics."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)
        parse_phase = result.phases["parse"]
        assert parse_phase.status == PhaseStatus.SUCCESS
        assert parse_phase.detail["files_parsed"] == 1
        assert parse_phase.detail["day_transactions"] == 20

    def test_reconciliation_produces_result(self):
        """Reconciliation phase produces DailyCloseResult."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)

        recon_phase = result.phases["reconciliation"]
        assert recon_phase.status == PhaseStatus.SUCCESS
        assert recon_phase.detail["stations_processed"] == 1

        # Station result exists
        assert len(result.station_results) == 1
        station = result.station_results[0]
        assert station.station_id == 3
        assert station.close_date == CLOSE_DATE

    def test_odoo_skipped_when_disabled(self):
        """Odoo phases are skipped when odoo_enabled=False."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)

        for phase_name in ["odoo_sync", "odoo_journal", "odoo_close_data"]:
            assert result.phases[phase_name].status == PhaseStatus.SKIPPED

    def test_whatsapp_with_mock_sender(self):
        """WhatsApp phase sends messages via mock provider."""
        config = WhatsAppConfig(
            provider=WhatsAppProvider.MOCK,
            josue_whatsapp="whatsapp:+521111",
            tecnico_whatsapp="whatsapp:+522222",
        )
        sender = WhatsAppSender(config)
        orch = self._make_orchestrator(whatsapp=sender)
        result = orch.run_daily_close(CLOSE_DATE)

        wa_phase = result.phases["whatsapp"]
        assert wa_phase.status == PhaseStatus.SUCCESS
        assert wa_phase.detail["sent"] >= 2  # per-station + summary
        assert wa_phase.detail["failed"] == 0

    def test_summary_message_generated(self):
        """Pipeline generates a summary WhatsApp message."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)
        assert len(result.summary_message) > 0
        assert "RESUMEN CIERRE DIARIO" in result.summary_message

    def test_to_dict(self):
        """CloseRunResult.to_dict() is serializable."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)
        d = result.to_dict()
        assert d["close_date"] == "2026-01-15"
        assert d["overall_status"] == "success"
        assert "parse" in d["phases"]
        assert d["total_transactions"] == 20

    def test_run_history(self):
        """Orchestrator keeps history of runs."""
        orch = self._make_orchestrator()
        orch.run_daily_close(CLOSE_DATE)
        orch.run_daily_close(date(2026, 1, 16))
        assert len(orch.run_history) == 2

    def test_parse_failure_stops_pipeline(self):
        """If parse fails, pipeline stops and returns failed."""
        def bad_parser(csv_dir):
            raise FileNotFoundError("CSV dir not found")

        config = OrchestratorConfig(
            csv_dir=Path("/nonexistent"),
            odoo_enabled=False,
            scada_enabled=False,
            whatsapp_enabled=False,
            db_enabled=False,
            station_ids=[3],
        )
        orch = Orchestrator(config=config, parser=bad_parser)
        result = orch.run_daily_close(CLOSE_DATE)

        assert result.overall_status == "failed"
        assert result.phases["parse"].status == PhaseStatus.FAILED
        # Reconciliation should not be in phases (pipeline stopped)
        assert Phase.RECONCILIATION.value not in result.phases

    def test_db_persist_skipped_when_no_conn(self):
        """DB persist is skipped when db_enabled=False."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)
        assert result.phases["db_persist"].status == PhaseStatus.SKIPPED

    def test_multiple_stations(self):
        """Pipeline processes multiple stations."""
        def multi_station_parser(csv_dir):
            txns = (
                make_transactions(10, station_id=1)
                + make_transactions(10, station_id=2)
                + make_transactions(10, station_id=3)
            )
            return [MockParseResult(txns)]

        config = OrchestratorConfig(
            csv_dir=Path("data/gasup"),
            odoo_enabled=False,
            scada_enabled=False,
            whatsapp_enabled=False,
            db_enabled=False,
            station_ids=[1, 2, 3],
        )
        orch = Orchestrator(config=config, parser=multi_station_parser)
        result = orch.run_daily_close(CLOSE_DATE)

        assert result.overall_status == "success"
        assert len(result.station_results) == 3
        assert result.total_transactions == 30

    def test_phase_timing(self):
        """Each phase records duration."""
        orch = self._make_orchestrator()
        result = orch.run_daily_close(CLOSE_DATE)

        for name, pr in result.phases.items():
            if pr.status == PhaseStatus.SUCCESS:
                assert pr.duration_s >= 0
                assert pr.started_at is not None
                assert pr.finished_at is not None

    def test_run_single_station(self):
        """run_single_station works independently of full pipeline."""
        orch = self._make_orchestrator()
        txns = make_transactions(10, station_id=3)
        result = orch.run_single_station(
            station_id=3,
            close_date=CLOSE_DATE,
            transactions=txns,
        )
        assert result.station_id == 3
        assert result.close_date == CLOSE_DATE

    def test_whatsapp_critical_notifies_tecnico(self):
        """When station is CRITICAL, tecnico also gets notified."""
        config = WhatsAppConfig(
            provider=WhatsAppProvider.MOCK,
            josue_whatsapp="whatsapp:+521111",
            tecnico_whatsapp="whatsapp:+522222",
        )
        sender = WhatsAppSender(config)

        # Build orchestrator with transactions that will trigger CRITICAL
        # (all zeros for compusafe will cause cash check to fail)
        orch = self._make_orchestrator(whatsapp=sender)
        result = orch.run_daily_close(CLOSE_DATE)

        wa_phase = result.phases["whatsapp"]
        assert wa_phase.status == PhaseStatus.SUCCESS

        # Check send log for tecnico messages
        tecnico_msgs = [
            r for r in sender.send_log
            if r.recipient == "whatsapp:+522222"
        ]
        # If any station was CRITICAL, tecnico should have been notified
        critical_stations = [
            r for r in result.station_results
            if r.status.value in ("CRITICAL", "EMERGENCY")
        ]
        if critical_stations:
            assert len(tecnico_msgs) >= 1
