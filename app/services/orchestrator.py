"""
Cron Orchestrator — the nightly close pipeline.

Runs at 23:00 (configurable) and executes the full daily close sequence:

  1. Parse   — Scan data/ for new GasUp CSV files → TransactionNormalized
  2. Odoo    — Sync clients + create journal entries + fetch close data
  3. Recon   — Run 7-block reconciliation for each station
  4. Persist — Save reconciliation results to PostgreSQL
  5. Notify  — Send WhatsApp messages (per-station + summary)
  6. SCADA   — Fetch daily SCADA totalizador delta for cross-validation

Each step is idempotent and individually retryable.
The orchestrator logs every phase with timing and error details.

Usage:
    orch = Orchestrator(config)
    result = await orch.run_daily_close(date.today())
    # or from worker:
    orch.run_daily_close_sync(date.today())
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

@dataclass
class OrchestratorConfig:
    """Configuration for the nightly close pipeline."""
    # Data directories
    csv_dir: Path = field(default_factory=lambda: Path("data/gasup"))
    output_dir: Path = field(default_factory=lambda: Path("output"))

    # Schedule
    close_hour: int = 23     # 23:00 local time
    close_minute: int = 0
    timezone: str = "America/Mexico_City"

    # Feature flags
    odoo_enabled: bool = True
    scada_enabled: bool = True
    whatsapp_enabled: bool = True
    db_enabled: bool = True

    # Retry
    max_retries: int = 3
    retry_delay_s: float = 5.0

    # Stations to process
    station_ids: list[int] = field(default_factory=lambda: [1, 2, 3])

    @classmethod
    def from_env(cls) -> OrchestratorConfig:
        return cls(
            csv_dir=Path(os.getenv("CSV_DIR", "data/gasup")),
            output_dir=Path(os.getenv("OUTPUT_DIR", "output")),
            close_hour=int(os.getenv("CLOSE_HOUR", "23")),
            close_minute=int(os.getenv("CLOSE_MINUTE", "0")),
            odoo_enabled=os.getenv("ODOO_ENABLED", "true").lower() == "true",
            scada_enabled=os.getenv("SCADA_ENABLED", "true").lower() == "true",
            whatsapp_enabled=os.getenv("WHATSAPP_ENABLED", "true").lower() == "true",
            db_enabled=os.getenv("DB_ENABLED", "true").lower() == "true",
        )


# ============================================================
# Phase tracking
# ============================================================

class Phase(str, Enum):
    PARSE = "parse"
    ODOO_SYNC = "odoo_sync"
    ODOO_JOURNAL = "odoo_journal"
    ODOO_CLOSE_DATA = "odoo_close_data"
    RECONCILIATION = "reconciliation"
    RETENTION = "retention"
    DB_PERSIST = "db_persist"
    WHATSAPP = "whatsapp"
    SCADA_TOTAL = "scada_total"


class PhaseStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PhaseResult:
    phase: Phase
    status: PhaseStatus = PhaseStatus.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_s: float = 0.0
    detail: dict = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def elapsed(self) -> str:
        return f"{self.duration_s:.1f}s"


@dataclass
class CloseRunResult:
    """Complete result of one nightly close run."""
    close_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    phases: dict[str, PhaseResult] = field(default_factory=dict)
    total_transactions: int = 0
    total_new_files: int = 0
    station_results: list = field(default_factory=list)
    overall_status: str = "pending"  # pending, success, partial, failed
    summary_message: str = ""

    @property
    def total_duration_s(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    def to_dict(self) -> dict:
        return {
            "close_date": self.close_date.isoformat(),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "total_duration_s": self.total_duration_s,
            "overall_status": self.overall_status,
            "total_transactions": self.total_transactions,
            "total_new_files": self.total_new_files,
            "phases": {
                name: {
                    "status": pr.status.value,
                    "duration_s": pr.duration_s,
                    "error": pr.error,
                    "detail": pr.detail,
                }
                for name, pr in self.phases.items()
            },
            "stations": len(self.station_results),
        }


# ============================================================
# Orchestrator
# ============================================================

class Orchestrator:
    """
    Nightly close pipeline orchestrator.

    Dependencies are injected so we can test with mocks:
      - parser: module with parse_directory()
      - odoo_client: OdooClient instance (or None)
      - whatsapp_sender: WhatsAppSender instance (or None)
      - db_conn: psycopg2 connection (or None)
    """

    def __init__(
        self,
        config: OrchestratorConfig,
        parser=None,
        odoo_client=None,
        whatsapp_sender=None,
        db_conn=None,
        market_profiles: dict[str, dict] | None = None,
    ):
        self.config = config
        self.parser = parser
        self.odoo = odoo_client
        self.whatsapp = whatsapp_sender
        self.db = db_conn
        self.market_profiles = market_profiles  # from Smart Loader
        self._run_history: list[CloseRunResult] = []
        self._previous_churn_stages: dict = {}  # persisted between runs

    @property
    def run_history(self) -> list[CloseRunResult]:
        return list(self._run_history)

    # ---- Phase runner (timing + error capture) ----

    def _run_phase(
        self,
        run: CloseRunResult,
        phase: Phase,
        fn,
        *args,
        skip_if: bool = False,
        **kwargs,
    ) -> PhaseResult:
        """Execute a phase function with timing, logging, and error capture."""
        pr = PhaseResult(phase=phase)
        run.phases[phase.value] = pr

        if skip_if:
            pr.status = PhaseStatus.SKIPPED
            pr.detail = {"reason": "disabled in config"}
            logger.info(f"  [{phase.value}] SKIPPED (disabled)")
            return pr

        pr.status = PhaseStatus.RUNNING
        pr.started_at = datetime.now(timezone.utc)
        logger.info(f"  [{phase.value}] Starting...")

        try:
            result = fn(*args, **kwargs)
            pr.status = PhaseStatus.SUCCESS
            if isinstance(result, dict):
                pr.detail = result
            logger.info(f"  [{phase.value}] OK ({pr.elapsed})")
            return pr
        except Exception as e:
            pr.status = PhaseStatus.FAILED
            pr.error = str(e)
            logger.error(f"  [{phase.value}] FAILED: {e}")
            return pr
        finally:
            pr.finished_at = datetime.now(timezone.utc)
            pr.duration_s = (pr.finished_at - pr.started_at).total_seconds()

    # ---- Pipeline phases ----

    def phase_parse(self, close_date: date) -> dict:
        """
        Phase 1: Parse CSV files from data/gasup/.

        Returns dict with parse stats.
        """
        from app.parsers.gasup import parse_directory

        parser_mod = self.parser or parse_directory

        if callable(parser_mod):
            results = parser_mod(self.config.csv_dir)
        else:
            results = parser_mod.parse_directory(self.config.csv_dir)

        all_txns = []
        new_files = 0
        total_rows = 0
        for pr in results:
            all_txns.extend(pr.transactions)
            total_rows += pr.row_count
            new_files += 1

        # Filter to close_date
        day_txns = [t for t in all_txns if t.timestamp_local.date() == close_date]

        self._current_all_txns = all_txns
        self._current_day_txns = day_txns

        return {
            "files_parsed": new_files,
            "total_rows": total_rows,
            "all_transactions": len(all_txns),
            "day_transactions": len(day_txns),
            "close_date": close_date.isoformat(),
        }

    def phase_odoo_sync(self) -> dict:
        """Phase 2a: Sync clients to Odoo."""
        if not self.odoo:
            return {"skipped": "no odoo client"}

        # Extract unique clients from transactions
        clients_seen = {}
        for txn in self._current_all_txns:
            if txn.placa and txn.placa not in clients_seen:
                clients_seen[txn.placa] = {
                    "placa": txn.placa,
                    "nombre": txn.nombre_conductor or "",
                }

        stats = self.odoo.sync_all_clients(list(clients_seen.values()))
        return stats

    def phase_odoo_journal(self, close_date: date) -> dict:
        """Phase 2b: Create daily batch journal entry in Odoo."""
        if not self.odoo:
            return {"skipped": "no odoo client"}

        results = {}
        for station_id in self.config.station_ids:
            station_txns = [
                t for t in self._current_day_txns
                if t.station_id == station_id
            ]
            if not station_txns:
                results[f"station_{station_id}"] = "no_transactions"
                continue

            try:
                entry_id = self.odoo.create_daily_batch_entry(
                    station_txns, close_date, station_id
                )
                results[f"station_{station_id}"] = {
                    "entry_id": entry_id,
                    "txn_count": len(station_txns),
                }
            except Exception as e:
                results[f"station_{station_id}"] = {"error": str(e)}

        return results

    def phase_odoo_close_data(self, close_date: date) -> dict:
        """Phase 2c: Fetch Compusafe, bank, TPV data from Odoo for reconciliation."""
        if not self.odoo:
            return {}

        close_data = {}
        for station_id in self.config.station_ids:
            try:
                data = self.odoo.get_daily_close_data(close_date, station_id)
                close_data[station_id] = data
            except Exception as e:
                logger.warning(f"Could not fetch Odoo close data for station {station_id}: {e}")
                close_data[station_id] = {}

        self._odoo_close_data = close_data
        return {"stations_fetched": len(close_data)}

    def phase_reconciliation(self, close_date: date) -> dict:
        """
        Phase 3: Run 7-block reconciliation for each station.

        Returns reconciliation summary.
        """
        from app.services.reconciliation import (
            run_daily_close,
            format_whatsapp_message,
            format_summary_message,
        )

        results = []
        for station_id in self.config.station_ids:
            # Get Odoo-supplied data for this station
            odoo_data = getattr(self, "_odoo_close_data", {}).get(station_id, {})

            try:
                result = run_daily_close(
                    station_id=station_id,
                    close_date=close_date,
                    transactions=self._current_day_txns,
                    **odoo_data,
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Reconciliation failed for station {station_id}: {e}")

        self._recon_results = results
        self._summary_message = format_summary_message(results) if results else ""

        statuses = [r.status.value for r in results]
        return {
            "stations_processed": len(results),
            "statuses": statuses,
            "summary_len": len(self._summary_message),
        }

    def phase_retention(self, close_date: date) -> dict:
        """
        Phase 3b: Run client retention analysis on all parsed transactions.

        Uses market_profiles from Smart Loader for segment classification
        and expected consumption context.

        Returns retention report summary.
        """
        from app.services.retention import (
            run_retention_analysis,
            format_retention_whatsapp,
        )

        report, profiles = run_retention_analysis(
            transactions=self._current_all_txns,
            reference_date=close_date,
            existing_profiles=self.market_profiles,
            previous_stages=self._previous_churn_stages,
        )

        # Store for WhatsApp phase and future runs
        self._retention_report = report
        self._retention_profiles = profiles
        self._retention_whatsapp_msg = format_retention_whatsapp(report)

        # Update previous stages for next run's transition detection
        from app.models.client import ChurnStage
        self._previous_churn_stages = {
            placa: p.churn_stage for placa, p in profiles.items()
        }

        return {
            "total_clients": report.total_clients,
            "active": report.active_clients,
            "green": report.green_count,
            "yellow": report.yellow_count,
            "orange": report.orange_count,
            "red": report.red_count,
            "alerts": len(report.alerts),
            "revenue_at_risk_mxn": float(report.revenue_at_risk_mxn),
            "revenue_lost_mxn": float(report.revenue_lost_mxn),
        }

    def phase_db_persist(self, close_date: date) -> dict:
        """Phase 4: Save reconciliation results to PostgreSQL."""
        if not self.db:
            return {"skipped": "no db connection"}

        saved = 0
        for result in self._recon_results:
            try:
                checks_json = json.dumps(
                    [
                        {
                            "check_id": c.check_id,
                            "description": c.description,
                            "result": c.result.value,
                            "expected": str(c.expected),
                            "actual": str(c.actual),
                            "difference": str(c.difference),
                        }
                        for c in result.checks
                    ],
                    default=str,
                )

                cursor = self.db.cursor()
                cursor.execute(
                    """
                    INSERT INTO reconciliation_runs
                        (station_id, run_date, status, source_a_litros, source_a_mxn,
                         source_c_nm3, source_d_mxn, checks_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, run_date)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        source_a_litros = EXCLUDED.source_a_litros,
                        source_a_mxn = EXCLUDED.source_a_mxn,
                        checks_json = EXCLUDED.checks_json
                    """,
                    (
                        result.station_id,
                        close_date,
                        result.status.value,
                        float(result.gasup.total_litros),
                        float(result.gasup.total_mxn),
                        None,  # scada_nm3 — from SCADA phase
                        None,  # odoo_total — future
                        checks_json,
                    ),
                )
                saved += 1
            except Exception as e:
                logger.error(f"DB persist failed for station {result.station_id}: {e}")

        if saved > 0:
            self.db.commit()

        return {"saved": saved, "total": len(self._recon_results)}

    def phase_whatsapp(self) -> dict:
        """Phase 5: Send WhatsApp notifications."""
        if not self.whatsapp:
            return {"skipped": "no whatsapp sender"}

        from app.services.reconciliation import format_whatsapp_message

        sent = 0
        failed = 0

        # Per-station messages to Josue
        for result in self._recon_results:
            msg = format_whatsapp_message(result)
            send_result = self.whatsapp.send("josue", msg)
            if send_result.success:
                sent += 1
            else:
                failed += 1

        # Summary to Josue
        if self._summary_message:
            send_result = self.whatsapp.send("josue", self._summary_message)
            if send_result.success:
                sent += 1
            else:
                failed += 1

        # If any CRITICAL/EMERGENCY, also notify tecnico
        critical_msgs = [
            r for r in self._recon_results
            if r.status.value in ("CRITICAL", "EMERGENCY")
        ]
        for result in critical_msgs:
            msg = format_whatsapp_message(result)
            send_result = self.whatsapp.send("tecnico", msg)
            if send_result.success:
                sent += 1
            else:
                failed += 1

        # Retention report (if available)
        retention_msg = getattr(self, "_retention_whatsapp_msg", "")
        if retention_msg:
            send_result = self.whatsapp.send("josue", retention_msg)
            if send_result.success:
                sent += 1
            else:
                failed += 1

            # Send individual high-priority retention alerts
            retention_report = getattr(self, "_retention_report", None)
            if retention_report:
                for alert in retention_report.alerts[:5]:  # Top 5 alerts
                    if alert.priority <= 2:  # Only ORANGE/RED
                        alert_msg = alert.to_whatsapp_message()
                        send_result = self.whatsapp.send("josue", alert_msg)
                        if send_result.success:
                            sent += 1
                        else:
                            failed += 1

        return {"sent": sent, "failed": failed}

    # ---- Main pipeline ----

    def run_daily_close(self, close_date: date) -> CloseRunResult:
        """
        Execute the full nightly close pipeline for a given date.

        Steps:
          1. Parse CSVs
          2. Odoo sync + journal + close data
          3. Reconciliation
          4. DB persist
          5. WhatsApp notify

        Returns CloseRunResult with all phase outcomes.
        """
        run = CloseRunResult(close_date=close_date)
        self._current_all_txns = []
        self._current_day_txns = []
        self._odoo_close_data = {}
        self._recon_results = []
        self._summary_message = ""
        self._retention_report = None
        self._retention_profiles = {}
        self._retention_whatsapp_msg = ""

        logger.info(f"{'='*60}")
        logger.info(f"DAILY CLOSE PIPELINE — {close_date.isoformat()}")
        logger.info(f"{'='*60}")

        # Phase 1: Parse
        p1 = self._run_phase(run, Phase.PARSE, self.phase_parse, close_date)
        run.total_transactions = p1.detail.get("day_transactions", 0)
        run.total_new_files = p1.detail.get("files_parsed", 0)

        if p1.status == PhaseStatus.FAILED:
            run.overall_status = "failed"
            run.finished_at = datetime.now(timezone.utc)
            self._run_history.append(run)
            return run

        # Phase 2: Odoo (sync, journal, close data)
        self._run_phase(
            run, Phase.ODOO_SYNC, self.phase_odoo_sync,
            skip_if=not self.config.odoo_enabled or not self.odoo,
        )
        self._run_phase(
            run, Phase.ODOO_JOURNAL, self.phase_odoo_journal, close_date,
            skip_if=not self.config.odoo_enabled or not self.odoo,
        )
        self._run_phase(
            run, Phase.ODOO_CLOSE_DATA, self.phase_odoo_close_data, close_date,
            skip_if=not self.config.odoo_enabled or not self.odoo,
        )

        # Phase 3: Reconciliation
        p3 = self._run_phase(run, Phase.RECONCILIATION, self.phase_reconciliation, close_date)
        run.station_results = self._recon_results
        run.summary_message = self._summary_message

        # Phase 3b: Retention analysis (uses all transactions, not just today's)
        self._retention_report = None
        self._retention_profiles = {}
        self._retention_whatsapp_msg = ""
        self._run_phase(
            run, Phase.RETENTION, self.phase_retention, close_date,
        )

        # Phase 4: DB persist
        self._run_phase(
            run, Phase.DB_PERSIST, self.phase_db_persist, close_date,
            skip_if=not self.config.db_enabled or not self.db,
        )

        # Phase 5: WhatsApp
        self._run_phase(
            run, Phase.WHATSAPP, self.phase_whatsapp,
            skip_if=not self.config.whatsapp_enabled or not self.whatsapp,
        )

        # Determine overall status
        phase_statuses = [pr.status for pr in run.phases.values()]
        if all(s in (PhaseStatus.SUCCESS, PhaseStatus.SKIPPED) for s in phase_statuses):
            run.overall_status = "success"
        elif any(s == PhaseStatus.FAILED for s in phase_statuses):
            # Check if reconciliation at least succeeded
            recon_phase = run.phases.get(Phase.RECONCILIATION.value)
            if recon_phase and recon_phase.status == PhaseStatus.SUCCESS:
                run.overall_status = "partial"
            else:
                run.overall_status = "failed"
        else:
            run.overall_status = "partial"

        run.finished_at = datetime.now(timezone.utc)
        self._run_history.append(run)

        logger.info(f"{'='*60}")
        logger.info(
            f"PIPELINE COMPLETE — {run.overall_status.upper()} "
            f"in {run.total_duration_s:.1f}s"
        )
        logger.info(f"  Transactions: {run.total_transactions}")
        logger.info(f"  Stations: {len(run.station_results)}")
        for name, pr in run.phases.items():
            logger.info(f"  {name}: {pr.status.value} ({pr.elapsed})")
        logger.info(f"{'='*60}")

        return run

    # ---- Convenience ----

    def run_single_station(
        self, station_id: int, close_date: date, transactions=None, **kwargs
    ):
        """
        Run reconciliation for a single station without the full pipeline.
        Useful for manual re-runs or debugging.
        """
        from app.services.reconciliation import run_daily_close, format_whatsapp_message

        if transactions is None:
            transactions = self._current_day_txns

        result = run_daily_close(
            station_id=station_id,
            close_date=close_date,
            transactions=transactions,
            **kwargs,
        )
        return result
