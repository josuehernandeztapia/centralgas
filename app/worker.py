"""
Worker Entry Point — python -m app.worker

The long-running background process that:
  1. Runs SCADA polling loop (every 5s alarms, every 30s analog)
  2. Schedules the nightly close at 23:00 CST
  3. Handles graceful shutdown on SIGTERM/SIGINT

Architecture:
  main thread   → scheduler loop (checks every 60s if it's close time)
  thread 1      → SCADA polling loop (reader → engine → MQTT)

Both threads respect a shared stop_event for graceful shutdown.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from datetime import date, datetime

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "info").upper(),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")


def get_local_now():
    """Get current time in Mexico City timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Mexico_City"))
    except ImportError:
        # Python 3.8 fallback
        from datetime import timezone, timedelta
        cst = timezone(timedelta(hours=-6))
        return datetime.now(cst)


def build_scada_loop(stop_event: threading.Event):
    """Build and start the SCADA polling loop in a thread."""
    try:
        from app.scada.plc_reader import PLCReader, MockPLCReader
        from app.scada.alert_engine import AlertEngine
        from app.scada.mqtt_publisher import MQTTConfig, MQTTPublisher, ScadaLoop

        # Use real PLC reader if snap7 available, else mock
        plc_ip = os.getenv("PLC_STAR_IP", "192.168.1.253")
        plc_port = int(os.getenv("PLC_STAR_PORT", "102"))
        use_mock = os.getenv("SCADA_MOCK", "false").lower() == "true"

        if use_mock:
            logger.info("SCADA using MockPLCReader (SCADA_MOCK=true)")
            reader = MockPLCReader(scenario="normal")
        else:
            try:
                reader = PLCReader(plc_ip=plc_ip, plc_port=plc_port)
            except Exception:
                logger.warning("snap7/pymodbus not available, falling back to MockPLCReader")
                reader = MockPLCReader(scenario="normal")

        if not reader.connect():
            logger.error("SCADA reader failed to connect — SCADA loop disabled")
            return None

        engine = AlertEngine(station_id=3)

        mqtt_config = MQTTConfig.from_env()
        publisher = MQTTPublisher(mqtt_config, station_id=3)

        if not publisher.connect():
            logger.warning("MQTT connect failed — SCADA loop will run without publishing")

        analog_interval = float(os.getenv("SCADA_READ_INTERVAL_S", "30"))
        alarm_interval = float(os.getenv("SCADA_ALARM_INTERVAL_S", "5"))

        loop = ScadaLoop(
            reader=reader,
            engine=engine,
            publisher=publisher,
            analog_interval_s=analog_interval,
            alarm_interval_s=alarm_interval,
        )

        # Wire stop_event
        loop._stop_event = stop_event

        thread = threading.Thread(target=loop.start, name="scada-loop", daemon=True)
        thread.start()
        logger.info(f"SCADA loop started (analog={analog_interval}s, alarm={alarm_interval}s)")
        return loop

    except Exception as e:
        logger.error(f"Failed to build SCADA loop: {e}")
        return None


def build_orchestrator():
    """Build the nightly close orchestrator with all dependencies."""
    from app.services.orchestrator import Orchestrator, OrchestratorConfig
    from app.services.whatsapp import WhatsAppSender

    config = OrchestratorConfig.from_env()

    # Odoo client (optional)
    odoo = None
    if config.odoo_enabled:
        try:
            from app.services.odoo_client import OdooClient, OdooConfig
            odoo_config = OdooConfig(
                url=os.getenv("ODOO_URL", "http://localhost:8069"),
                db=os.getenv("ODOO_DB", "centralgas_erp"),
                username=os.getenv("ODOO_USER", "admin"),
                password=os.getenv("ODOO_PASSWORD", "admin"),
            )
            odoo = OdooClient(odoo_config)
            if odoo.authenticate():
                logger.info("Odoo connected")
            else:
                logger.warning("Odoo auth failed — running without Odoo")
                odoo = None
        except Exception as e:
            logger.warning(f"Odoo unavailable: {e}")

    # WhatsApp sender
    whatsapp = WhatsAppSender.from_env()
    logger.info(f"WhatsApp provider: {whatsapp.config.provider.value}")

    # DB connection (optional)
    db = None
    if config.db_enabled:
        try:
            import psycopg2
            db = psycopg2.connect(os.getenv("DATABASE_URL", ""))
            db.autocommit = False
            logger.info("PostgreSQL connected")
        except Exception as e:
            logger.warning(f"PostgreSQL unavailable: {e}")

    return Orchestrator(
        config=config,
        odoo_client=odoo,
        whatsapp_sender=whatsapp,
        db_conn=db,
    )


def main():
    """Main worker loop."""
    logger.info("="*60)
    logger.info("CENTRAL GAS WORKER — Starting up")
    logger.info("="*60)

    stop_event = threading.Event()

    # Graceful shutdown
    def handle_signal(signum, frame):
        logger.info(f"Received signal {signum} — shutting down...")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Start SCADA loop in background thread
    scada_enabled = os.getenv("SCADA_ENABLED", "true").lower() == "true"
    if scada_enabled:
        scada_loop = build_scada_loop(stop_event)
    else:
        logger.info("SCADA loop disabled (SCADA_ENABLED=false)")
        scada_loop = None

    # Build orchestrator
    orchestrator = build_orchestrator()

    close_hour = int(os.getenv("CLOSE_HOUR", "23"))
    close_minute = int(os.getenv("CLOSE_MINUTE", "0"))
    last_close_date: date | None = None

    logger.info(f"Nightly close scheduled at {close_hour:02d}:{close_minute:02d} CST")
    logger.info("Worker ready — entering main loop")

    while not stop_event.is_set():
        now = get_local_now()

        # Check if it's close time and we haven't run today
        if (
            now.hour == close_hour
            and now.minute >= close_minute
            and now.date() != last_close_date
        ):
            close_date = now.date()
            logger.info(f"Triggering nightly close for {close_date}")

            try:
                result = orchestrator.run_daily_close(close_date)
                last_close_date = close_date
                logger.info(
                    f"Nightly close complete: {result.overall_status} "
                    f"({result.total_duration_s:.1f}s)"
                )
            except Exception as e:
                logger.error(f"Nightly close FAILED: {e}")
                last_close_date = close_date  # Don't retry this date

        # Sleep 60s between checks (but wake on stop_event)
        stop_event.wait(timeout=60)

    # Graceful shutdown
    logger.info("Shutting down...")
    if scada_loop:
        scada_loop.request_stop()
    logger.info("Worker stopped")


if __name__ == "__main__":
    main()
