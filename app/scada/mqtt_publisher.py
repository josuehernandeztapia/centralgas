"""
MQTT Publisher — HU-4.3

Publishes SCADA telemetry and alarm events to Mosquitto broker.

Topic structure:
  centralgas/{station_id}/scada/analog/{tag_id}   — analog readings
  centralgas/{station_id}/scada/alarm/{tag_id}     — alarm events
  centralgas/{station_id}/scada/alert/{severity}   — processed alerts
  centralgas/{station_id}/scada/status             — connection heartbeat

Messages are JSON payloads with QoS 1 (at-least-once).
Retained messages on status topic for last-known-state.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from app.scada.alert_engine import AlertAction, AlertEngine
from app.scada.plc_reader import AlarmEvent, BasePLCReader, ScadaReading

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

@dataclass
class MQTTConfig:
    """MQTT connection parameters, loaded from env vars."""
    host: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    client_id: str = "centralgas-scada"
    keepalive: int = 60
    qos: int = 1
    retain_status: bool = True
    base_topic: str = "centralgas"

    @classmethod
    def from_env(cls) -> MQTTConfig:
        return cls(
            host=os.getenv("MQTT_HOST", "localhost"),
            port=int(os.getenv("MQTT_PORT", "1883")),
            username=os.getenv("MQTT_USER", ""),
            password=os.getenv("MQTT_PASSWORD", ""),
        )


# ============================================================
# MQTT Publisher
# ============================================================

class MQTTPublisher:
    """
    Publishes SCADA data to MQTT broker.

    Handles connection lifecycle, automatic reconnect,
    and structured topic hierarchy.

    Usage:
        publisher = MQTTPublisher(config, station_id=3)
        publisher.connect()
        publisher.publish_readings(readings)
        publisher.publish_alarms(events)
        publisher.publish_alerts(actions)
        publisher.disconnect()
    """

    def __init__(self, config: MQTTConfig, station_id: int = 3):
        self.config = config
        self.station_id = station_id
        self._client = None
        self._connected = False
        self._lock = threading.Lock()
        self._publish_count = 0
        self._error_count = 0
        self._last_publish: Optional[datetime] = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict:
        return {
            "connected": self._connected,
            "publish_count": self._publish_count,
            "error_count": self._error_count,
            "last_publish": self._last_publish.isoformat() if self._last_publish else None,
        }

    # ---- Topic helpers ----

    def _topic(self, *parts: str) -> str:
        """Build topic: centralgas/{station_id}/{parts...}"""
        return "/".join([self.config.base_topic, str(self.station_id), *parts])

    # ---- Connection lifecycle ----

    def connect(self) -> bool:
        """Connect to MQTT broker. Returns True if successful."""
        try:
            import paho.mqtt.client as mqtt

            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.config.client_id,
            )

            if self.config.username:
                self._client.username_pw_set(
                    self.config.username, self.config.password
                )

            # Callbacks
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_publish = self._on_publish

            # Last Will — if we disconnect unexpectedly
            self._client.will_set(
                self._topic("scada", "status"),
                payload=json.dumps({"status": "offline", "ts": datetime.now(timezone.utc).isoformat()}),
                qos=self.config.qos,
                retain=True,
            )

            self._client.connect(
                self.config.host,
                self.config.port,
                keepalive=self.config.keepalive,
            )
            self._client.loop_start()

            # Wait briefly for connection callback
            for _ in range(30):  # 3 seconds max
                if self._connected:
                    break
                time.sleep(0.1)

            return self._connected

        except ImportError:
            logger.error("paho-mqtt not installed — pip install paho-mqtt")
            return False
        except Exception as e:
            logger.error(f"MQTT connect failed: {e}")
            return False

    def disconnect(self):
        """Graceful disconnect with offline status."""
        if self._client:
            try:
                # Publish offline status before disconnecting
                self._publish_json(
                    self._topic("scada", "status"),
                    {"status": "offline", "ts": datetime.now(timezone.utc).isoformat()},
                    retain=True,
                )
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.warning(f"MQTT disconnect error: {e}")
            finally:
                self._connected = False

    # ---- Callbacks ----

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self._connected = True
            logger.info(f"MQTT connected to {self.config.host}:{self.config.port}")

            # Publish online status
            self._publish_json(
                self._topic("scada", "status"),
                {
                    "status": "online",
                    "station_id": self.station_id,
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                retain=True,
            )
        else:
            logger.error(f"MQTT connect failed: reason_code={reason_code}")
            self._connected = False

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self._connected = False
        if reason_code != 0:
            logger.warning(f"MQTT unexpected disconnect: reason_code={reason_code}")

    def _on_publish(self, client, userdata, mid, reason_codes=None, properties=None):
        with self._lock:
            self._publish_count += 1
            self._last_publish = datetime.now(timezone.utc)

    # ---- Publishing ----

    def _publish_json(self, topic: str, payload: dict, retain: bool = False) -> bool:
        """Publish a JSON payload to topic."""
        if not self._client:
            return False
        try:
            msg = json.dumps(payload, default=str)
            result = self._client.publish(
                topic, msg,
                qos=self.config.qos,
                retain=retain,
            )
            return result.rc == 0
        except Exception as e:
            logger.warning(f"MQTT publish error on {topic}: {e}")
            with self._lock:
                self._error_count += 1
            return False

    def publish_readings(self, readings: list[ScadaReading]) -> int:
        """
        Publish analog readings. Returns count of successfully published messages.

        Topic: centralgas/{station}/scada/analog/{tag_id}
        """
        if not self._connected:
            logger.warning("MQTT not connected — skipping readings publish")
            return 0

        published = 0
        for reading in readings:
            topic = self._topic("scada", "analog", reading.tag_id)
            if self._publish_json(topic, reading.to_mqtt_payload()):
                published += 1

        logger.debug(f"Published {published}/{len(readings)} analog readings")
        return published

    def publish_alarms(self, events: list[AlarmEvent]) -> int:
        """
        Publish alarm events. Returns count of successfully published messages.

        Topic: centralgas/{station}/scada/alarm/{tag_id}
        """
        if not self._connected:
            logger.warning("MQTT not connected — skipping alarm publish")
            return 0

        published = 0
        for event in events:
            topic = self._topic("scada", "alarm", event.tag_id)
            if self._publish_json(topic, event.to_mqtt_payload()):
                published += 1

        logger.debug(f"Published {published}/{len(events)} alarm events")
        return published

    def publish_alerts(self, actions: list[AlertAction]) -> int:
        """
        Publish processed alerts by severity level.

        Topic: centralgas/{station}/scada/alert/{severity}
        """
        if not self._connected:
            logger.warning("MQTT not connected — skipping alert publish")
            return 0

        published = 0
        for action in actions:
            topic = self._topic("scada", "alert", action.severity.value)
            payload = {
                "alert_id": action.alert_id,
                "tag_id": action.tag_id,
                "tag_name": action.tag_name,
                "severity": action.severity.value,
                "value": action.value,
                "threshold": action.threshold,
                "unit": action.unit,
                "message": action.message,
                "recipients": action.recipients,
                "is_escalation": action.is_escalation,
                "is_recurrence": action.is_recurrence,
                "whatsapp_message": action.to_whatsapp_message(),
                "ts": action.timestamp.isoformat(),
            }
            if self._publish_json(topic, payload):
                published += 1

        logger.debug(f"Published {published}/{len(actions)} alerts")
        return published

    def publish_heartbeat(self) -> bool:
        """Publish a status heartbeat with current stats."""
        return self._publish_json(
            self._topic("scada", "status"),
            {
                "status": "online",
                "station_id": self.station_id,
                **self.stats,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            retain=True,
        )


# ============================================================
# SCADA Loop — ties reader + engine + publisher together
# ============================================================

class ScadaLoop:
    """
    Main SCADA polling loop.

    Reads PLC → runs alert engine → publishes to MQTT.
    Designed to run in a background thread or async task.

    Usage:
        loop = ScadaLoop(reader, engine, publisher)
        loop.start()        # blocking — call from thread
        loop.request_stop() # signal graceful shutdown
    """

    def __init__(
        self,
        reader: BasePLCReader,
        engine: AlertEngine,
        publisher: MQTTPublisher,
        analog_interval_s: float = 30.0,
        alarm_interval_s: float = 5.0,
    ):
        self.reader = reader
        self.engine = engine
        self.publisher = publisher
        self.analog_interval_s = analog_interval_s
        self.alarm_interval_s = alarm_interval_s
        self._stop_event = threading.Event()
        self._cycle_count = 0

    def request_stop(self):
        """Signal the loop to stop after current cycle."""
        self._stop_event.set()

    @property
    def cycle_count(self) -> int:
        return self._cycle_count

    def run_once(self) -> dict:
        """
        Execute one full SCADA cycle:
          1. Read analog tags
          2. Read alarm tags
          3. Process through alert engine
          4. Publish everything to MQTT

        Returns summary dict of what happened.
        """
        result = {
            "readings": 0,
            "alarms": 0,
            "alerts": 0,
            "published_readings": 0,
            "published_alarms": 0,
            "published_alerts": 0,
            "errors": [],
        }

        # 1. Read analog
        try:
            readings = self.reader.read_analog_tags()
            result["readings"] = len(readings)
        except Exception as e:
            logger.error(f"Failed to read analog tags: {e}")
            result["errors"].append(f"analog_read: {e}")
            readings = []

        # 2. Read alarms
        try:
            alarms = self.reader.read_alarm_tags()
            result["alarms"] = len(alarms)
        except Exception as e:
            logger.error(f"Failed to read alarm tags: {e}")
            result["errors"].append(f"alarm_read: {e}")
            alarms = []

        # 3. Alert engine
        alerts = []
        try:
            if readings:
                alerts.extend(self.engine.process_readings(readings))
            if alarms:
                alerts.extend(self.engine.process_alarms(alarms))
            result["alerts"] = len(alerts)
        except Exception as e:
            logger.error(f"Alert engine error: {e}")
            result["errors"].append(f"alert_engine: {e}")

        # 4. Publish
        if self.publisher.is_connected:
            result["published_readings"] = self.publisher.publish_readings(readings)
            result["published_alarms"] = self.publisher.publish_alarms(alarms)
            result["published_alerts"] = self.publisher.publish_alerts(alerts)
            self.publisher.publish_heartbeat()

        self._cycle_count += 1
        return result

    def start(self):
        """
        Blocking loop — call from a thread.

        Reads alarms every alarm_interval_s, analog every analog_interval_s.
        """
        logger.info(
            f"SCADA loop starting: analog every {self.analog_interval_s}s, "
            f"alarms every {self.alarm_interval_s}s"
        )

        last_analog = 0.0
        last_alarm = 0.0

        while not self._stop_event.is_set():
            now = time.monotonic()

            # Alarm check (more frequent)
            if now - last_alarm >= self.alarm_interval_s:
                try:
                    alarms = self.reader.read_alarm_tags()
                    if alarms:
                        alerts = self.engine.process_alarms(alarms)
                        if self.publisher.is_connected:
                            self.publisher.publish_alarms(alarms)
                            if alerts:
                                self.publisher.publish_alerts(alerts)
                except Exception as e:
                    logger.error(f"Alarm cycle error: {e}")
                last_alarm = now

            # Analog check (less frequent)
            if now - last_analog >= self.analog_interval_s:
                try:
                    readings = self.reader.read_analog_tags()
                    if readings:
                        alerts = self.engine.process_readings(readings)
                        if self.publisher.is_connected:
                            self.publisher.publish_readings(readings)
                            if alerts:
                                self.publisher.publish_alerts(alerts)
                    self.publisher.publish_heartbeat()
                except Exception as e:
                    logger.error(f"Analog cycle error: {e}")
                last_analog = now
                self._cycle_count += 1

            # Sleep in small increments so stop_event is responsive
            self._stop_event.wait(timeout=min(self.alarm_interval_s, 1.0))

        logger.info(f"SCADA loop stopped after {self._cycle_count} cycles")
