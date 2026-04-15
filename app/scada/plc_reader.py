"""
PLC Reader — HU-4.1, HU-4.2

Reads compressor ACKIA data from Smart S7-200 PLC via snap7 (ISOTCP).
Reads dispenser STAR data via Modbus RTU (pymodbus).

Both protocols are abstracted behind a unified interface:
  reader.read_analog_tags() → list[ScadaReading]
  reader.read_alarm_tags()  → list[AlarmEvent]

For environments without physical PLC (dev, test, demo),
use MockPLCReader which generates realistic synthetic data.
"""

from __future__ import annotations

import logging
import struct
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from app.scada.tags import (
    ALL_ALARM_TAGS,
    ALARM_BYTE_OFFSETS,
    AlarmTag,
    AnalogTag,
    COMPRESSOR_ALARM_TAGS,
    COMPRESSOR_ANALOG_TAGS,
    DISPENSER_TAGS,
    Severity,
    TagSource,
)

logger = logging.getLogger(__name__)


# ============================================================
# Data containers
# ============================================================

@dataclass
class ScadaReading:
    """A single analog reading from PLC or dispenser."""
    tag_id: str
    tag_name: str
    source: TagSource
    raw_value: int
    scaled_value: float
    unit: str
    quality: int = 0             # 0=OK, 1=suspect, 2=bad
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mqtt_payload(self) -> dict:
        return {
            "tag": self.tag_id,
            "value": self.scaled_value,
            "raw": self.raw_value,
            "unit": self.unit,
            "quality": self.quality,
            "ts": self.timestamp_utc.isoformat(),
        }


@dataclass
class AlarmEvent:
    """A single alarm state change."""
    tag_id: str
    tag_name: str
    severity: Severity
    active: bool                 # True = alarm ON, False = cleared
    description: str
    siemens_addr: str
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mqtt_payload(self) -> dict:
        return {
            "tag": self.tag_id,
            "alarm": self.tag_name,
            "severity": self.severity.value,
            "active": self.active,
            "description": self.description,
            "addr": self.siemens_addr,
            "ts": self.timestamp_utc.isoformat(),
        }


# ============================================================
# Abstract base reader
# ============================================================

class BasePLCReader(ABC):
    """Abstract interface for PLC/dispenser reading."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection. Returns True if successful."""
        ...

    @abstractmethod
    def disconnect(self):
        """Close connection."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        ...

    @abstractmethod
    def read_analog_tags(self) -> list[ScadaReading]:
        """Read all analog tags (compressor + dispenser)."""
        ...

    @abstractmethod
    def read_alarm_tags(self) -> list[AlarmEvent]:
        """Read all alarm bits and return active alarms."""
        ...


# ============================================================
# Real PLC Reader (snap7 + pymodbus)
# ============================================================

class PLCReader(BasePLCReader):
    """
    Production reader for ACKIA compressor (snap7) + STAR dispenser (Modbus).

    Usage:
        reader = PLCReader(plc_ip="192.168.1.253", plc_port=102)
        reader.connect()
        readings = reader.read_analog_tags()
        alarms = reader.read_alarm_tags()
    """

    def __init__(
        self,
        plc_ip: str = "192.168.1.253",
        plc_port: int = 102,
        plc_rack: int = 0,
        plc_slot: int = 0,
        modbus_port: str = "/dev/ttyUSB0",
        modbus_addr: int = 1,
        modbus_baud: int = 19200,
    ):
        self.plc_ip = plc_ip
        self.plc_port = plc_port
        self.plc_rack = plc_rack
        self.plc_slot = plc_slot
        self.modbus_port = modbus_port
        self.modbus_addr = modbus_addr
        self.modbus_baud = modbus_baud
        self._snap7_client = None
        self._modbus_client = None
        self._connected_plc = False
        self._connected_modbus = False
        self._prev_alarm_state: dict[str, bool] = {}

    def connect(self) -> bool:
        ok = True
        # snap7 for compressor
        try:
            import snap7
            self._snap7_client = snap7.client.Client()
            self._snap7_client.connect(self.plc_ip, self.plc_rack, self.plc_slot, self.plc_port)
            self._connected_plc = self._snap7_client.get_connected()
            logger.info(f"snap7 connected to {self.plc_ip}:{self.plc_port}")
        except Exception as e:
            logger.error(f"snap7 connection failed: {e}")
            self._connected_plc = False
            ok = False

        # pymodbus for dispenser
        try:
            from pymodbus.client import ModbusSerialClient
            self._modbus_client = ModbusSerialClient(
                port=self.modbus_port,
                baudrate=self.modbus_baud,
                parity="N",
                stopbits=1,
                bytesize=8,
                timeout=3,
            )
            self._connected_modbus = self._modbus_client.connect()
            logger.info(f"Modbus connected to {self.modbus_port}")
        except Exception as e:
            logger.error(f"Modbus connection failed: {e}")
            self._connected_modbus = False
            # Not fatal — dispenser may not be present
        return ok

    def disconnect(self):
        if self._snap7_client:
            try:
                self._snap7_client.disconnect()
            except Exception:
                pass
        if self._modbus_client:
            try:
                self._modbus_client.close()
            except Exception:
                pass
        self._connected_plc = False
        self._connected_modbus = False

    def is_connected(self) -> bool:
        return self._connected_plc

    def read_analog_tags(self) -> list[ScadaReading]:
        readings = []
        now = datetime.now(timezone.utc)

        # Compressor: read VW area (V-memory words)
        if self._connected_plc and self._snap7_client:
            for tag in COMPRESSOR_ANALOG_TAGS:
                try:
                    # VW address: strip "VW" prefix, read 2 bytes from V-area
                    addr = int(tag.siemens_addr.replace("VW", ""))
                    data = self._snap7_client.read_area(0x84, 0, addr, 2)  # 0x84 = V area
                    raw = struct.unpack(">h", bytes(data))[0]  # Big-endian signed int16
                    scaled = raw * tag.scale

                    readings.append(ScadaReading(
                        tag_id=tag.tag_id, tag_name=tag.name,
                        source=tag.source, raw_value=raw,
                        scaled_value=round(scaled, 4), unit=tag.unit,
                        quality=0, timestamp_utc=now,
                    ))
                except Exception as e:
                    logger.warning(f"Failed to read {tag.tag_id}: {e}")
                    readings.append(ScadaReading(
                        tag_id=tag.tag_id, tag_name=tag.name,
                        source=tag.source, raw_value=0,
                        scaled_value=0.0, unit=tag.unit,
                        quality=2, timestamp_utc=now,
                    ))

        # Dispenser: read Modbus holding registers
        if self._connected_modbus and self._modbus_client:
            for tag in DISPENSER_TAGS:
                try:
                    # Modbus address offset (40001-based)
                    addr = tag.modbus_addr - 40001
                    result = self._modbus_client.read_holding_registers(addr, 1, slave=self.modbus_addr)
                    if not result.isError():
                        raw = result.registers[0]
                        scaled = raw * tag.scale
                        readings.append(ScadaReading(
                            tag_id=tag.tag_id, tag_name=tag.name,
                            source=tag.source, raw_value=raw,
                            scaled_value=round(scaled, 4), unit=tag.unit,
                            quality=0, timestamp_utc=now,
                        ))
                except Exception as e:
                    logger.warning(f"Failed to read {tag.tag_id}: {e}")

        return readings

    def read_alarm_tags(self) -> list[AlarmEvent]:
        if not self._connected_plc or not self._snap7_client:
            return []

        events = []
        now = datetime.now(timezone.utc)

        # Read M-area bytes that contain alarm bits
        for byte_offset in ALARM_BYTE_OFFSETS:
            try:
                data = self._snap7_client.read_area(0x83, 0, byte_offset, 1)  # 0x83 = M area
                byte_val = data[0]

                # Check each alarm in this byte
                for tag in COMPRESSOR_ALARM_TAGS:
                    if tag.byte_offset != byte_offset:
                        continue

                    bit_active = bool(byte_val & (1 << tag.bit_offset))
                    prev_state = self._prev_alarm_state.get(tag.tag_id, False)

                    # Only emit event on state change
                    if bit_active != prev_state:
                        events.append(AlarmEvent(
                            tag_id=tag.tag_id, tag_name=tag.name,
                            severity=tag.severity, active=bit_active,
                            description=tag.description,
                            siemens_addr=tag.siemens_addr,
                            timestamp_utc=now,
                        ))
                        self._prev_alarm_state[tag.tag_id] = bit_active

            except Exception as e:
                logger.warning(f"Failed to read M{byte_offset}: {e}")

        return events


# ============================================================
# Mock PLC Reader (for dev/test/demo)
# ============================================================

class MockPLCReader(BasePLCReader):
    """
    Synthetic data generator that mimics real PLC behavior.
    Generates realistic compressor telemetry + alarm patterns.

    Usage:
        reader = MockPLCReader()
        reader.connect()
        readings = reader.read_analog_tags()
        alarms = reader.read_alarm_tags()
    """

    def __init__(self, scenario: str = "normal"):
        """
        Scenarios:
          "normal"    — compressor running fine, no alarms
          "warming"   — oil temp rising toward 80°C
          "low_inlet" — Naturgy pressure dropping
          "alarm"     — multiple alarms active
          "offline"   — PLC unreachable
        """
        self.scenario = scenario
        self._connected = False
        self._cycle = 0
        self._prev_alarm_state: dict[str, bool] = {}
        import random
        self._rng = random.Random(42)

    def connect(self) -> bool:
        if self.scenario == "offline":
            self._connected = False
            return False
        self._connected = True
        return True

    def disconnect(self):
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def read_analog_tags(self) -> list[ScadaReading]:
        if not self._connected:
            return []

        self._cycle += 1
        now = datetime.now(timezone.utc)
        readings = []

        # Base values for "normal" operation
        base_values = {
            "COMP_DESPLAZAMIENTO_1": (800, 1200),
            "COMP_DESPLAZAMIENTO_2": (800, 1200),
            "COMP_P_ACEITE_1":       (40, 60),     # 0.40-0.60 MPa
            "COMP_P_ACEITE_2":       (40, 60),
            "COMP_P_ENTRADA":        (8, 12),       # 0.08-0.12 MPa (Naturgy ~10 bar)
            "COMP_P_MEDIA":          (500, 700),    # 5.0-7.0 MPa
            "COMP_P_ALTA":           (2000, 2500),  # 20-25 MPa
            "COMP_P_PROTECCION_HP":  (2200, 2600),  # 22-26 MPa
            "COMP_T_ACEITE":         (550, 700),    # 55-70°C
            "COMP_NIVEL_AGUA":       (20, 35),      # 20-35 cm
            "COMP_NIVEL_ACEITE":     (20, 35),
        }

        # Scenario overrides
        if self.scenario == "warming":
            # T_aceite climbing each cycle
            t = min(550 + self._cycle * 5, 860)
            base_values["COMP_T_ACEITE"] = (t, t + 10)

        elif self.scenario == "low_inlet":
            # P_entrada dropping
            p = max(12 - self._cycle, 2)
            base_values["COMP_P_ENTRADA"] = (p, p + 1)

        elif self.scenario == "alarm":
            base_values["COMP_T_ACEITE"] = (850, 870)     # >85°C
            base_values["COMP_P_ENTRADA"] = (2, 3)         # Very low
            base_values["COMP_NIVEL_AGUA"] = (3, 5)         # Low

        for tag in COMPRESSOR_ANALOG_TAGS:
            lo, hi = base_values.get(tag.tag_id, (100, 200))
            raw = self._rng.randint(lo, hi)
            scaled = round(raw * tag.scale, 4)

            readings.append(ScadaReading(
                tag_id=tag.tag_id, tag_name=tag.name,
                source=tag.source, raw_value=raw,
                scaled_value=scaled, unit=tag.unit,
                quality=0, timestamp_utc=now,
            ))

        # Dispenser mock
        disp_values = {
            "DISP_STATUS": (0, 4),
            "DISP_TOTALIZER_QTY_L": (30000, 65000),
            "DISP_TOTALIZER_QTY_H": (100, 200),
            "DISP_TOTALIZER_MNY_L": (40000, 65000),
            "DISP_TOTALIZER_MNY_H": (200, 400),
            "DISP_PRESSURE": (1800, 2500),           # 180-250 bar
            "DISP_FLOW": (50, 120),                   # 5-12 L/min
            "DISP_VALVE_STATUS": (0, 3),
            "DISP_LAST_QTY_L": (1500, 4500),         # 15-45 L
            "DISP_LAST_QTY_H": (0, 0),
            "DISP_LAST_MONEY_L": (20000, 60000),     # $200-$600
            "DISP_LAST_MONEY_H": (0, 0),
            "DISP_LAST_UNIT_PRICE": (1399, 1399),    # $13.99
            "DISP_PRICE": (1399, 1399),
            "DISP_DENSITY": (717, 717),               # 0.717 kg/m³
        }

        for tag in DISPENSER_TAGS:
            lo, hi = disp_values.get(tag.tag_id, (0, 100))
            raw = self._rng.randint(lo, hi)
            scaled = round(raw * tag.scale, 4)
            readings.append(ScadaReading(
                tag_id=tag.tag_id, tag_name=tag.name,
                source=tag.source, raw_value=raw,
                scaled_value=scaled, unit=tag.unit,
                quality=0, timestamp_utc=now,
            ))

        return readings

    def read_alarm_tags(self) -> list[AlarmEvent]:
        if not self._connected:
            return []

        now = datetime.now(timezone.utc)
        events = []

        # Build alarm state based on scenario
        active_alarms: set[str] = set()

        if self.scenario == "warming" and self._cycle > 5:
            active_alarms.add("ALM_T_ACEITE_80")
        if self.scenario == "warming" and self._cycle > 10:
            active_alarms.add("ALM_T_ACEITE_85")

        if self.scenario == "low_inlet":
            if self._cycle > 3:
                active_alarms.add("ALM_BAJA_P_ENTRADA_W")
            if self._cycle > 6:
                active_alarms.add("ALM_BAJA_P_ENTRADA_S")

        if self.scenario == "alarm":
            active_alarms.update([
                "ALM_T_ACEITE_85",
                "ALM_BAJA_P_ENTRADA_S",
                "ALM_NIVEL_AGUA_S",
                "ALM_SOBRECARGA_VENT1",
            ])

        # Emit events for state changes
        for tag in COMPRESSOR_ALARM_TAGS:
            is_active = tag.tag_id in active_alarms
            prev = self._prev_alarm_state.get(tag.tag_id, False)

            if is_active != prev:
                events.append(AlarmEvent(
                    tag_id=tag.tag_id, tag_name=tag.name,
                    severity=tag.severity, active=is_active,
                    description=tag.description,
                    siemens_addr=tag.siemens_addr,
                    timestamp_utc=now,
                ))
                self._prev_alarm_state[tag.tag_id] = is_active

        return events
