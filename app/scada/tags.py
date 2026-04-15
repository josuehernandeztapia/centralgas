"""
SCADA Tag Definitions — ACKIA Compressor + STAR Dispenser

Source: Mapeo_SCADA_ACKIA_200524.xlsx + Modbus protocol STAR-1.pdf
PLC: Smart S7-200, ISOTCP 192.168.1.253:102
Dispenser: STAR PMII, Modbus RS485, 19200bps

Tag naming convention:
  COMP_*  = Compressor ACKIA (snap7)
  DISP_*  = Dispenser STAR (Modbus)
  ALM_*   = Alarm bits (snap7 coils)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


class TagSource(str, Enum):
    COMPRESSOR = "COMPRESSOR"
    DISPENSER = "DISPENSER"


@dataclass(frozen=True)
class AnalogTag:
    """Definition of an analog (numeric) SCADA tag."""
    tag_id: str                    # e.g. "COMP_P_ENTRADA"
    name: str                      # Human-readable
    source: TagSource
    siemens_addr: str              # VW1034 or "40028"
    modbus_addr: int               # 4005 or 40028
    modbus_func: int               # 3 = read holding register
    data_type: str                 # "int16" or "word"
    unit: str                      # MPa, °C, cm, L, bar
    scale: float                   # raw × scale = real value
    warn_low: Optional[float] = None
    warn_high: Optional[float] = None
    crit_low: Optional[float] = None
    crit_high: Optional[float] = None


@dataclass(frozen=True)
class AlarmTag:
    """Definition of a boolean alarm tag."""
    tag_id: str                    # e.g. "ALM_SOBRECARGA_BOMBA"
    name: str                      # Human-readable
    siemens_addr: str              # M15.0
    byte_offset: int               # Byte index in M-area (15, 16, 17, 0, 1)
    bit_offset: int                # Bit within byte (0-7)
    coil_addr: int                 # Modbus coil 1-34
    severity: Severity             # What severity when TRUE
    description: str               # What it means
    escalation_minutes: int = 30   # WARNING→CRITICAL after N min


# ============================================================
# COMPRESSOR ACKIA — 11 Analog Tags (VW1004–VW1072)
# ============================================================

COMPRESSOR_ANALOG_TAGS: list[AnalogTag] = [
    AnalogTag(
        tag_id="COMP_DESPLAZAMIENTO_1",
        name="Desplazamiento #1",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1004", modbus_addr=4001, modbus_func=3,
        data_type="int16", unit="", scale=1.0,
    ),
    AnalogTag(
        tag_id="COMP_DESPLAZAMIENTO_2",
        name="Desplazamiento #2",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1014", modbus_addr=4002, modbus_func=3,
        data_type="int16", unit="", scale=1.0,
    ),
    AnalogTag(
        tag_id="COMP_P_ACEITE_1",
        name="Presión aceite #1",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1022", modbus_addr=4003, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        warn_high=1.0, crit_high=1.5,
    ),
    AnalogTag(
        tag_id="COMP_P_ACEITE_2",
        name="Presión aceite #2",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1028", modbus_addr=4004, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        warn_high=1.0, crit_high=1.5,
    ),
    AnalogTag(
        tag_id="COMP_P_ENTRADA",
        name="Presión entrada",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1034", modbus_addr=4005, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        warn_low=0.05, crit_low=0.03,  # Problema Naturgy: baja presión 10 bar
    ),
    AnalogTag(
        tag_id="COMP_P_MEDIA",
        name="Presión media",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1040", modbus_addr=4006, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        warn_high=8.0, crit_high=10.0,
    ),
    AnalogTag(
        tag_id="COMP_P_ALTA",
        name="Presión alta",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1046", modbus_addr=4007, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        warn_high=25.0, crit_high=28.0,
    ),
    AnalogTag(
        tag_id="COMP_P_PROTECCION_HP",
        name="Protección alta presión",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1052", modbus_addr=4008, modbus_func=3,
        data_type="int16", unit="MPa", scale=0.01,
        crit_high=30.0,
    ),
    AnalogTag(
        tag_id="COMP_T_ACEITE",
        name="Temperatura aceite",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1060", modbus_addr=4009, modbus_func=3,
        data_type="int16", unit="°C", scale=0.1,
        warn_high=80.0, crit_high=85.0,  # M17.0 y M17.1
    ),
    AnalogTag(
        tag_id="COMP_NIVEL_AGUA",
        name="Nivel agua",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1066", modbus_addr=4010, modbus_func=3,
        data_type="int16", unit="cm", scale=1.0,
        warn_low=10.0, crit_low=5.0,
    ),
    AnalogTag(
        tag_id="COMP_NIVEL_ACEITE",
        name="Nivel aceite",
        source=TagSource.COMPRESSOR,
        siemens_addr="VW1072", modbus_addr=4011, modbus_func=3,
        data_type="int16", unit="cm", scale=1.0,
        warn_low=10.0, crit_low=5.0,
    ),
]


# ============================================================
# COMPRESSOR ACKIA — 34 Alarm Bits (M15.0–M1.6)
# ============================================================

COMPRESSOR_ALARM_TAGS: list[AlarmTag] = [
    # M15: Sobrecargas + anomalías punto cero + sobrepresión hidráulica
    AlarmTag("ALM_SOBRECARGA_BOMBA",    "Sobrecarga bomba agua",          "M15.0", 15, 0, 1,  Severity.CRITICAL,  "Motor bomba agua en sobrecarga", 15),
    AlarmTag("ALM_SOBRECARGA_VENT1",    "Sobrecarga ventilador #1",       "M15.1", 15, 1, 2,  Severity.WARNING,   "Motor ventilador 1 en sobrecarga"),
    AlarmTag("ALM_SOBRECARGA_VENT2",    "Sobrecarga ventilador #2",       "M15.2", 15, 2, 3,  Severity.WARNING,   "Motor ventilador 2 en sobrecarga"),
    AlarmTag("ALM_SOBRECARGA_VENT3",    "Sobrecarga ventilador #3",       "M15.3", 15, 3, 4,  Severity.WARNING,   "Motor ventilador 3 en sobrecarga"),
    AlarmTag("ALM_PUNTOCERO_PISTON1",   "Anomalía punto cero pistón #1",  "M15.4", 15, 4, 5,  Severity.CRITICAL,  "Pistón 1 fuera de punto cero — posible daño mecánico"),
    AlarmTag("ALM_PUNTOCERO_PISTON2",   "Anomalía punto cero pistón #2",  "M15.5", 15, 5, 6,  Severity.CRITICAL,  "Pistón 2 fuera de punto cero — posible daño mecánico"),
    AlarmTag("ALM_SOBREPRESION_HID1_W", "Sobrepresión hidráulica #1 WARN","M15.6", 15, 6, 7,  Severity.WARNING,   "Presión hidráulica #1 alta — advertencia"),
    AlarmTag("ALM_SOBREPRESION_HID1_S", "Sobrepresión hidráulica #1 STOP","M15.7", 15, 7, 8,  Severity.EMERGENCY, "Sobrepresión hidráulica #1 — SHUTDOWN", 0),

    # M16: Sobrepresión hidráulica #2 + presión entrada + media + alta
    AlarmTag("ALM_SOBREPRESION_HID2_W", "Sobrepresión hidráulica #2 WARN","M16.0", 16, 0, 9,  Severity.WARNING,   "Presión hidráulica #2 alta — advertencia"),
    AlarmTag("ALM_SOBREPRESION_HID2_S", "Sobrepresión hidráulica #2 STOP","M16.1", 16, 1, 10, Severity.EMERGENCY, "Sobrepresión hidráulica #2 — SHUTDOWN", 0),
    AlarmTag("ALM_BAJA_P_ENTRADA_W",    "Baja presión entrada WARN",      "M16.2", 16, 2, 11, Severity.WARNING,   "Presión entrada baja — posible problema Naturgy"),
    AlarmTag("ALM_BAJA_P_ENTRADA_S",    "Baja presión entrada STOP",      "M16.3", 16, 3, 12, Severity.CRITICAL,  "Presión entrada crítica — PARADA. Verificar suministro Naturgy", 0),
    AlarmTag("ALM_SOBREPRESION_MEDIA_W","Sobrepresión media WARN",        "M16.4", 16, 4, 13, Severity.WARNING,   "Presión media alta — advertencia"),
    AlarmTag("ALM_SOBREPRESION_MEDIA_S","Sobrepresión media STOP",        "M16.5", 16, 5, 14, Severity.EMERGENCY, "Sobrepresión media — SHUTDOWN", 0),
    AlarmTag("ALM_SOBREPRESION_ALTA_S", "Sobrepresión alta STOP",         "M16.6", 16, 6, 15, Severity.EMERGENCY, "Sobrepresión alta — SHUTDOWN INMEDIATO", 0),
    AlarmTag("ALM_SOBREPRESION_ALTA_W", "Sobrepresión alta WARN",         "M16.7", 16, 7, 16, Severity.WARNING,   "Presión alta elevada — advertencia"),

    # M17: Temperatura aceite + niveles agua/aceite + fuga
    AlarmTag("ALM_T_ACEITE_80",         "T° aceite >80°C WARN",           "M17.0", 17, 0, 17, Severity.WARNING,   "Temperatura aceite >80°C — enfriar"),
    AlarmTag("ALM_T_ACEITE_85",         "T° aceite >85°C STOP",           "M17.1", 17, 1, 18, Severity.CRITICAL,  "Temperatura aceite >85°C — PARADA", 0),
    AlarmTag("ALM_NIVEL_AGUA_W",        "Nivel agua bajo WARN",           "M17.2", 17, 2, 19, Severity.WARNING,   "Nivel agua bajo — rellenar"),
    AlarmTag("ALM_NIVEL_AGUA_S",        "Nivel agua bajo STOP",           "M17.3", 17, 3, 20, Severity.CRITICAL,  "Nivel agua crítico — PARADA", 0),
    AlarmTag("ALM_NIVEL_ACEITE_W",      "Nivel aceite bajo WARN",         "M17.4", 17, 4, 21, Severity.WARNING,   "Nivel aceite bajo — rellenar"),
    AlarmTag("ALM_NIVEL_ACEITE_S",      "Nivel aceite bajo STOP",         "M17.5", 17, 5, 22, Severity.CRITICAL,  "Nivel aceite crítico — PARADA", 0),
    AlarmTag("ALM_FUGA_ACEITE",         "Fuga mecánica aceite",           "M17.6", 17, 6, 23, Severity.EMERGENCY, "Fuga de aceite detectada — EMERGENCIA", 0),

    # M0-M1: Desconexión de sensores (11 bits)
    AlarmTag("ALM_DESC_DESPLAZ1",       "Desconexión desplazamiento #1",  "M0.4",  0,  4, 24, Severity.WARNING,   "Sensor desplazamiento 1 desconectado"),
    AlarmTag("ALM_DESC_DESPLAZ2",       "Desconexión desplazamiento #2",  "M0.5",  0,  5, 25, Severity.WARNING,   "Sensor desplazamiento 2 desconectado"),
    AlarmTag("ALM_DESC_P_ACEITE1",      "Desconexión presión aceite #1",  "M0.6",  0,  6, 26, Severity.WARNING,   "Sensor presión aceite 1 desconectado"),
    AlarmTag("ALM_DESC_P_ACEITE2",      "Desconexión presión aceite #2",  "M0.7",  0,  7, 27, Severity.WARNING,   "Sensor presión aceite 2 desconectado"),
    AlarmTag("ALM_DESC_P_ENTRADA",      "Desconexión presión entrada",    "M1.0",  1,  0, 28, Severity.CRITICAL,  "Sensor presión entrada desconectado — CIEGO"),
    AlarmTag("ALM_DESC_P_MEDIA",        "Desconexión presión media",      "M1.1",  1,  1, 29, Severity.CRITICAL,  "Sensor presión media desconectado"),
    AlarmTag("ALM_DESC_P_ALTA",         "Desconexión presión alta",       "M1.2",  1,  2, 30, Severity.CRITICAL,  "Sensor alta presión desconectado — PELIGRO"),
    AlarmTag("ALM_DESC_PROTECCION_HP",  "Desconexión protección HP",      "M1.3",  1,  3, 31, Severity.EMERGENCY, "Sensor protección HP desconectado — SIN PROTECCIÓN", 0),
    AlarmTag("ALM_DESC_T_ACEITE",       "Desconexión temp aceite",        "M1.4",  1,  4, 32, Severity.WARNING,   "Sensor temperatura aceite desconectado"),
    AlarmTag("ALM_DESC_NIVEL_AGUA",     "Desconexión nivel agua",         "M1.5",  1,  5, 33, Severity.WARNING,   "Sensor nivel agua desconectado"),
    AlarmTag("ALM_DESC_NIVEL_ACEITE",   "Desconexión nivel aceite",       "M1.6",  1,  6, 34, Severity.WARNING,   "Sensor nivel aceite desconectado"),
]


# ============================================================
# DISPENSER STAR — 27 Modbus Registers (40020–40046)
# ============================================================

class DispenserStatus(int, Enum):
    READY = 0
    FILLING = 1
    ERROR = 2
    WAITING_AUTH = 3
    END_OF_CHARGE = 4


class ValveStatus(int, Enum):
    ALL_CLOSED = 0
    LOW_PRESSURE = 1
    MEDIUM_PRESSURE = 2
    HIGH_PRESSURE = 3


DISPENSER_TAGS: list[AnalogTag] = [
    AnalogTag("DISP_SW_VERSION",      "Software version",       TagSource.DISPENSER, "40020", 40020, 3, "word", "", 1.0),
    AnalogTag("DISP_ENABLE_FILLING",  "Enable filling",         TagSource.DISPENSER, "40021", 40021, 3, "word", "", 1.0),
    AnalogTag("DISP_STATUS",          "Dispenser status",       TagSource.DISPENSER, "40022", 40022, 3, "word", "", 1.0),
    AnalogTag("DISP_ERROR_CODE",      "Error code",             TagSource.DISPENSER, "40023", 40023, 3, "word", "", 1.0),
    AnalogTag("DISP_TOTALIZER_QTY_L", "Totalizer qty (low)",    TagSource.DISPENSER, "40024", 40024, 3, "word", "L", 1.0),
    AnalogTag("DISP_TOTALIZER_QTY_H", "Totalizer qty (high)",   TagSource.DISPENSER, "40025", 40025, 3, "word", "L", 1.0),
    AnalogTag("DISP_TOTALIZER_MNY_L", "Totalizer money (low)",  TagSource.DISPENSER, "40026", 40026, 3, "word", "$", 1.0),
    AnalogTag("DISP_TOTALIZER_MNY_H", "Totalizer money (high)", TagSource.DISPENSER, "40027", 40027, 3, "word", "$", 1.0),
    AnalogTag("DISP_PRESSURE",        "Pressure",               TagSource.DISPENSER, "40028", 40028, 3, "word", "bar", 0.1),
    AnalogTag("DISP_FLOW",            "Flow rate",              TagSource.DISPENSER, "40029", 40029, 3, "word", "L/min", 0.1),
    AnalogTag("DISP_VALVE_STATUS",    "Valve status",           TagSource.DISPENSER, "40030", 40030, 3, "word", "", 1.0),
    AnalogTag("DISP_LAST_QTY_L",      "Last qty delivered (L)", TagSource.DISPENSER, "40031", 40031, 3, "word", "L", 0.01),
    AnalogTag("DISP_LAST_QTY_H",      "Last qty delivered (H)", TagSource.DISPENSER, "40032", 40032, 3, "word", "L", 0.01),
    AnalogTag("DISP_LAST_MONEY_L",    "Last money (L)",         TagSource.DISPENSER, "40033", 40033, 3, "word", "$", 0.01),
    AnalogTag("DISP_LAST_MONEY_H",    "Last money (H)",         TagSource.DISPENSER, "40034", 40034, 3, "word", "$", 0.01),
    AnalogTag("DISP_LAST_UNIT_PRICE", "Last unit price",        TagSource.DISPENSER, "40035", 40035, 3, "word", "$/L", 0.01),
    AnalogTag("DISP_PRICE",           "Current price",          TagSource.DISPENSER, "40044", 40044, 3, "word", "$/L", 0.01),
    AnalogTag("DISP_DENSITY",         "Gas density",            TagSource.DISPENSER, "40045", 40045, 3, "word", "kg/m³", 0.001),
    AnalogTag("DISP_DEVICE_ADDR",     "Device address",         TagSource.DISPENSER, "40046", 40046, 3, "word", "", 1.0),
]


# ============================================================
# Quick-access lookups
# ============================================================

ALL_ANALOG_TAGS = {t.tag_id: t for t in COMPRESSOR_ANALOG_TAGS + DISPENSER_TAGS}
ALL_ALARM_TAGS = {t.tag_id: t for t in COMPRESSOR_ALARM_TAGS}

# M-area bytes that contain alarm bits
ALARM_BYTE_OFFSETS = sorted(set(t.byte_offset for t in COMPRESSOR_ALARM_TAGS))
# → [0, 1, 15, 16, 17]
