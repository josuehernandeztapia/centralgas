"""
Pydantic models for GasUp transaction data.
Handles both pre-2023 (17 cols) and post-2023 (13 cols) schemas.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

# AGS timezone: UTC-6 (no DST in Aguascalientes)
CST_OFFSET = timedelta(hours=-6)
CST = timezone(CST_OFFSET)

# Plate regex: standard AGS format A#####A, with exceptions for legacy plates
PLACA_REGEX_STRICT = re.compile(r"^A\d{5}A$")
PLACA_REGEX_LOOSE = re.compile(r"^A[\dA-Z]{3,6}A?$")

# Known 331 plates loaded at runtime from client DB
KNOWN_PLATES: set[str] = set()


class SchemaVersion(str, Enum):
    PRE_2023 = "pre2023"   # 17 columns: Fecha de venta, ..., Desc_medio_pago, Segmento Localizado
    POST_2023 = "post2023"  # 13 columns: Fecha de ventas, ..., Suma de precio_total, Recaudo


class MedioPago(str, Enum):
    EFECTIVO = "EFECTIVO"
    PREPAGO = "PREPAGO"
    CREDITO = "CREDITO"
    TARJETA_DEBITO = "TARJETA_DEBITO"
    TARJETA_CREDITO = "TARJETA_CREDITO"
    BONOS_EDS = "BONOS_EDS"
    DESCONOCIDO = "DESCONOCIDO"


class AnomalyType(str, Enum):
    HIGH_VOLUME = "ANOMALY_HIGH_VOLUME"        # >55 lt
    VERY_HIGH_VOLUME = "ANOMALY_VERY_HIGH"     # >100 lt — casi seguro error
    ZERO_VOLUME = "ANOMALY_ZERO"               # 0 lt
    NEGATIVE = "ANOMALY_NEGATIVE"               # monto negativo
    UNKNOWN_PLATE = "ANOMALY_UNKNOWN_PLATE"     # placa no en catalogo
    HIGH_FREQUENCY = "ANOMALY_HIGH_FREQUENCY"   # 4+ cargas/dia mismo vehiculo
    SUSPICIOUS_GAP = "ANOMALY_SUSPICIOUS_GAP"   # <3h gap con ambas >20lt
    INVALID_PLATE = "ANOMALY_INVALID_PLATE"     # no matchea regex
    PRICE_OUTLIER = "ANOMALY_PRICE_OUTLIER"     # PVP fuera de rango $7-$28


class Anomaly(BaseModel):
    type: AnomalyType
    detail: str


# Station mapping: NatGas name → our station_id
STATION_MAP: dict[str, int] = {
    "EDS Nacozari": 3,
    "EDS Siglo XXI": 1,
    "EDS José Maria Chávez": 1,
    "EDS Jose Maria Chavez": 1,
    "EDS José María Chávez": 1,
    "EDS Poniente": 1,
    "EDS OJO CALIENTE": 2,
    "EDS Ojo Caliente": 2,
    "EDS Lázaro Cárdenas": None,  # Fuera de AGS — skip or flag
    "EDS Abasto": None,
    "EDS MALECON": None,
    "EDS Periférico Sur": None,
}

MEDIO_PAGO_MAP: dict[str, MedioPago] = {
    "Efectivo": MedioPago.EFECTIVO,
    "Prepago": MedioPago.PREPAGO,
    "Crédito": MedioPago.CREDITO,
    "Credito": MedioPago.CREDITO,
    "Tarjeta Débito": MedioPago.TARJETA_DEBITO,
    "Tarjeta Debito": MedioPago.TARJETA_DEBITO,
    "Tarjeta Crédito": MedioPago.TARJETA_CREDITO,
    "Tarjeta Credito": MedioPago.TARJETA_CREDITO,
    "Bonos EDS": MedioPago.BONOS_EDS,
}


class TransactionRaw(BaseModel):
    """Raw transaction as parsed from CSV, before normalization."""

    # Source tracking
    source_file: str
    source_row: int
    schema_version: SchemaVersion

    # Common fields (both schemas)
    fecha: str                          # Raw date string
    estacion: str
    plaza: str
    gasup_placa_id: Optional[int] = None
    placa: str
    modelo: Optional[str] = None
    marca: Optional[str] = None
    linea: Optional[str] = None
    fecha_conversion: Optional[str] = None
    litros: Decimal
    pvp: Decimal
    total_mxn: Decimal

    # Pre-2023 only
    valor_recaudo: Optional[Decimal] = Decimal("0")
    recaudo_pagado: Optional[Decimal] = Decimal("0")
    venta_mas_recaudo: Optional[Decimal] = None
    medio_pago: Optional[str] = None
    segmento: Optional[str] = None

    # Post-2023 only
    recaudo: Optional[Decimal] = Decimal("0")


class TransactionNormalized(BaseModel):
    """Normalized transaction ready for DB insertion."""

    # Source
    source_file: str
    source_hash: Optional[str] = None
    source_row: int
    schema_version: SchemaVersion

    # Location
    station_id: Optional[int] = None
    station_natgas: str
    plaza: str = "AGUASCALIENTES"

    # Timestamps
    timestamp_utc: datetime
    timestamp_local: datetime

    # Vehicle
    gasup_placa_id: Optional[int] = None
    placa: str
    modelo: Optional[str] = None
    marca: Optional[str] = None
    linea: Optional[str] = None
    fecha_conversion: Optional[datetime] = None

    # Transaction
    litros: Decimal
    pvp: Decimal
    total_mxn: Decimal
    recaudo_valor: Decimal = Decimal("0")
    recaudo_pagado: Decimal = Decimal("0")
    venta_mas_recaudo: Optional[Decimal] = None
    medio_pago: Optional[MedioPago] = None
    segmento: Optional[str] = None

    # Derived
    kg: Decimal                         # litros * 0.717
    nm3: Decimal                        # litros * 1.0
    ingreso_neto: Decimal               # total_mxn / 1.16
    iva: Decimal                        # total_mxn - ingreso_neto

    # State
    odoo_move_id: Optional[int] = None
    reconciled: bool = False
    anomalies: list[Anomaly] = Field(default_factory=list)


def normalize_medio_pago(raw: Optional[str]) -> Optional[MedioPago]:
    if raw is None:
        return None
    return MEDIO_PAGO_MAP.get(raw.strip(), MedioPago.DESCONOCIDO)


def detect_anomalies(txn: TransactionNormalized) -> list[Anomaly]:
    """Detect anomalies based on thresholds from 377K historical transactions."""
    anomalies: list[Anomaly] = []

    # Volume checks
    if txn.litros == 0:
        anomalies.append(Anomaly(
            type=AnomalyType.ZERO_VOLUME,
            detail="Carga de 0 litros — error de sistema o cancelacion"
        ))
    elif txn.litros > 100:
        anomalies.append(Anomaly(
            type=AnomalyType.VERY_HIGH_VOLUME,
            detail=f"Carga de {txn.litros} lt — casi seguro error de medicion"
        ))
    elif txn.litros > 55:
        anomalies.append(Anomaly(
            type=AnomalyType.HIGH_VOLUME,
            detail=f"Carga de {txn.litros} lt (P95=44.9 lt). Posible tanque grande o error."
        ))

    # Negative amounts
    if txn.total_mxn < 0:
        anomalies.append(Anomaly(
            type=AnomalyType.NEGATIVE,
            detail=f"Monto negativo: ${txn.total_mxn} MXN"
        ))

    # Unknown plate
    if KNOWN_PLATES and txn.placa not in KNOWN_PLATES:
        anomalies.append(Anomaly(
            type=AnomalyType.UNKNOWN_PLATE,
            detail=f"Placa {txn.placa} no encontrada en catalogo de {len(KNOWN_PLATES)} placas"
        ))

    # Invalid plate format
    if not PLACA_REGEX_LOOSE.match(txn.placa):
        anomalies.append(Anomaly(
            type=AnomalyType.INVALID_PLATE,
            detail=f"Placa {txn.placa} no matchea formato AGS"
        ))

    # Price outlier (historical range: $7.59 — $27.98)
    if txn.pvp < 5 or txn.pvp > 30:
        anomalies.append(Anomaly(
            type=AnomalyType.PRICE_OUTLIER,
            detail=f"PVP ${txn.pvp}/lt fuera de rango historico $7-$28"
        ))

    return anomalies
