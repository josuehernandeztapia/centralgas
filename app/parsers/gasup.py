"""
GasUp CSV Parser — HU-1.1, HU-1.2, HU-1.3, HU-1.4

Handles both pre-2023 (17 cols) and post-2023 (13 cols) schemas.
Detects schema automatically by header row.
Normalizes to unified TransactionNormalized model.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Generator, Optional

from app.models.transaction import (
    Anomaly,
    AnomalyType,
    MedioPago,
    SchemaVersion,
    STATION_MAP,
    TransactionNormalized,
    TransactionRaw,
    CST,
    detect_anomalies,
    normalize_medio_pago,
)

logger = logging.getLogger(__name__)

# UTC-6 for Aguascalientes (no DST)
UTC = timezone.utc
CST_OFFSET = timedelta(hours=-6)


# ============================================================
# Schema detection
# ============================================================

PRE_2023_MARKER = "Fecha de venta"   # sin "s" final
POST_2023_MARKER = "Fecha de ventas"  # con "s"

PRE_2023_COLUMNS = [
    "Fecha de venta", "Estación de servicio", "Plaza", "Id_placa", "Placa",
    "Desc_Modelo", "Desc_Marca", "Desc_Linea", "Fh_conversion", "Litros",
    "PVP", "Valor recaudo", "Reacaudo pagado", "TOTAL PRECIO GNV",
    "Venta total mas recaudo", "Desc_medio_pago", "Segmento Localizado",
]

POST_2023_COLUMNS = [
    "Fecha de ventas", "Estación de servicio", "Plaza", "Id_placa", "Placa",
    "Desc_Modelo", "Desc_Marca", "Desc_Linea", "Fh_conversion", "Litros",
    "PVP", "Suma de precio_total", "Recaudo",
]


def detect_schema(header_line: str) -> SchemaVersion:
    """Detect CSV schema from header line."""
    # Strip BOM
    header = header_line.lstrip("\ufeff").strip()
    if header.startswith(POST_2023_MARKER):
        return SchemaVersion.POST_2023
    elif header.startswith(PRE_2023_MARKER):
        return SchemaVersion.PRE_2023
    else:
        raise ValueError(f"Unknown schema. Header starts with: {header[:50]}")


# ============================================================
# File hashing for deduplication
# ============================================================

def file_hash(filepath: Path) -> str:
    """SHA256 hash of file contents."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ============================================================
# Date parsing
# ============================================================

def parse_date(raw: str) -> Optional[datetime]:
    """Parse date from various GasUp formats."""
    if not raw or raw.strip() == "":
        return None

    raw = raw.strip()

    # Try ISO-like: 2025-05-01 06:07:47
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ]:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue

    # Handle: "15/11/2024 02:57:04 p. m." or "30/06/2022 11:36:40 p. m."
    if "p. m." in raw or "a. m." in raw:
        cleaned = raw.replace(" p. m.", " PM").replace(" a. m.", " AM")
        for fmt in ["%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %I:%M %p"]:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue

    logger.warning(f"Could not parse date: {raw}")
    return None


def to_utc(local_dt: datetime) -> datetime:
    """Convert CST (UTC-6) to UTC. Aguascalientes has no DST."""
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=CST)
    return local_dt.astimezone(UTC)


# ============================================================
# Decimal helpers
# ============================================================

def safe_decimal(value: str, default: Decimal = Decimal("0")) -> Decimal:
    """Parse a decimal value, returning default on failure."""
    if not value or value.strip() == "":
        return default
    try:
        return Decimal(value.strip().replace(",", ""))
    except InvalidOperation:
        return default


def safe_int(value: str) -> Optional[int]:
    if not value or value.strip() == "":
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


# ============================================================
# Row parsing
# ============================================================

def parse_row_pre2023(row: dict, source_file: str, row_num: int) -> TransactionRaw:
    """Parse a pre-2023 CSV row (17 columns)."""
    return TransactionRaw(
        source_file=source_file,
        source_row=row_num,
        schema_version=SchemaVersion.PRE_2023,
        fecha=row.get("Fecha de venta", ""),
        estacion=row.get("Estación de servicio", "").strip(),
        plaza=row.get("Plaza", "AGUASCALIENTES").strip().upper(),
        gasup_placa_id=safe_int(row.get("Id_placa", "")),
        placa=row.get("Placa", "").strip().upper(),
        modelo=row.get("Desc_Modelo", "").strip() or None,
        marca=row.get("Desc_Marca", "").strip() or None,
        linea=row.get("Desc_Linea", "").strip() or None,
        fecha_conversion=row.get("Fh_conversion", ""),
        litros=safe_decimal(row.get("Litros", "0")),
        pvp=safe_decimal(row.get("PVP", "0")),
        total_mxn=safe_decimal(row.get("TOTAL PRECIO GNV", "0")),
        valor_recaudo=safe_decimal(row.get("Valor recaudo", "0")),
        recaudo_pagado=safe_decimal(row.get("Reacaudo pagado", "0")),
        venta_mas_recaudo=safe_decimal(row.get("Venta total mas recaudo", "0")),
        medio_pago=row.get("Desc_medio_pago", "").strip() or None,
        segmento=row.get("Segmento Localizado", "").strip() or None,
    )


def parse_row_post2023(row: dict, source_file: str, row_num: int) -> TransactionRaw:
    """Parse a post-2023 CSV row (13 columns)."""
    return TransactionRaw(
        source_file=source_file,
        source_row=row_num,
        schema_version=SchemaVersion.POST_2023,
        fecha=row.get("Fecha de ventas", ""),
        estacion=row.get("Estación de servicio", "").strip(),
        plaza=row.get("Plaza", "AGUASCALIENTES").strip().upper(),
        gasup_placa_id=safe_int(row.get("Id_placa", "")),
        placa=row.get("Placa", "").strip().upper(),
        modelo=row.get("Desc_Modelo", "").strip() or None,
        marca=row.get("Desc_Marca", "").strip() or None,
        linea=row.get("Desc_Linea", "").strip() or None,
        fecha_conversion=row.get("Fh_conversion", ""),
        litros=safe_decimal(row.get("Litros", "0")),
        pvp=safe_decimal(row.get("PVP", "0")),
        total_mxn=safe_decimal(row.get("Suma de precio_total", "0")),
        recaudo=safe_decimal(row.get("Recaudo", "0")),
        # Fields not available in post-2023
        medio_pago=None,
        segmento=None,
        valor_recaudo=Decimal("0"),
        recaudo_pagado=Decimal("0"),
        venta_mas_recaudo=None,
    )


# ============================================================
# Normalization
# ============================================================

GNC_DENSITY = Decimal("0.717")  # kg per liter at standard conditions
IVA_FACTOR = Decimal("1.16")


def normalize(raw: TransactionRaw) -> TransactionNormalized:
    """Convert raw transaction to normalized form with derived fields."""

    # Parse timestamp
    local_dt = parse_date(raw.fecha)
    if local_dt is None:
        # Fallback: use epoch and flag
        local_dt = datetime(2000, 1, 1)

    utc_dt = to_utc(local_dt)
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=CST)

    # Map station
    station_id = STATION_MAP.get(raw.estacion)

    # Parse fecha_conversion
    fconv = parse_date(raw.fecha_conversion) if raw.fecha_conversion else None

    # Derived fields
    litros = raw.litros
    kg = litros * GNC_DENSITY
    nm3 = litros  # In GasData, 1 litro equivalente = 1 Nm3
    ingreso_neto = (raw.total_mxn / IVA_FACTOR).quantize(Decimal("0.01"))
    iva = raw.total_mxn - ingreso_neto

    # Medio de pago
    medio = normalize_medio_pago(raw.medio_pago)

    # Handle post-2023 recaudo field
    recaudo_pagado = raw.recaudo_pagado
    if raw.schema_version == SchemaVersion.POST_2023 and raw.recaudo:
        recaudo_pagado = raw.recaudo

    txn = TransactionNormalized(
        source_file=raw.source_file,
        source_row=raw.source_row,
        schema_version=raw.schema_version,
        station_id=station_id,
        station_natgas=raw.estacion,
        plaza=raw.plaza or "AGUASCALIENTES",
        timestamp_utc=utc_dt,
        timestamp_local=local_dt,
        gasup_placa_id=raw.gasup_placa_id,
        placa=raw.placa,
        modelo=raw.modelo,
        marca=raw.marca,
        linea=raw.linea,
        fecha_conversion=fconv,
        litros=litros,
        pvp=raw.pvp,
        total_mxn=raw.total_mxn,
        recaudo_valor=raw.valor_recaudo or Decimal("0"),
        recaudo_pagado=recaudo_pagado or Decimal("0"),
        venta_mas_recaudo=raw.venta_mas_recaudo,
        medio_pago=medio,
        segmento=raw.segmento,
        kg=kg.quantize(Decimal("0.0001")),
        nm3=nm3.quantize(Decimal("0.0001")),
        ingreso_neto=ingreso_neto,
        iva=iva.quantize(Decimal("0.01")),
    )

    # Detect anomalies
    txn.anomalies = detect_anomalies(txn)

    return txn


# ============================================================
# Main parser: file → stream of normalized transactions
# ============================================================

class ParseResult:
    """Result of parsing a single CSV file."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.file_name = filepath.name
        self.file_hash = file_hash(filepath)
        self.schema_version: Optional[SchemaVersion] = None
        self.total_rows = 0
        self.parsed_rows = 0
        self.error_rows = 0
        self.anomaly_count = 0
        self.transactions: list[TransactionNormalized] = []
        self.errors: list[dict] = []

    def summary(self) -> str:
        return (
            f"{self.file_name}: {self.parsed_rows}/{self.total_rows} rows parsed "
            f"({self.error_rows} errors, {self.anomaly_count} anomalies) "
            f"[{self.schema_version.value if self.schema_version else '?'}]"
        )


def parse_csv(filepath: Path) -> ParseResult:
    """
    Parse a GasUp CSV file.
    Auto-detects schema (pre-2023 vs post-2023).
    Returns ParseResult with normalized transactions.
    """
    result = ParseResult(filepath)

    # Detect encoding: all files are UTF-8 with BOM
    with open(filepath, "r", encoding="utf-8-sig") as f:
        header_line = f.readline()
        result.schema_version = detect_schema(header_line)

    # Parse
    parse_fn = (
        parse_row_pre2023 if result.schema_version == SchemaVersion.PRE_2023
        else parse_row_post2023
    )

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # row 1 = header
            result.total_rows += 1
            try:
                raw = parse_fn(row, result.file_name, row_num)
                txn = normalize(raw)
                txn.source_hash = result.file_hash
                result.transactions.append(txn)
                result.parsed_rows += 1
                if txn.anomalies:
                    result.anomaly_count += len(txn.anomalies)
            except Exception as e:
                result.error_rows += 1
                result.errors.append({
                    "row": row_num,
                    "error": str(e),
                    "data": dict(row),
                })
                if result.error_rows <= 5:
                    logger.error(f"Row {row_num} in {filepath.name}: {e}")

    logger.info(result.summary())
    return result


def parse_directory(dirpath: Path) -> list[ParseResult]:
    """Parse all CSV files in a directory."""
    results = []
    csv_files = sorted(dirpath.glob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in {dirpath}")

    for filepath in csv_files:
        result = parse_csv(filepath)
        results.append(result)

    # Summary
    total_txn = sum(r.parsed_rows for r in results)
    total_err = sum(r.error_rows for r in results)
    total_anom = sum(r.anomaly_count for r in results)
    logger.info(
        f"TOTAL: {total_txn:,} transactions parsed, "
        f"{total_err:,} errors, {total_anom:,} anomalies "
        f"from {len(results)} files"
    )

    return results
