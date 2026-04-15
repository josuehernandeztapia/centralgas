"""
Bulk insert of GasUp transactions into Postgres.

Reads DATABASE_URL from env. Uses psycopg2 + execute_values for performance.

Maps from app.services.gasup_connector.GasUpTransaction (dataclass produced by
the parser) to the `transactions` table in init_db_fly.sql.

Field name mapping (parser → DB column):
    placa             → placa
    litros            → litros
    precio_unitario   → pvp
    total             → total_mxn
    fecha_hora        → timestamp_utc, timestamp_local
    estacion_nombre   → station_natgas
    estacion_id       → (kept in station_natgas as fallback; station_id INTEGER lookup pending)
    medio_pago        → medio_pago
    sobreprecio       → recaudo_pagado (per-LEQ surcharge)
    total_sobreprecio → recaudo_valor (total surcharge for the transaction)
    ticket_id         → not stored directly (used for in-memory dedup in connector)

Derived (computed in _row_to_tuple):
    kg            = litros * 0.717
    nm3           = litros * 1.0
    ingreso_neto  = total / 1.16
    iva           = total - ingreso_neto

Idempotency:
    Uses ON CONFLICT (source_hash, source_row) DO NOTHING.
    source_row is the position of the transaction in the parsed list (0..N-1).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger("db.transactions")

# Aguascalientes timezone offset (UTC-6, no DST)
CST_OFFSET = timedelta(hours=-6)
CST_TZ = timezone(CST_OFFSET)


# Columns we insert (matches transactions table in init_db_fly.sql).
# Order MUST match the values tuple built per row.
INSERT_COLUMNS = (
    "source_file",
    "source_hash",
    "source_row",
    "schema_version",
    "station_id",
    "station_natgas",
    "plaza",
    "timestamp_utc",
    "timestamp_local",
    "gasup_placa_id",
    "placa",
    "modelo",
    "marca",
    "linea",
    "fecha_conversion",
    "litros",
    "pvp",
    "total_mxn",
    "recaudo_valor",
    "recaudo_pagado",
    "venta_mas_recaudo",
    "medio_pago",
    "segmento",
    "kg",
    "nm3",
    "ingreso_neto",
    "iva",
    "anomalies",
)


def _to_decimal(v: Any) -> Decimal:
    """Coerce float/int/None to Decimal for psycopg2 numeric columns."""
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _normalize_dt(dt: Any) -> tuple[datetime | None, datetime | None]:
    """
    Return (timestamp_utc, timestamp_local) from any datetime-like value.

    The parser may emit:
      - datetime with tzinfo (use as-is)
      - naive datetime (assume Aguascalientes local time)
      - None / non-datetime → (None, None)
    """
    if not isinstance(dt, datetime):
        return (None, None)
    if dt.tzinfo is None:
        # Treat as local Aguascalientes time
        local = dt.replace(tzinfo=CST_TZ)
    else:
        local = dt
    utc = local.astimezone(timezone.utc)
    return (utc, local)


def _row_to_tuple(tx: Any, source_file: str, source_hash: str, source_row: int) -> tuple:
    """Map a GasUpTransaction (from app.services.gasup_connector) → tuple matching INSERT_COLUMNS."""
    fecha = getattr(tx, "fecha_hora", None)
    ts_utc, ts_local = _normalize_dt(fecha)

    placa = (getattr(tx, "placa", "") or "").strip().upper()
    estacion_nombre = (getattr(tx, "estacion_nombre", "") or "").strip()
    estacion_id_str = (getattr(tx, "estacion_id", "") or "").strip()
    # Use the station name; fall back to id string. station_id (INT FK) lookup is pending.
    station_natgas = estacion_nombre or estacion_id_str

    litros = _to_decimal(getattr(tx, "litros", 0))
    pvp = _to_decimal(getattr(tx, "precio_unitario", 0))
    total_mxn = _to_decimal(getattr(tx, "total", 0))
    sobreprecio_per_leq = _to_decimal(getattr(tx, "sobreprecio", 0))
    sobreprecio_total = _to_decimal(getattr(tx, "total_sobreprecio", 0))

    # Derived columns (NIF: 16% IVA included in total)
    kg = (litros * Decimal("0.717")).quantize(Decimal("0.0001"))
    nm3 = litros  # 1 LEQ ≈ 1 Nm³ for natgas pricing
    ingreso_neto = (total_mxn / Decimal("1.16")).quantize(Decimal("0.01"))
    iva = (total_mxn - ingreso_neto).quantize(Decimal("0.01"))
    venta_mas_recaudo = total_mxn + sobreprecio_total

    medio_pago = (getattr(tx, "medio_pago", None) or "").upper() or None

    return (
        source_file,
        source_hash,
        source_row,
        "post2023",                  # schema_version
        None,                        # station_id (FK lookup pending)
        station_natgas,
        "AGUASCALIENTES",            # plaza
        ts_utc,
        ts_local,
        None,                        # gasup_placa_id (not in dataclass)
        placa,
        None,                        # modelo (not in dataclass)
        None,                        # marca
        None,                        # linea
        None,                        # fecha_conversion
        litros,
        pvp,
        total_mxn,
        sobreprecio_total,           # recaudo_valor (total surcharge)
        sobreprecio_per_leq,         # recaudo_pagado (per-LEQ surcharge)
        venta_mas_recaudo,
        medio_pago,
        None,                        # segmento
        kg,
        nm3,
        ingreso_neto,
        iva,
        json.dumps([]),              # anomalies (empty for now)
    )


def bulk_insert_transactions(
    transactions: Iterable[Any],
    source_file: str,
    source_hash: str | None = None,
    batch_size: int = 1000,
) -> dict:
    """
    Bulk insert GasUpTransaction objects into the `transactions` table.

    Returns:
        {
            "inserted": int,         # rows actually written (excludes ON CONFLICT skips)
            "skipped": int,          # rows ignored by ON CONFLICT (already in DB)
            "errors": int,
            "errors_detail": [str, ...],
            "skipped_invalid": int,  # rows dropped pre-insert (missing required fields)
        }
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return {
            "inserted": 0, "skipped": 0, "errors": 1,
            "errors_detail": ["DATABASE_URL env var not set"],
            "skipped_invalid": 0,
        }

    if source_hash is None:
        source_hash = hashlib.sha256(source_file.encode()).hexdigest()[:16]

    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as e:
        return {
            "inserted": 0, "skipped": 0, "errors": 1,
            "errors_detail": [f"psycopg2 import failed: {e}"],
            "skipped_invalid": 0,
        }

    raw_txs = list(transactions)
    if not raw_txs:
        return {"inserted": 0, "skipped": 0, "errors": 0, "errors_detail": [], "skipped_invalid": 0}

    # Pre-filter: drop rows missing required NOT NULL fields (timestamp + placa)
    enumerated = []
    skipped_invalid = 0
    for idx, tx in enumerate(raw_txs):
        fecha = getattr(tx, "fecha_hora", None)
        placa = getattr(tx, "placa", None)
        if not isinstance(fecha, datetime) or not placa:
            skipped_invalid += 1
            continue
        enumerated.append((idx, tx))

    if not enumerated:
        return {
            "inserted": 0, "skipped": 0, "errors": 0, "errors_detail": [],
            "skipped_invalid": skipped_invalid,
        }

    cols_csv = ", ".join(INSERT_COLUMNS)
    insert_sql = (
        f"INSERT INTO transactions ({cols_csv}) VALUES %s "
        f"ON CONFLICT (source_hash, source_row) DO NOTHING"
    )

    total_attempted = 0
    total_inserted = 0
    errors: list[str] = []

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for i in range(0, len(enumerated), batch_size):
                    batch = enumerated[i:i + batch_size]
                    rows = []
                    for idx, tx in batch:
                        try:
                            rows.append(_row_to_tuple(tx, source_file, source_hash, idx))
                        except Exception as e:
                            errors.append(f"row {idx} mapping failed: {e}")
                            if len(errors) > 10:
                                break

                    if not rows:
                        continue

                    try:
                        execute_values(cur, insert_sql, rows, template=None, page_size=batch_size)
                        total_inserted += cur.rowcount
                        total_attempted += len(rows)
                    except Exception as e:
                        errors.append(f"batch {i}-{i+len(rows)}: {e}")
                        conn.rollback()
                        if len(errors) > 10:
                            break
    finally:
        conn.close()

    skipped = max(0, total_attempted - total_inserted)
    return {
        "inserted": total_inserted,
        "skipped": skipped,
        "errors": len(errors),
        "errors_detail": errors[:10],
        "skipped_invalid": skipped_invalid,
    }


def query_transaction_count() -> int:
    """Quick sanity helper — total rows in the transactions table."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return -1
    try:
        import psycopg2
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM transactions;")
                return cur.fetchone()[0]
    except Exception as e:
        logger.warning(f"query_transaction_count failed: {e}")
        return -1
