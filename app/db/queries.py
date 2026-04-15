"""
Read queries against the transactions table.

Used by the /transactions, /stats, and /sobreprecio/distribution endpoints.

All functions:
  - Read DATABASE_URL from env (set by Fly secrets)
  - Use psycopg2 (no ORM overhead — Phase 1 priority is correctness + speed of iteration)
  - Return plain dicts/lists ready to JSON-serialize
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger("db.queries")


def _get_conn():
    """Open a psycopg2 connection. Caller is responsible for closing."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL env var not set")
    import psycopg2
    return psycopg2.connect(db_url)


def _serialize(v: Any) -> Any:
    """JSON-friendly conversion for Decimal/datetime values."""
    if isinstance(v, Decimal):
        # Avoid scientific notation for big numbers; preserve precision
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


# ============================================================
# /transactions — paginated list with filters
# ============================================================

def list_transactions(
    limit: int = 100,
    offset: int = 0,
    estacion: Optional[str] = None,
    placa: Optional[str] = None,
    date_from: Optional[str] = None,    # ISO string YYYY-MM-DD
    date_to: Optional[str] = None,
) -> dict:
    """
    Return a paginated slice of transactions ordered by timestamp_local DESC.

    Filters (all optional):
      - estacion: substring match on station_natgas (ILIKE %estacion%)
      - placa: exact match
      - date_from / date_to: timestamp_local >= date_from AND < date_to+1d

    Returns:
        {
          "total": int,
          "limit": int,
          "offset": int,
          "filters": {...},
          "rows": [{...}, ...]
        }
    """
    limit = max(1, min(limit, 1000))   # safety cap
    offset = max(0, offset)

    where = []
    params: list[Any] = []

    if estacion:
        where.append("station_natgas ILIKE %s")
        params.append(f"%{estacion}%")
    if placa:
        where.append("placa = %s")
        params.append(placa.upper().strip())
    if date_from:
        where.append("timestamp_local >= %s")
        params.append(date_from)
    if date_to:
        where.append("timestamp_local < (%s::date + INTERVAL '1 day')")
        params.append(date_to)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    cols = (
        "id", "source_file", "station_natgas", "timestamp_local", "placa",
        "litros", "pvp", "total_mxn", "recaudo_valor", "recaudo_pagado",
        "medio_pago", "kg", "nm3", "ingreso_neto", "iva",
    )
    cols_csv = ", ".join(cols)

    sql_count = f"SELECT COUNT(*) FROM transactions {where_sql}"
    sql_rows = (
        f"SELECT {cols_csv} FROM transactions {where_sql} "
        f"ORDER BY timestamp_local DESC NULLS LAST "
        f"LIMIT %s OFFSET %s"
    )

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_count, params)
                total = cur.fetchone()[0]

                cur.execute(sql_rows, params + [limit, offset])
                rows = []
                for r in cur.fetchall():
                    rows.append({c: _serialize(v) for c, v in zip(cols, r)})
    finally:
        conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "filters": {
            "estacion": estacion, "placa": placa,
            "date_from": date_from, "date_to": date_to,
        },
        "rows": rows,
    }


# ============================================================
# /stats — aggregate KPIs across the table
# ============================================================

def aggregate_stats() -> dict:
    """
    Compute high-level KPIs: counts, sums, breakdowns by estacion / medio_pago / date.

    Returns:
        {
          "total_transactions": int,
          "total_litros": float,
          "total_kg": float,
          "total_nm3": float,
          "total_mxn": float,
          "total_neto": float,
          "total_iva": float,
          "total_sobreprecio": float,
          "date_range": {"min": iso, "max": iso, "days": int},
          "by_estacion": [{"station_natgas": str, "count": int, "litros": float, "mxn": float}, ...],
          "by_medio_pago": [{"medio_pago": str, "count": int, "pct": float}, ...],
          "by_day": [{"day": iso, "count": int, "litros": float, "mxn": float}, ...]   # last 30 days
        }
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Top-level aggregates + date range
                cur.execute("""
                    SELECT
                      COUNT(*),
                      COALESCE(SUM(litros), 0),
                      COALESCE(SUM(kg), 0),
                      COALESCE(SUM(nm3), 0),
                      COALESCE(SUM(total_mxn), 0),
                      COALESCE(SUM(ingreso_neto), 0),
                      COALESCE(SUM(iva), 0),
                      COALESCE(SUM(recaudo_valor), 0),
                      MIN(timestamp_local),
                      MAX(timestamp_local)
                    FROM transactions;
                """)
                (
                    total_count, sum_litros, sum_kg, sum_nm3,
                    sum_mxn, sum_neto, sum_iva, sum_sobreprecio,
                    min_ts, max_ts,
                ) = cur.fetchone()

                date_range = {
                    "min": _serialize(min_ts),
                    "max": _serialize(max_ts),
                    "days": ((max_ts - min_ts).days + 1) if (min_ts and max_ts) else 0,
                }

                # By estacion
                cur.execute("""
                    SELECT station_natgas, COUNT(*), COALESCE(SUM(litros), 0), COALESCE(SUM(total_mxn), 0)
                    FROM transactions
                    GROUP BY station_natgas
                    ORDER BY COUNT(*) DESC
                    LIMIT 50;
                """)
                by_estacion = [
                    {"station_natgas": r[0] or "", "count": r[1],
                     "litros": float(r[2]), "mxn": float(r[3])}
                    for r in cur.fetchall()
                ]

                # By medio_pago
                cur.execute("""
                    SELECT medio_pago, COUNT(*)
                    FROM transactions
                    GROUP BY medio_pago
                    ORDER BY COUNT(*) DESC;
                """)
                rows = cur.fetchall()
                by_medio_pago = []
                if total_count > 0:
                    for medio, n in rows:
                        by_medio_pago.append({
                            "medio_pago": medio or "(unknown)",
                            "count": n,
                            "pct": round(100.0 * n / total_count, 2),
                        })

                # By day — last 30 days that HAVE data (not calendar-based).
                # Avoids showing empty charts when MAX(timestamp) is recent but
                # historical data is spread over years with gaps (dataset of NatGas
                # covers Dec 2023 – Apr 2026 with concentrated dates, not daily).
                cur.execute("""
                    SELECT day, cnt, litros_sum, mxn_sum
                    FROM (
                        SELECT
                            DATE(timestamp_local) AS day,
                            COUNT(*) AS cnt,
                            COALESCE(SUM(litros), 0) AS litros_sum,
                            COALESCE(SUM(total_mxn), 0) AS mxn_sum
                        FROM transactions
                        WHERE timestamp_local IS NOT NULL
                        GROUP BY DATE(timestamp_local)
                        ORDER BY day DESC
                        LIMIT 30
                    ) t
                    ORDER BY day DESC;
                """)
                by_day = [
                    {"day": str(r[0]), "count": r[1], "litros": float(r[2]), "mxn": float(r[3])}
                    for r in cur.fetchall()
                ]
    finally:
        conn.close()

    return {
        "total_transactions": total_count,
        "total_litros": float(sum_litros),
        "total_kg": float(sum_kg),
        "total_nm3": float(sum_nm3),
        "total_mxn": float(sum_mxn),
        "total_neto": float(sum_neto),
        "total_iva": float(sum_iva),
        "total_sobreprecio": float(sum_sobreprecio),
        "date_range": date_range,
        "by_estacion": by_estacion,
        "by_medio_pago": by_medio_pago,
        "by_day": by_day,
    }


# ============================================================
# /sobreprecio/distribution — histogram + percentiles
# ============================================================

def sobreprecio_distribution(buckets: int = 12) -> dict:
    """
    Return histogram + descriptive stats for the per-LEQ surcharge field
    (recaudo_pagado, where the bulk_insert puts `tx.sobreprecio`).

    Args:
        buckets: number of histogram bins. Default 12 (covers $0–$24 in $2 steps).

    Returns:
        {
          "stats": {
            "n_nonzero": int, "n_total": int,
            "min": float, "max": float, "mean": float, "median": float,
            "p25": float, "p75": float, "p90": float, "p95": float, "p99": float, "stddev": float,
          },
          "buckets": [
            {"lower": float, "upper": float, "count": int, "pct": float}, ...
          ]
        }
    """
    buckets = max(1, min(buckets, 100))

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Stats over rows where sobreprecio > 0 (zero rows distort distribution)
                cur.execute("""
                    SELECT
                      COUNT(*) FILTER (WHERE recaudo_pagado > 0)            AS n_nonzero,
                      COUNT(*)                                              AS n_total,
                      MIN(recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS minv,
                      MAX(recaudo_pagado)                                   AS maxv,
                      AVG(recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS mean,
                      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p50,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p25,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p75,
                      PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p90,
                      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p95,
                      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS p99,
                      STDDEV(recaudo_pagado) FILTER (WHERE recaudo_pagado > 0) AS stddev
                    FROM transactions;
                """)
                row = cur.fetchone()
                (
                    n_nonzero, n_total, minv, maxv, mean,
                    p50, p25, p75, p90, p95, p99, stddev
                ) = row

                stats = {
                    "n_nonzero": n_nonzero or 0,
                    "n_total": n_total or 0,
                    "min": float(minv) if minv is not None else 0.0,
                    "max": float(maxv) if maxv is not None else 0.0,
                    "mean": round(float(mean), 4) if mean is not None else 0.0,
                    "median": round(float(p50), 4) if p50 is not None else 0.0,
                    "p25": round(float(p25), 4) if p25 is not None else 0.0,
                    "p75": round(float(p75), 4) if p75 is not None else 0.0,
                    "p90": round(float(p90), 4) if p90 is not None else 0.0,
                    "p95": round(float(p95), 4) if p95 is not None else 0.0,
                    "p99": round(float(p99), 4) if p99 is not None else 0.0,
                    "stddev": round(float(stddev), 4) if stddev is not None else 0.0,
                }

                bucket_list: list[dict] = []
                if n_nonzero and p99 is not None and float(p99) > 0:
                    # Cap histogram at p99 to avoid visual distortion from outliers.
                    # The "real" sobreprecio range is $0-$14/LEQ (per Central Gas docs).
                    # Outliers beyond p99 go into a separate overflow bucket.
                    cap = float(p99)
                    width = cap / buckets if cap > 0 else 1.0

                    cur.execute(
                        """
                        SELECT
                          width_bucket(recaudo_pagado::numeric, 0, %s, %s) AS bucket,
                          COUNT(*)
                        FROM transactions
                        WHERE recaudo_pagado > 0
                        GROUP BY bucket
                        ORDER BY bucket;
                        """,
                        (cap, buckets),
                    )
                    counts = {int(b): int(n) for b, n in cur.fetchall()}
                    total = max(1, n_nonzero)
                    for i in range(1, buckets + 1):
                        lo = (i - 1) * width
                        hi = i * width
                        n = counts.get(i, 0)
                        bucket_list.append({
                            "lower": round(lo, 4),
                            "upper": round(hi, 4),
                            "count": n,
                            "pct": round(100.0 * n / total, 2),
                        })
                    # Overflow bucket (everything > p99)
                    overflow = counts.get(buckets + 1, 0)
                    if overflow:
                        bucket_list.append({
                            "lower": round(cap, 4),
                            "upper": None,
                            "count": overflow,
                            "pct": round(100.0 * overflow / total, 2),
                        })
    finally:
        conn.close()

    return {"stats": stats, "buckets": bucket_list}
