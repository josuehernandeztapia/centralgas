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

                # By segmento (HU-DASH-3.6) — join with clients for vehicle type
                cur.execute("""
                    SELECT
                        COALESCE(c.segmento, 'SIN_DATOS') AS segmento,
                        COUNT(*) AS n,
                        COALESCE(SUM(t.litros), 0) AS litros,
                        COALESCE(SUM(t.total_mxn), 0) AS mxn
                    FROM transactions t
                    LEFT JOIN clients c ON c.placa = t.placa
                    GROUP BY COALESCE(c.segmento, 'SIN_DATOS')
                    ORDER BY n DESC;
                """)
                by_segmento = []
                if total_count > 0:
                    for seg, n, litros, mxn_val in cur.fetchall():
                        by_segmento.append({
                            "segmento": seg,
                            "count": n,
                            "pct": round(100.0 * n / total_count, 1),
                            "litros": float(litros),
                            "mxn": float(mxn_val),
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
        "by_segmento": by_segmento,
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


# ============================================================
# /api/recaudos — CMU collections derived from transactions
# ============================================================

def list_recaudos(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    placa: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    """
    Return transactions with recaudo > 0, enriched with client name.

    The recaudo fields in GasUp are:
      - recaudo_pagado = tarifa per LEQ (sobreprecio)
      - recaudo_valor  = litros × tarifa (total surcharge)

    This query is the functional equivalent of the weekly
    NatGas→CMU recaudos Excel, but available daily via API.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            where = ["t.recaudo_pagado > 0", "t.recaudo_pagado IS NOT NULL"]
            params: list[Any] = []

            if placa:
                where.append("t.placa = %s")
                params.append(placa.upper().strip())
            if date_from:
                where.append("DATE(t.timestamp_local) >= %s")
                params.append(date_from)
            if date_to:
                where.append("DATE(t.timestamp_local) <= %s")
                params.append(date_to)

            where_sql = " AND ".join(where)

            # Total count
            cur.execute(f"SELECT COUNT(*) FROM transactions t WHERE {where_sql}", params)
            total = cur.fetchone()[0]

            # KPIs
            cur.execute(f"""
                SELECT
                    COUNT(DISTINCT t.placa) AS placas_activas,
                    SUM(t.litros) AS total_litros,
                    SUM(t.recaudo_valor) AS total_recaudado,
                    AVG(t.recaudo_pagado) AS tarifa_promedio,
                    MIN(DATE(t.timestamp_local)) AS fecha_min,
                    MAX(DATE(t.timestamp_local)) AS fecha_max
                FROM transactions t
                WHERE {where_sql}
            """, params)
            kpi_row = cur.fetchone()
            kpis = {
                "placas_activas": kpi_row[0] or 0,
                "total_litros": float(kpi_row[1] or 0),
                "total_recaudado": float(kpi_row[2] or 0),
                "tarifa_promedio": float(kpi_row[3] or 0),
                "fecha_min": str(kpi_row[4]) if kpi_row[4] else None,
                "fecha_max": str(kpi_row[5]) if kpi_row[5] else None,
            }

            # By placa summary
            cur.execute(f"""
                SELECT
                    t.placa,
                    c.nombre,
                    COUNT(*) AS cargas,
                    SUM(t.litros) AS litros,
                    AVG(t.recaudo_pagado) AS tarifa_leq,
                    SUM(t.recaudo_valor) AS total_recaudado
                FROM transactions t
                LEFT JOIN clients c ON c.placa = t.placa
                WHERE {where_sql}
                GROUP BY t.placa, c.nombre
                ORDER BY total_recaudado DESC
                LIMIT 50
            """, params)
            cols_placa = [d[0] for d in cur.description]
            by_placa = [
                {c: _serialize(v) for c, v in zip(cols_placa, row)}
                for row in cur.fetchall()
            ]

            # By day summary
            cur.execute(f"""
                SELECT
                    DATE(t.timestamp_local) AS dia,
                    COUNT(*) AS cargas,
                    COUNT(DISTINCT t.placa) AS placas,
                    SUM(t.litros) AS litros,
                    SUM(t.recaudo_valor) AS recaudado
                FROM transactions t
                WHERE {where_sql}
                GROUP BY DATE(t.timestamp_local)
                ORDER BY dia DESC
                LIMIT 60
            """, params)
            cols_day = [d[0] for d in cur.description]
            by_day = [
                {c: _serialize(v) for c, v in zip(cols_day, row)}
                for row in cur.fetchall()
            ]

            # Detail rows (paginated)
            cur.execute(f"""
                SELECT
                    t.placa,
                    c.nombre AS conductor,
                    t.litros,
                    t.recaudo_pagado AS tarifa_leq,
                    t.recaudo_valor AS cantidad_recaudo,
                    t.timestamp_local AS fecha_hora_venta,
                    DATE(t.timestamp_local) AS fecha_venta,
                    t.station_natgas AS estacion,
                    t.pvp,
                    t.total_mxn,
                    t.placa || '-' || t.recaudo_pagado AS id_placa_recaudo
                FROM transactions t
                LEFT JOIN clients c ON c.placa = t.placa
                WHERE {where_sql}
                ORDER BY t.timestamp_local DESC
                LIMIT %s OFFSET %s
            """, params + [min(limit, 5000), offset])
            cols = [d[0] for d in cur.description]
            rows = [
                {c: _serialize(v) for c, v in zip(cols, row)}
                for row in cur.fetchall()
            ]

    finally:
        conn.close()

    return {
        "total": total,
        "kpis": kpis,
        "by_placa": by_placa,
        "by_day": by_day,
        "rows": rows,
    }


# ============================================================
# /api/stations/:name — station drill-down (HU-DASH-2.1)
# ============================================================

def station_detail(station_name: str) -> dict:
    """
    Drill-down for a single station: volume by month, top placas,
    hourly heatmap, and recent transactions.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # KPIs for this station
            cur.execute("""
                SELECT
                    COUNT(*) AS total_txn,
                    COALESCE(SUM(litros), 0) AS total_litros,
                    COALESCE(SUM(total_mxn), 0) AS total_mxn,
                    COALESCE(SUM(recaudo_valor), 0) AS total_recaudo,
                    COUNT(DISTINCT placa) AS placas_unicas,
                    MIN(timestamp_local) AS primera_txn,
                    MAX(timestamp_local) AS ultima_txn
                FROM transactions
                WHERE station_natgas = %s
            """, (station_name,))
            row = cur.fetchone()
            kpis = {
                "total_txn": row[0],
                "total_litros": float(row[1]),
                "total_mxn": float(row[2]),
                "total_recaudo": float(row[3]),
                "placas_unicas": row[4],
                "primera_txn": _serialize(row[5]),
                "ultima_txn": _serialize(row[6]),
            }

            # Volume by month (last 12 months with data)
            cur.execute("""
                SELECT
                    TO_CHAR(timestamp_local, 'YYYY-MM') AS mes,
                    COUNT(*) AS cargas,
                    COALESCE(SUM(litros), 0) AS litros,
                    COALESCE(SUM(total_mxn), 0) AS mxn
                FROM transactions
                WHERE station_natgas = %s AND timestamp_local IS NOT NULL
                GROUP BY TO_CHAR(timestamp_local, 'YYYY-MM')
                ORDER BY mes DESC
                LIMIT 12
            """, (station_name,))
            by_month = [
                {"mes": r[0], "cargas": r[1], "litros": float(r[2]), "mxn": float(r[3])}
                for r in cur.fetchall()
            ]

            # Top 20 placas by volume
            cur.execute("""
                SELECT
                    t.placa,
                    c.nombre,
                    COUNT(*) AS cargas,
                    COALESCE(SUM(t.litros), 0) AS litros,
                    COALESCE(SUM(t.total_mxn), 0) AS mxn,
                    MAX(t.timestamp_local) AS ultima_carga
                FROM transactions t
                LEFT JOIN clients c ON c.placa = t.placa
                WHERE t.station_natgas = %s
                GROUP BY t.placa, c.nombre
                ORDER BY litros DESC
                LIMIT 20
            """, (station_name,))
            top_placas = [
                {
                    "placa": r[0], "nombre": r[1], "cargas": r[2],
                    "litros": float(r[3]), "mxn": float(r[4]),
                    "ultima_carga": _serialize(r[5]),
                }
                for r in cur.fetchall()
            ]

            # Hourly heatmap (day_of_week × hour → count)
            cur.execute("""
                SELECT
                    EXTRACT(DOW FROM timestamp_local)::int AS dow,
                    EXTRACT(HOUR FROM timestamp_local)::int AS hora,
                    COUNT(*) AS n
                FROM transactions
                WHERE station_natgas = %s AND timestamp_local IS NOT NULL
                GROUP BY dow, hora
                ORDER BY dow, hora
            """, (station_name,))
            heatmap = [
                {"dow": r[0], "hora": r[1], "n": r[2]}
                for r in cur.fetchall()
            ]

            # Medio de pago breakdown
            cur.execute("""
                SELECT medio_pago, COUNT(*) AS n
                FROM transactions
                WHERE station_natgas = %s
                GROUP BY medio_pago
                ORDER BY n DESC
            """, (station_name,))
            total_txn = max(1, kpis["total_txn"])
            by_medio = [
                {"medio_pago": r[0] or "(unknown)", "count": r[1], "pct": round(100.0 * r[1] / total_txn, 1)}
                for r in cur.fetchall()
            ]

    finally:
        conn.close()

    return {
        "station_name": station_name,
        "kpis": kpis,
        "by_month": list(reversed(by_month)),
        "top_placas": top_placas,
        "heatmap": heatmap,
        "by_medio": by_medio,
    }


# ============================================================
# /api/placas/:placa — placa drill-down (HU-DASH-2.2)
# ============================================================

def placa_detail(placa: str) -> dict:
    """
    Drill-down for a single placa: lifetime stats, monthly trend,
    stations visited, recent transactions, and retention info.
    """
    placa = placa.upper().strip()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Client info
            cur.execute("""
                SELECT nombre, telefono, segmento, modelo_vehiculo,
                       fecha_conversion, consumo_prom_lt, eds_principal, notas
                FROM clients WHERE placa = %s
            """, (placa,))
            client_row = cur.fetchone()
            client = None
            if client_row:
                client = {
                    "nombre": client_row[0], "telefono": client_row[1],
                    "segmento": client_row[2], "modelo": client_row[3],
                    "fecha_conversion": _serialize(client_row[4]),
                    "consumo_prom_lt": float(client_row[5]) if client_row[5] else None,
                    "eds_principal": client_row[6], "notas": client_row[7],
                }

            # Lifetime KPIs
            cur.execute("""
                SELECT
                    COUNT(*) AS total_cargas,
                    COALESCE(SUM(litros), 0) AS total_litros,
                    COALESCE(SUM(total_mxn), 0) AS total_mxn,
                    COALESCE(SUM(recaudo_valor), 0) AS total_recaudo,
                    COALESCE(AVG(litros), 0) AS prom_litros,
                    MIN(timestamp_local) AS primera_carga,
                    MAX(timestamp_local) AS ultima_carga
                FROM transactions WHERE placa = %s
            """, (placa,))
            row = cur.fetchone()
            kpis = {
                "total_cargas": row[0],
                "total_litros": float(row[1]),
                "total_mxn": float(row[2]),
                "total_recaudo": float(row[3]),
                "prom_litros": float(row[4]),
                "primera_carga": _serialize(row[5]),
                "ultima_carga": _serialize(row[6]),
            }
            # Days since last refueling
            if row[6]:
                from datetime import datetime as dt, timezone as tz
                now = dt.now(tz.utc)
                last = row[6].replace(tzinfo=tz.utc) if row[6].tzinfo is None else row[6]
                kpis["dias_sin_cargar"] = (now - last).days
            else:
                kpis["dias_sin_cargar"] = None

            # Monthly trend (last 12)
            cur.execute("""
                SELECT
                    TO_CHAR(timestamp_local, 'YYYY-MM') AS mes,
                    COUNT(*) AS cargas,
                    COALESCE(SUM(litros), 0) AS litros,
                    COALESCE(SUM(total_mxn), 0) AS mxn
                FROM transactions
                WHERE placa = %s AND timestamp_local IS NOT NULL
                GROUP BY TO_CHAR(timestamp_local, 'YYYY-MM')
                ORDER BY mes DESC LIMIT 12
            """, (placa,))
            by_month = [
                {"mes": r[0], "cargas": r[1], "litros": float(r[2]), "mxn": float(r[3])}
                for r in cur.fetchall()
            ]

            # Stations visited
            cur.execute("""
                SELECT
                    station_natgas,
                    COUNT(*) AS cargas,
                    COALESCE(SUM(litros), 0) AS litros
                FROM transactions WHERE placa = %s
                GROUP BY station_natgas
                ORDER BY cargas DESC
            """, (placa,))
            stations = [
                {"station": r[0] or "—", "cargas": r[1], "litros": float(r[2])}
                for r in cur.fetchall()
            ]

            # Recent transactions (last 50)
            cur.execute("""
                SELECT
                    timestamp_local, station_natgas, litros,
                    pvp, total_mxn, recaudo_pagado, recaudo_valor, medio_pago
                FROM transactions
                WHERE placa = %s
                ORDER BY timestamp_local DESC NULLS LAST
                LIMIT 50
            """, (placa,))
            cols = ["fecha", "estacion", "litros", "pvp", "total_mxn",
                    "tarifa_recaudo", "recaudo_total", "medio_pago"]
            recent = [
                {c: _serialize(v) for c, v in zip(cols, r)}
                for r in cur.fetchall()
            ]

    finally:
        conn.close()

    return {
        "placa": placa,
        "client": client,
        "kpis": kpis,
        "by_month": list(reversed(by_month)),
        "stations": stations,
        "recent": recent,
    }


# ============================================================
# /api/search — global search (HU-DASH-2.5)
# ============================================================

def global_search(q: str, limit: int = 20) -> dict:
    """
    Search placas and stations matching query string.
    Returns grouped results by type.
    """
    q = q.strip()
    if not q or len(q) < 2:
        return {"results": [], "query": q}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            results = []
            pattern = f"%{q.upper()}%"

            # Search placas
            cur.execute("""
                SELECT DISTINCT t.placa, c.nombre, COUNT(*) AS cargas
                FROM transactions t
                LEFT JOIN clients c ON c.placa = t.placa
                WHERE t.placa LIKE %s OR UPPER(c.nombre) LIKE %s
                GROUP BY t.placa, c.nombre
                ORDER BY cargas DESC
                LIMIT %s
            """, (pattern, pattern, limit))
            for r in cur.fetchall():
                results.append({
                    "type": "placa",
                    "id": r[0],
                    "label": r[0],
                    "detail": r[1] or "",
                    "meta": f"{r[2]} cargas",
                })

            # Search stations
            cur.execute("""
                SELECT station_natgas, COUNT(*) AS cargas,
                       COALESCE(SUM(litros), 0) AS litros
                FROM transactions
                WHERE UPPER(station_natgas) LIKE %s
                GROUP BY station_natgas
                ORDER BY cargas DESC
                LIMIT %s
            """, (pattern, limit))
            for r in cur.fetchall():
                results.append({
                    "type": "estacion",
                    "id": r[0],
                    "label": r[0],
                    "detail": f"{r[1]} txn, {float(r[2]):,.0f} LEQ",
                    "meta": "",
                })

    finally:
        conn.close()

    return {"results": results, "query": q}


# ============================================================
# /api/retention — placas inactivas (HU-DASH-3.3)
# ============================================================

def retention_alerts(days_inactive: int = 7, min_cargas: int = 3) -> dict:
    """
    Placas activas (at least min_cargas in last 90 days) that haven't
    refueled in the last days_inactive days. Prioritized by historical
    monthly volume.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH active AS (
                    SELECT
                        t.placa,
                        c.nombre,
                        c.telefono,
                        c.segmento,
                        COUNT(*) AS cargas_90d,
                        COALESCE(SUM(t.litros), 0) AS litros_90d,
                        COALESCE(AVG(t.litros), 0) AS prom_litros,
                        MAX(t.timestamp_local) AS ultima_carga,
                        MODE() WITHIN GROUP (ORDER BY t.station_natgas) AS estacion_frecuente
                    FROM transactions t
                    LEFT JOIN clients c ON c.placa = t.placa
                    WHERE t.timestamp_local >= NOW() - INTERVAL '90 days'
                    GROUP BY t.placa, c.nombre, c.telefono, c.segmento
                    HAVING COUNT(*) >= %(min_cargas)s
                )
                SELECT
                    placa, nombre, telefono, segmento,
                    cargas_90d, litros_90d, prom_litros,
                    ultima_carga, estacion_frecuente,
                    EXTRACT(DAY FROM NOW() - ultima_carga)::int AS dias_sin_cargar
                FROM active
                WHERE EXTRACT(DAY FROM NOW() - ultima_carga) >= %(days_inactive)s
                ORDER BY litros_90d DESC
                LIMIT 200
            """, {"min_cargas": min_cargas, "days_inactive": days_inactive})

            cols = [d[0] for d in cur.description]
            rows = []
            for r in cur.fetchall():
                row = {c: _serialize(v) for c, v in zip(cols, r)}
                rows.append(row)

            # Summary KPIs
            total_at_risk = len(rows)
            litros_at_risk = sum(r.get("litros_90d", 0) for r in rows)

    finally:
        conn.close()

    return {
        "days_inactive": days_inactive,
        "min_cargas": min_cargas,
        "total_at_risk": total_at_risk,
        "litros_at_risk": litros_at_risk,
        "rows": rows,
    }
