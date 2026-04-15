"""
Migration: widen `placa` and `modelo` columns from VARCHAR(10) to VARCHAR(20).

Real NatGas data has placas longer than 10 chars (fleet/foreign plates).
Same for modelo (we may eventually store more than just the year).

Materialized view `daily_close` depends on transactions.placa so we must
DROP it before the ALTER and RECREATE it after. The view definition is
copied verbatim from init_db_fly.sql.

Safe to re-run.

Usage (from inside Fly machine):
    python3 scripts/migrate_widen_placa.py
"""

from __future__ import annotations

import os
import sys


# 1) Drop the materialized view that blocks the ALTER
DROP_VIEW = "DROP MATERIALIZED VIEW IF EXISTS daily_close;"

# 2) Widen the columns
ALTER_SQL = """
ALTER TABLE transactions ALTER COLUMN placa  TYPE VARCHAR(20);
ALTER TABLE transactions ALTER COLUMN modelo TYPE VARCHAR(20);
ALTER TABLE clients      ALTER COLUMN placa           TYPE VARCHAR(20);
ALTER TABLE clients      ALTER COLUMN modelo_vehiculo TYPE VARCHAR(20);
"""

# 3) Recreate the materialized view (same definition as in init_db_fly.sql)
CREATE_VIEW = """
CREATE MATERIALIZED VIEW daily_close AS
SELECT
    t.station_id,
    DATE(t.timestamp_local) AS close_date,
    COUNT(*)                AS total_cargas,
    COUNT(DISTINCT t.placa) AS unique_placas,
    SUM(t.litros)           AS total_litros,
    SUM(t.kg)               AS total_kg,
    SUM(t.nm3)              AS total_nm3,
    SUM(t.total_mxn)        AS total_mxn,
    SUM(t.ingreso_neto)     AS total_neto,
    SUM(t.iva)              AS total_iva,
    AVG(t.pvp)              AS avg_pvp,
    AVG(t.litros)           AS avg_litros_per_carga,
    r.status                AS reconc_status,
    COUNT(*) FILTER (WHERE jsonb_array_length(t.anomalies) > 0) AS anomaly_count
FROM transactions t
LEFT JOIN reconciliation_runs r
    ON r.station_id = t.station_id
    AND r.run_date = DATE(t.timestamp_local)
GROUP BY t.station_id, DATE(t.timestamp_local), r.status
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_close_pk ON daily_close(station_id, close_date);
"""


def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var not set", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed", file=sys.stderr)
        return 1

    print("Connecting to:", db_url.rsplit("@", 1)[-1])
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                print("→ Dropping materialized view daily_close (if exists)...")
                cur.execute(DROP_VIEW)
                print("→ Widening placa + modelo columns...")
                cur.execute(ALTER_SQL)
                print("→ Recreating materialized view daily_close...")
                cur.execute(CREATE_VIEW)

                # Verify column widths
                cur.execute("""
                    SELECT table_name, column_name, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name IN ('transactions', 'clients')
                      AND column_name IN ('placa', 'modelo', 'modelo_vehiculo')
                    ORDER BY table_name, column_name;
                """)
                print("\n✅ Migration applied. Column widths now:")
                for tbl, col, w in cur.fetchall():
                    print(f"   {tbl:14s} {col:20s} VARCHAR({w})")
                # Verify view exists
                cur.execute("""
                    SELECT matviewname FROM pg_matviews
                    WHERE schemaname = 'public';
                """)
                views = [r[0] for r in cur.fetchall()]
                print(f"\n   Materialized views: {views}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
