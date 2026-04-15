"""
Cleanup: remove 12 garbage rows from the bad recaudos_financiera upload test.

Context:
  During Phase 1 validation we accidentally uploaded a recaudos_financiera
  .xls with a malformed curl command (missing '@' prefix). The parser
  ingested 12 rows where `placa` was populated with the financiera name
  (IMPULSATE INCLUSION, GRUPO ANISAL S.A, FINANCIERA VANTEC...) and
  timestamps were set to the upload time instead of the real sale date.

  These rows pollute:
    - /stats total_transactions (31,028 vs real 31,016)
    - Sobreprecio max ($385/LEQ vs real ~$14/LEQ)
    - by_day chart (only dot at the upload day)

Fix:
  DELETE rows where source_file LIKE '%RecaudosFinanciera%'.
  Real ventas_detalladas data remains untouched.

Safe to re-run. Idempotent (if 0 rows match, no-op).

Usage:
    python3 scripts/migrate_cleanup_bad_recaudos.py
"""

from __future__ import annotations

import os
import sys


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
                # 1. Preview what we're about to delete
                cur.execute("""
                    SELECT id, source_file, placa, timestamp_local, total_mxn
                    FROM transactions
                    WHERE source_file ILIKE '%RecaudosFinanciera%'
                       OR source_file ILIKE '%recaudos_financiera%'
                    ORDER BY id
                    LIMIT 20;
                """)
                preview = cur.fetchall()
                print(f"\n→ Found {len(preview)} rows matching recaudos_financiera filename:")
                for row in preview:
                    id_, src, placa, ts, total = row
                    print(f"   id={id_:>6}  '{src}'  placa={placa}  ts={ts}  total={total}")

                if not preview:
                    print("\n✅ No garbage rows to clean. Table is clean.")
                    return 0

                # 2. Delete
                cur.execute("""
                    DELETE FROM transactions
                    WHERE source_file ILIKE '%RecaudosFinanciera%'
                       OR source_file ILIKE '%recaudos_financiera%';
                """)
                deleted = cur.rowcount
                print(f"\n✅ Deleted {deleted} rows.")

                # 3. Verify new totals
                cur.execute("SELECT COUNT(*), MAX(recaudo_pagado) FROM transactions;")
                total, max_sp = cur.fetchone()
                print(f"\n   New total_transactions: {total}")
                print(f"   New max sobreprecio/LEQ: ${float(max_sp or 0):.2f}")

                # 4. Refresh materialized view that depends on transactions
                cur.execute("REFRESH MATERIALIZED VIEW daily_close;")
                print(f"   Refreshed daily_close materialized view.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
