"""
Idempotent migration: add UNIQUE INDEX uq_txn_source_hash_row to transactions table.

Run this when the schema was created BEFORE the unique index was added to init_db_fly.sql.
Safe to re-run (uses IF NOT EXISTS).

Usage (from inside Fly machine):
    python3 scripts/migrate_add_unique_idx.py
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

    sql = """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_txn_source_hash_row
        ON transactions(source_hash, source_row);
    """

    print("Connecting to:", db_url.rsplit("@", 1)[-1])
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                # Verify
                cur.execute("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = 'transactions'
                    ORDER BY indexname;
                """)
                indexes = [r[0] for r in cur.fetchall()]
                print(f"\n✅ Migration applied. Indexes on transactions ({len(indexes)}):")
                for ix in indexes:
                    print(f"   - {ix}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
