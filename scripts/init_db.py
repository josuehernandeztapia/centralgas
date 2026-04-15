"""
Apply database schema from inside the container.

Usage (from inside Fly machine):
    python3 scripts/init_db.py [--reset]

Options:
    --reset   DROP all tables before re-creating (DESTRUCTIVE — Phase 1 only)

Reads DATABASE_URL from environment (set automatically by `fly postgres attach`).
Picks init_db_fly.sql by default (no TimescaleDB), falls back to init_db.sql.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize Central Gas DB schema")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="DROP all tables first (destructive — Phase 1 validation only)",
    )
    parser.add_argument(
        "--sql-file",
        default=None,
        help="SQL file path (default: scripts/init_db_fly.sql, falls back to init_db.sql)",
    )
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var not set", file=sys.stderr)
        return 1

    # Pick SQL file
    here = Path(__file__).parent
    if args.sql_file:
        sql_path = Path(args.sql_file)
    else:
        sql_path = here / "init_db_fly.sql"
        if not sql_path.exists():
            sql_path = here / "init_db.sql"

    if not sql_path.exists():
        print(f"ERROR: SQL file not found: {sql_path}", file=sys.stderr)
        return 1

    print(f"Using SQL file:  {sql_path}")
    print(f"Database:        {db_url.rsplit('@', 1)[-1]}")  # show only host portion

    try:
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    except ImportError:
        print("ERROR: psycopg2 not installed (should be in requirements.txt)", file=sys.stderr)
        return 1

    conn = psycopg2.connect(db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    if args.reset:
        print("\n⚠️  --reset: dropping schema 'public' and recreating...")
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
        cur.execute("GRANT ALL ON SCHEMA public TO PUBLIC;")
        print("   schema 'public' reset.")

    sql = sql_path.read_text()
    print(f"\nApplying schema ({len(sql):,} chars)...")
    try:
        cur.execute(sql)
    except Exception as e:
        print(f"ERROR applying schema: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Verify
    cur.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
    )
    tables = [r[0] for r in cur.fetchall()]
    print(f"\n✅ Schema applied. Tables in public schema ({len(tables)}):")
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        count = cur.fetchone()[0]
        print(f"   - {t:30s} ({count} rows)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
