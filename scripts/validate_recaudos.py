#!/usr/bin/env python3
"""
Validate recaudos: cross-reference NatGas→CMU Excel files against
the transactions table to verify that our model (recaudo = derived
view of ventas) is correct.

For each row in the recaudos Excel:
  1. Find the matching transaction by: placa + timestamp + litros
  2. Compare: Excel.cantidad_recaudo vs DB.recaudo_valor (litros × tarifa)
  3. Report matches, mismatches, and orphans

Usage:
    python3 scripts/validate_recaudos.py /path/to/recaudos_dir/

    The directory should contain files named like:
      S12.26 F CMundo $ 1,065.39.xlsx
      S13.26 F CMundo $ 17,214.42.xlsx
      ...

    Requires DATABASE_URL env var for DB comparison.
    Can run in --dry-run mode (no DB) to just analyze the Excel files.
"""

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("validate_recaudos")


def parse_recaudos_excels(directory: str) -> list[dict]:
    """Parse all recaudos xlsx files in directory → flat list of rows."""
    import openpyxl

    rows = []
    for fname in sorted(os.listdir(directory)):
        if not fname.endswith('.xlsx') or fname.startswith('~'):
            continue
        if 'CMundo' not in fname and 'cmundo' not in fname.lower():
            continue

        path = os.path.join(directory, fname)
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active

        for row in ws.iter_rows(min_row=4, values_only=True):
            if row[0] is None:
                continue
            try:
                financiera = str(row[0]).strip()
                placa = str(row[1]).strip().upper()
                cantidad_recaudo = float(row[2] or 0)
                fecha_hora = row[3]  # datetime
                litros = float(row[4] or 0)
                ticket = str(row[5]).strip()
                estacion = str(row[6]).strip()
                valor_recaudo = float(row[9] or 0)  # tarifa per LEQ
                id_credito = str(row[11] or '').strip()

                rows.append({
                    'source_file': fname,
                    'financiera': financiera,
                    'placa': placa,
                    'cantidad_recaudo': cantidad_recaudo,
                    'fecha_hora': fecha_hora,
                    'litros': litros,
                    'ticket': ticket,
                    'estacion': estacion,
                    'valor_recaudo': valor_recaudo,
                    'id_credito': id_credito,
                    # Verify formula
                    'calculated': round(litros * valor_recaudo, 2),
                    'formula_ok': abs(cantidad_recaudo - round(litros * valor_recaudo, 2)) < 0.05,
                })
            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"Skipping row in {fname}: {e}")
                continue

        wb.close()
        logger.info(f"Parsed {fname}: found rows up to now = {len(rows)}")

    return rows


def cross_reference_db(recaudos: list[dict]) -> dict:
    """
    Cross-reference recaudo rows against DB transactions.

    Match criteria: placa + DATE(timestamp_local) + ABS(litros diff) < 0.1
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.warning("DATABASE_URL not set — skipping DB cross-reference")
        return {"skipped": True}

    import psycopg2

    conn = psycopg2.connect(db_url)
    matched = 0
    mismatched = 0
    orphans = 0
    details = []

    try:
        with conn.cursor() as cur:
            for r in recaudos:
                if not r['fecha_hora'] or not isinstance(r['fecha_hora'], datetime):
                    orphans += 1
                    continue

                # Search for matching transaction
                fecha = r['fecha_hora']
                cur.execute("""
                    SELECT id, litros, recaudo_pagado, recaudo_valor,
                           timestamp_local, station_natgas
                    FROM transactions
                    WHERE placa = %s
                      AND timestamp_local BETWEEN %s AND %s
                      AND ABS(litros - %s) < 0.5
                    ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp_local - %s)))
                    LIMIT 1
                """, (
                    r['placa'],
                    fecha - timedelta(hours=1),
                    fecha + timedelta(hours=1),
                    r['litros'],
                    fecha,
                ))

                row = cur.fetchone()
                if row is None:
                    orphans += 1
                    if orphans <= 10:
                        details.append({
                            'status': 'ORPHAN',
                            'placa': r['placa'],
                            'fecha': str(r['fecha_hora']),
                            'litros': r['litros'],
                            'estacion': r['estacion'],
                            'note': 'No matching transaction in DB',
                        })
                    continue

                db_id, db_litros, db_tarifa, db_total, db_ts, db_station = row

                # Compare
                excel_total = r['cantidad_recaudo']
                db_total_f = float(db_total or 0)
                tarifa_match = abs(float(db_tarifa or 0) - r['valor_recaudo']) < 0.01
                total_match = abs(db_total_f - excel_total) < 1.0

                if tarifa_match and total_match:
                    matched += 1
                else:
                    mismatched += 1
                    if mismatched <= 10:
                        details.append({
                            'status': 'MISMATCH',
                            'placa': r['placa'],
                            'db_id': db_id,
                            'excel_tarifa': r['valor_recaudo'],
                            'db_tarifa': float(db_tarifa or 0),
                            'excel_total': excel_total,
                            'db_total': db_total_f,
                            'tarifa_ok': tarifa_match,
                            'total_ok': total_match,
                        })
    finally:
        conn.close()

    return {
        'skipped': False,
        'matched': matched,
        'mismatched': mismatched,
        'orphans': orphans,
        'total': len(recaudos),
        'match_rate': round(100 * matched / max(1, matched + mismatched), 1),
        'details': details,
    }


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <directory_with_recaudos_xlsx>")
        print(f"       Set DATABASE_URL env for DB cross-reference")
        print(f"       Use --dry-run to skip DB check")
        sys.exit(1)

    directory = sys.argv[1]
    dry_run = '--dry-run' in sys.argv

    if not os.path.isdir(directory):
        logger.error(f"Not a directory: {directory}")
        sys.exit(1)

    # 1. Parse all Excel files
    recaudos = parse_recaudos_excels(directory)
    if not recaudos:
        logger.warning("No recaudo rows found")
        sys.exit(0)

    # 2. Analyze Excel-only stats
    print(f"\n{'='*60}")
    print(f"RECAUDOS EXCEL ANALYSIS")
    print(f"{'='*60}")
    print(f"Total rows:          {len(recaudos)}")

    placas = set(r['placa'] for r in recaudos)
    print(f"Unique placas:       {len(placas)}")

    total_recaudo = sum(r['cantidad_recaudo'] for r in recaudos)
    total_litros = sum(r['litros'] for r in recaudos)
    print(f"Total recaudo:       ${total_recaudo:,.2f}")
    print(f"Total litros:        {total_litros:,.2f}")

    # Formula check
    formula_ok = sum(1 for r in recaudos if r['formula_ok'])
    print(f"Formula check:       {formula_ok}/{len(recaudos)} ({100*formula_ok/len(recaudos):.1f}%)")
    print(f"  (cantidad = litros × valor_recaudo)")

    # By estacion
    by_estacion = defaultdict(int)
    for r in recaudos:
        by_estacion[r['estacion']] += 1
    print(f"\nBy estación:")
    for est, count in sorted(by_estacion.items(), key=lambda x: -x[1]):
        print(f"  {est:20s}: {count:4d} ({100*count/len(recaudos):.1f}%)")

    # By placa
    by_placa = defaultdict(lambda: {'count': 0, 'litros': 0, 'recaudo': 0, 'tarifa': set()})
    for r in recaudos:
        bp = by_placa[r['placa']]
        bp['count'] += 1
        bp['litros'] += r['litros']
        bp['recaudo'] += r['cantidad_recaudo']
        bp['tarifa'].add(r['valor_recaudo'])
    print(f"\nBy placa:")
    for placa in sorted(by_placa, key=lambda p: -by_placa[p]['recaudo']):
        bp = by_placa[placa]
        tarifas = ', '.join(f'${t}' for t in sorted(bp['tarifa']))
        print(f"  {placa:10s}: {bp['count']:3d} cargas, {bp['litros']:8.1f} LEQ, ${bp['recaudo']:10,.2f} recaudo, tarifa={tarifas}")

    # 3. Cross-reference DB (unless dry-run)
    if dry_run:
        print(f"\n[--dry-run] Skipping DB cross-reference")
    else:
        print(f"\n{'='*60}")
        print(f"DB CROSS-REFERENCE")
        print(f"{'='*60}")
        result = cross_reference_db(recaudos)
        if result.get('skipped'):
            print("  Skipped (no DATABASE_URL)")
        else:
            print(f"  Matched:    {result['matched']}")
            print(f"  Mismatched: {result['mismatched']}")
            print(f"  Orphans:    {result['orphans']} (no match in DB)")
            print(f"  Match rate: {result['match_rate']}% (of non-orphan)")
            if result['details']:
                print(f"\n  Details (first 10):")
                for d in result['details']:
                    if d['status'] == 'ORPHAN':
                        print(f"    ORPHAN: {d['placa']} @ {d['fecha']} ({d['litros']} LEQ) [{d['estacion']}]")
                    else:
                        print(f"    MISMATCH: {d['placa']} DB#{d['db_id']}: "
                              f"tarifa excel=${d['excel_tarifa']} vs db=${d['db_tarifa']} "
                              f"| total excel=${d['excel_total']:.2f} vs db=${d['db_total']:.2f}")

    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
