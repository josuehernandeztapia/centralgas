"""
Tests for GasUp CSV Parser using REAL data from Plataforma_GASUP/Datasets_GNC.

Expected results calibrated against analysis of 388,665 transactions:
- Pre-2023: 276,120 rows across 12 files (17 columns)
- Post-2023: 112,545 rows across 6 files (13 columns)
- Total unique plates: 331
- Nacozari: 73.8% of volume
- Medios de pago (pre-2023): Efectivo 83.9%, Prepago 10.6%
"""

import sys
from pathlib import Path
from collections import Counter
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.parsers.gasup import parse_csv, parse_directory, detect_schema, SchemaVersion
from app.models.transaction import AnomalyType

# Path to real CSVs
DATASETS_DIR = Path("/sessions/cool-focused-pasteur/mnt/Downloads/CMU/Plataforma_Tech/Plataforma_GASUP/Datasets_GNC")

# ============================================================
# Test schema detection
# ============================================================

def test_schema_detection_pre2023():
    """Pre-2023 files should be detected by 'Fecha de venta' (no 's')."""
    f = DATASETS_DIR / "2022 NOV-DIC AGS Combis.csv"
    with open(f, encoding="utf-8-sig") as fh:
        header = fh.readline()
    assert detect_schema(header) == SchemaVersion.PRE_2023


def test_schema_detection_post2023():
    """Post-2023 files should be detected by 'Fecha de ventas' (with 's')."""
    f = DATASETS_DIR / "2025 MAY-JUL AGS Combis.csv"
    with open(f, encoding="utf-8-sig") as fh:
        header = fh.readline()
    assert detect_schema(header) == SchemaVersion.POST_2023


# ============================================================
# Test individual file parsing
# ============================================================

def test_parse_pre2023_file():
    """Parse a pre-2023 file and verify structure."""
    f = DATASETS_DIR / "2022 NOV-DIC AGS Combis.csv"
    result = parse_csv(f)

    assert result.schema_version == SchemaVersion.PRE_2023
    assert result.total_rows == 11176, f"Expected 11176 rows, got {result.total_rows}"
    assert result.error_rows == 0, f"Got {result.error_rows} errors"
    assert result.parsed_rows == 11176

    # Check first transaction has medio_pago (pre-2023 feature)
    txn0 = result.transactions[0]
    assert txn0.medio_pago is not None, "Pre-2023 should have medio_pago"
    assert txn0.segmento is not None, "Pre-2023 should have segmento"

    # Check derived fields
    assert txn0.kg > 0, "kg should be calculated"
    assert txn0.nm3 > 0, "nm3 should be calculated"
    assert txn0.ingreso_neto > 0, "ingreso_neto should be calculated"
    assert txn0.iva > 0, "iva should be calculated"
    assert txn0.ingreso_neto + txn0.iva == txn0.total_mxn, "neto + iva should = total"

    print(f"  OK: {result.summary()}")


def test_parse_post2023_file():
    """Parse a post-2023 file and verify structure."""
    f = DATASETS_DIR / "2025 MAY-JUL AGS Combis.csv"
    result = parse_csv(f)

    assert result.schema_version == SchemaVersion.POST_2023
    assert result.total_rows == 18362, f"Expected 18362 rows, got {result.total_rows}"
    assert result.error_rows == 0, f"Got {result.error_rows} errors"

    # Post-2023 should NOT have medio_pago
    txn0 = result.transactions[0]
    assert txn0.medio_pago is None, "Post-2023 should not have medio_pago"
    assert txn0.segmento is None, "Post-2023 should not have segmento"

    # Verify litros with 4 decimals are handled
    has_decimal = any(
        txn.litros != txn.litros.to_integral_value()
        for txn in result.transactions[:100]
    )
    assert has_decimal, "Post-2023 should have litros with decimals"

    print(f"  OK: {result.summary()}")


# ============================================================
# Test full directory parse (all 18 files)
# ============================================================

def test_parse_all_files():
    """Parse all 18 CSV files and validate against known totals."""
    results = parse_directory(DATASETS_DIR)

    # Count schemas
    pre = [r for r in results if r.schema_version == SchemaVersion.PRE_2023]
    post = [r for r in results if r.schema_version == SchemaVersion.POST_2023]
    assert len(pre) == 12, f"Expected 12 pre-2023 files, got {len(pre)}"
    assert len(post) == 6, f"Expected 6 post-2023 files, got {len(post)}"

    # Total records
    total_pre = sum(r.parsed_rows for r in pre)
    total_post = sum(r.parsed_rows for r in post)
    total = total_pre + total_post

    print(f"\n  Pre-2023:  {total_pre:>7,} rows")
    print(f"  Post-2023: {total_post:>7,} rows")
    print(f"  TOTAL:     {total:>7,} rows")

    assert total_pre == 276120, f"Expected 276,120 pre-2023, got {total_pre}"
    assert total_post == 112545, f"Expected 112,545 post-2023, got {total_post}"
    assert total == 388665, f"Expected 388,665 total, got {total}"

    # Unique plates
    all_txns = [txn for r in results for txn in r.transactions]
    plates = set(txn.placa for txn in all_txns)
    print(f"  Unique plates: {len(plates)}")
    assert len(plates) == 331, f"Expected 331 unique plates, got {len(plates)}"

    # Station distribution
    station_counts = Counter(txn.station_natgas for txn in all_txns)
    nacozari_pct = station_counts.get("EDS Nacozari", 0) / total * 100
    print(f"  Nacozari: {nacozari_pct:.1f}% ({station_counts.get('EDS Nacozari', 0):,})")
    assert 73 < nacozari_pct < 75, f"Nacozari should be ~73.8%, got {nacozari_pct:.1f}%"

    # Medio de pago distribution (pre-2023 only)
    pre_txns = [txn for r in pre for txn in r.transactions]
    medio_counts = Counter(txn.medio_pago.value for txn in pre_txns if txn.medio_pago)
    efectivo_pct = medio_counts.get("EFECTIVO", 0) / len(pre_txns) * 100
    print(f"  Efectivo (pre-2023): {efectivo_pct:.1f}%")
    assert 83 < efectivo_pct < 85, f"Efectivo should be ~83.9%, got {efectivo_pct:.1f}%"

    # Zero errors
    total_errors = sum(r.error_rows for r in results)
    print(f"  Total errors: {total_errors}")
    assert total_errors == 0, f"Expected 0 errors, got {total_errors}"

    # Anomalies (should exist but be reasonable)
    total_anomalies = sum(r.anomaly_count for r in results)
    print(f"  Total anomalies flagged: {total_anomalies}")

    print(f"\n  ALL TESTS PASSED")


# ============================================================
# Test anomaly detection
# ============================================================

def test_anomaly_detection():
    """Verify anomaly flags on known edge cases."""
    f = DATASETS_DIR / "2025 MAY-JUL AGS Combis.csv"
    result = parse_csv(f)

    anomaly_types = Counter()
    for txn in result.transactions:
        for a in txn.anomalies:
            anomaly_types[a.type] += 1

    print(f"\n  Anomalies in {f.name}:")
    for atype, count in anomaly_types.most_common():
        print(f"    {atype.value}: {count}")

    # There should be some anomalies but not too many
    assert result.anomaly_count > 0, "Should detect some anomalies"
    assert result.anomaly_count < result.parsed_rows * 0.1, "Anomalies should be <10%"

    print(f"  OK: {result.anomaly_count} anomalies in {result.parsed_rows} rows")


# ============================================================
# Run all tests
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("GasUp Parser Tests — Real Data (388K transactions)")
    print("=" * 60)

    tests = [
        ("Schema detection (pre-2023)", test_schema_detection_pre2023),
        ("Schema detection (post-2023)", test_schema_detection_post2023),
        ("Parse pre-2023 file", test_parse_pre2023_file),
        ("Parse post-2023 file", test_parse_post2023_file),
        ("Anomaly detection", test_anomaly_detection),
        ("Parse ALL 18 files (388K rows)", test_parse_all_files),
    ]

    passed = 0
    failed = 0
    for name, test_fn in tests:
        print(f"\n--- {name} ---")
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"  FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("=" * 60)
