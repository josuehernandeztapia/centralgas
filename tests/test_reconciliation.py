"""
Tests for Daily Close Reconciliation Service.

Tests use REAL GasUp data (388K transactions) to validate that
the reconciliation engine produces correct Block 1 (Cols A-P)
output and that all check logic works correctly.

Reference values from NatGas Excel "2026.CIERRE OPERACIONES EDS 5 FEB.xlsx":
  - Dec 31, 2025: 4653.66 litros, PVP $13.99, efectivo $52,085.65
  - Jan 1, 2026: 2681.07 litros, PVP $13.99, efectivo $30,032.25
"""

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.reconciliation import (
    BankBlock,
    CashSummaryBlock,
    CheckResult,
    CloseStatus,
    CompusafeBlock,
    DailyCloseResult,
    ETVBlock,
    GasUpDailyBlock,
    IncomeBlock,
    ReconciliationCheck,
    ReconciliationThresholds,
    TPVAuditBlock,
)
from app.services.reconciliation import (
    build_cash_summary,
    build_gasup_block,
    build_bank_block,
    build_income_block,
    build_tpv_audit,
    determine_status,
    format_whatsapp_message,
    format_summary_message,
    run_checks,
    run_daily_close,
)
from app.models.transaction import (
    MedioPago,
    SchemaVersion,
    TransactionNormalized,
    CST,
)
from app.parsers.gasup import parse_csv, parse_directory

# Path to real CSVs
DATASETS_DIR = Path("/sessions/cool-focused-pasteur/mnt/Downloads/CMU/Plataforma_Tech/Plataforma_GASUP/Datasets_GNC")


# ============================================================
# Helper: create a synthetic transaction for unit tests
# ============================================================

def make_txn(
    placa: str = "A12345A",
    station_id: int = 3,
    litros: Decimal = Decimal("25.0000"),
    pvp: Decimal = Decimal("13.99"),
    total_mxn: Decimal = Decimal("349.75"),
    medio_pago: MedioPago = MedioPago.EFECTIVO,
    recaudo: Decimal = Decimal("0"),
    local_date: date = date(2025, 12, 31),
) -> TransactionNormalized:
    """Create a synthetic normalized transaction for testing."""
    local_dt = datetime(local_date.year, local_date.month, local_date.day, 10, 0, 0, tzinfo=CST)
    utc_dt = local_dt.astimezone(timezone.utc)
    ingreso_neto = (total_mxn / Decimal("1.16")).quantize(Decimal("0.01"))
    iva = total_mxn - ingreso_neto
    kg = (litros * Decimal("0.717")).quantize(Decimal("0.0001"))

    return TransactionNormalized(
        source_file="test.csv",
        source_row=1,
        schema_version=SchemaVersion.PRE_2023,
        station_id=station_id,
        station_natgas="EDS Nacozari",
        plaza="AGUASCALIENTES",
        timestamp_utc=utc_dt,
        timestamp_local=local_dt,
        placa=placa,
        litros=litros,
        pvp=pvp,
        total_mxn=total_mxn,
        recaudo_pagado=recaudo,
        medio_pago=medio_pago,
        kg=kg,
        nm3=litros,
        ingreso_neto=ingreso_neto,
        iva=iva,
    )


# ============================================================
# Test Block 1: GasUp daily block builder
# ============================================================

def test_gasup_block_basic():
    """Build GasUp block from synthetic transactions and verify totals."""
    d = date(2025, 12, 31)
    txns = [
        make_txn(placa="A00001A", litros=Decimal("30.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("419.70"), medio_pago=MedioPago.EFECTIVO, local_date=d),
        make_txn(placa="A00002A", litros=Decimal("20.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("279.80"), medio_pago=MedioPago.EFECTIVO, local_date=d),
        make_txn(placa="A00003A", litros=Decimal("15.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("209.85"), medio_pago=MedioPago.PREPAGO, local_date=d),
        make_txn(placa="A00004A", litros=Decimal("25.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("349.75"), medio_pago=MedioPago.TARJETA_DEBITO, local_date=d),
        make_txn(placa="A00005A", litros=Decimal("10.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("139.90"), recaudo=Decimal("50.00"),
                 medio_pago=MedioPago.EFECTIVO, local_date=d),
    ]

    block = build_gasup_block(txns, d, station_id=3)

    assert block.close_date == d
    assert block.num_cargas == 5
    assert block.unique_placas == 5

    # Litros: 30 + 20 + 10 = 60 efectivo, 15 prepago, 25 tarjeta
    assert block.litros_contado == Decimal("60.00"), f"Got {block.litros_contado}"
    assert block.litros_prepago == Decimal("15.00"), f"Got {block.litros_prepago}"

    # Total litros = 30 + 20 + 15 + 25 + 10 = 100
    assert block.total_litros == Decimal("100.00"), f"Got {block.total_litros}"

    # TPV (tarjeta debito)
    assert block.tpv_total == Decimal("349.75"), f"Got {block.tpv_total}"

    # Recaudos
    assert block.total_recaudos == Decimal("50.00"), f"Got {block.total_recaudos}"

    print(f"  OK: GasUp block — {block.num_cargas} cargas, {block.total_litros} lt, ${block.total_mxn}")


def test_gasup_block_empty():
    """Empty transaction list should return zero block."""
    block = build_gasup_block([], date(2025, 12, 31), station_id=3)
    assert block.total_litros == Decimal("0")
    assert block.num_cargas == 0
    print("  OK: Empty GasUp block")


def test_gasup_block_filters_by_date_and_station():
    """Transactions for other dates/stations should be excluded."""
    d = date(2025, 12, 31)
    txns = [
        make_txn(station_id=3, local_date=d),              # Include
        make_txn(station_id=3, local_date=date(2026, 1, 1)),  # Exclude: wrong date
        make_txn(station_id=1, local_date=d),              # Exclude: wrong station
    ]
    block = build_gasup_block(txns, d, station_id=3)
    assert block.num_cargas == 1
    print("  OK: Filters by date and station")


# ============================================================
# Test Block 3: Cash summary
# ============================================================

def test_cash_summary():
    """Cash summary: picos = P - S, diferencia should be ~$0."""
    gasup = GasUpDailyBlock(
        close_date=date(2025, 12, 31),
        ventas_mas_recaudos=Decimal("55993.45"),  # Col P
    )
    compusafe = CompusafeBlock(
        efectivo_ingresado=Decimal("55950.00"),   # Col S (from NatGas data)
    )
    cs = build_cash_summary(gasup, compusafe)

    # Picos = 55993.45 - 55950.00 = 43.45
    assert cs.picos_reales == Decimal("43.45"), f"Got {cs.picos_reales}"
    # Total OASIS = P = 55993.45
    assert cs.total_efectivo_oasis == Decimal("55993.45")
    # Efectivo obtenido = S + T = 55950 + 43.45 = 55993.45
    assert cs.efectivo_total_obtenido == Decimal("55993.45")
    # Diferencia = U - V = 0
    assert cs.diferencia_efectivo == Decimal("0.00")

    print(f"  OK: Cash summary — picos ${cs.picos_reales}, diff ${cs.diferencia_efectivo}")


def test_cash_summary_with_real_natgas_values():
    """Use actual NatGas Excel values for Dec 31, 2025."""
    # From NatGas Excel row 8 (Dec 31, 2025):
    # P = 55993.4518, S = 55950, T = 43, V = 55993, W = -0.4518
    gasup = GasUpDailyBlock(
        close_date=date(2025, 12, 31),
        ventas_mas_recaudos=Decimal("55993.45"),  # P rounded to 2 dec
    )
    compusafe = CompusafeBlock(
        efectivo_ingresado=Decimal("55950.00"),
    )
    cs = build_cash_summary(gasup, compusafe)

    # NatGas shows picos = 43, our calc should be 43.45
    # The small difference is because NatGas rounds differently
    assert abs(cs.picos_reales - Decimal("43")) < Decimal("1"), \
        f"Picos should be ~$43, got ${cs.picos_reales}"
    # Diferencia should be essentially $0
    assert abs(cs.diferencia_efectivo) < Decimal("1"), \
        f"Cash difference should be ~$0, got ${cs.diferencia_efectivo}"

    print(f"  OK: NatGas Dec 31 values match — picos ${cs.picos_reales}")


# ============================================================
# Test Block 5: Bank reconciliation
# ============================================================

def test_bank_block():
    """Bank block: AF = AD - AC - AE."""
    gasup = GasUpDailyBlock(
        close_date=date(2025, 12, 31),
        ventas_mas_recaudos=Decimal("55993.45"),
    )
    cs = CashSummaryBlock(picos_reales=Decimal("43.45"))

    bank = build_bank_block(
        gasup, cs,
        banco_monto=Decimal("55950.00"),  # AC from NatGas
    )

    # AF = 55993.45 - 55950 - 43.45 = 0.00
    assert bank.diferencia_vs_efectivo == Decimal("0.00"), \
        f"Expected $0, got ${bank.diferencia_vs_efectivo}"

    print(f"  OK: Bank block — diff ${bank.diferencia_vs_efectivo}")


# ============================================================
# Test Block 6: TPV audit
# ============================================================

def test_tpv_audit():
    """TPV audit: AH = AG - L."""
    gasup = GasUpDailyBlock(
        close_date=date(2025, 12, 31),
        tpv_total=Decimal("7992.99"),  # L from NatGas Dec 31
    )
    # AG from NatGas: 8031.04
    tpv = build_tpv_audit(gasup, Decimal("8031.04"))

    # AH = 8031.04 - 7992.99 = 38.05
    assert tpv.diferencia_vs_gasdata == Decimal("38.05"), \
        f"Expected $38.05, got ${tpv.diferencia_vs_gasdata}"

    print(f"  OK: TPV audit — diff ${tpv.diferencia_vs_gasdata}")


# ============================================================
# Test reconciliation checks
# ============================================================

def test_checks_all_pass():
    """All checks should pass when deltas are within thresholds."""
    d = date(2025, 12, 31)
    result = DailyCloseResult(
        station_id=3,
        station_name="Nacozari",
        close_date=d,
        generated_at=datetime.now(timezone.utc),
        gasup=GasUpDailyBlock(
            close_date=d,
            litros_contado=Decimal("4596.82"),
            litros_diff_precio=Decimal("0"),
            litros_credito=Decimal("0"),
            litros_prepago=Decimal("56.84"),
            litros_tarjeta=Decimal("0"),
            litros_bonos=Decimal("0"),
            litros_otros=Decimal("0"),
            total_litros=Decimal("4653.66"),
            tpv_total=Decimal("7992.99"),
            total_venta_efectivo=Decimal("52085.65"),
            total_recaudos=Decimal("3907.80"),
            ventas_mas_recaudos=Decimal("55993.45"),
        ),
        compusafe=CompusafeBlock(efectivo_ingresado=Decimal("55950.00")),
        cash_summary=CashSummaryBlock(
            picos_reales=Decimal("43.45"),
            total_efectivo_oasis=Decimal("55993.45"),
            efectivo_total_obtenido=Decimal("55993.45"),
            diferencia_efectivo=Decimal("0.00"),
        ),
        etv=ETVBlock(dia_venta=d, importe=Decimal("43.45")),
        bank=BankBlock(
            banco_monto=Decimal("55950.00"),
            ventas_recaudos_check=Decimal("55993.45"),
            picos_necesarios=Decimal("43.45"),
            diferencia_vs_efectivo=Decimal("0.00"),
        ),
        tpv_audit=TPVAuditBlock(
            tiras_auditoras=Decimal("7992.99"),
            diferencia_vs_gasdata=Decimal("0.00"),
        ),
        income=IncomeBlock(
            bancos=Decimal("55993.45"),
            diferencia=Decimal("0.00"),
        ),
    )

    thresholds = ReconciliationThresholds()
    checks = run_checks(result, thresholds)

    passing = [c for c in checks if c.result == CheckResult.PASS]
    failing = [c for c in checks if c.result == CheckResult.FAIL]

    print(f"  Checks: {len(passing)} pass, {len(failing)} fail")
    for c in checks:
        print(f"    {c.result.value:4s} {c.check_id}: {c.detail}")

    assert len(failing) == 0, f"Expected 0 failures, got {len(failing)}"
    print(f"  OK: All checks pass")


def test_checks_detect_warning():
    """A $60 TPV discrepancy should trigger WARNING."""
    d = date(2025, 12, 31)
    result = DailyCloseResult(
        station_id=3,
        station_name="Nacozari",
        close_date=d,
        generated_at=datetime.now(timezone.utc),
        gasup=GasUpDailyBlock(
            close_date=d, total_litros=Decimal("4000"),
            litros_contado=Decimal("3000"),
            litros_tarjeta=Decimal("1000"),
            tpv_total=Decimal("5000"),
            ventas_mas_recaudos=Decimal("50000"),
        ),
        compusafe=CompusafeBlock(efectivo_ingresado=Decimal("49800")),
        cash_summary=CashSummaryBlock(
            picos_reales=Decimal("200"),
            total_efectivo_oasis=Decimal("50000"),
            efectivo_total_obtenido=Decimal("50000"),
            diferencia_efectivo=Decimal("0"),
        ),
        etv=ETVBlock(dia_venta=d, importe=Decimal("200")),
        bank=BankBlock(
            banco_monto=Decimal("49800"),
            ventas_recaudos_check=Decimal("50000"),
            picos_necesarios=Decimal("200"),
            diferencia_vs_efectivo=Decimal("0"),
        ),
        tpv_audit=TPVAuditBlock(
            tiras_auditoras=Decimal("5060"),   # $60 off — triggers TPV warn
            diferencia_vs_gasdata=Decimal("60"),
        ),
        income=IncomeBlock(
            bancos=Decimal("50000"),
            diferencia=Decimal("0"),
        ),
    )

    checks = run_checks(result, ReconciliationThresholds())
    tpv_check = next(c for c in checks if c.check_id == "CHK_TPV_AUDIT")
    assert tpv_check.result == CheckResult.WARN, f"Expected WARN, got {tpv_check.result}"

    status = determine_status(checks)
    assert status == CloseStatus.WARNING
    print(f"  OK: TPV $60 diff → WARNING status")


def test_checks_detect_critical():
    """A $600 bank discrepancy should trigger CRITICAL."""
    d = date(2025, 12, 31)
    result = DailyCloseResult(
        station_id=3,
        station_name="Nacozari",
        close_date=d,
        generated_at=datetime.now(timezone.utc),
        gasup=GasUpDailyBlock(
            close_date=d, total_litros=Decimal("4000"),
            litros_contado=Decimal("3000"),
            litros_tarjeta=Decimal("1000"),
            tpv_total=Decimal("5000"),
            ventas_mas_recaudos=Decimal("50000"),
        ),
        compusafe=CompusafeBlock(efectivo_ingresado=Decimal("49800")),
        cash_summary=CashSummaryBlock(
            picos_reales=Decimal("200"),
            total_efectivo_oasis=Decimal("50000"),
            efectivo_total_obtenido=Decimal("50000"),
            diferencia_efectivo=Decimal("0"),
        ),
        etv=ETVBlock(dia_venta=d, importe=Decimal("200")),
        bank=BankBlock(
            banco_monto=Decimal("49200"),      # $600 short!
            ventas_recaudos_check=Decimal("50000"),
            picos_necesarios=Decimal("200"),
            diferencia_vs_efectivo=Decimal("-600"),
        ),
        tpv_audit=TPVAuditBlock(
            tiras_auditoras=Decimal("5000"),
            diferencia_vs_gasdata=Decimal("0"),
        ),
        income=IncomeBlock(
            bancos=Decimal("49400"),
            diferencia=Decimal("0"),
        ),
    )

    checks = run_checks(result, ReconciliationThresholds())
    bank_check = next(c for c in checks if c.check_id == "CHK_BANK_VS_CASH")
    assert bank_check.result == CheckResult.FAIL, f"Expected FAIL, got {bank_check.result}"

    status = determine_status(checks)
    assert status == CloseStatus.CRITICAL
    print(f"  OK: Bank $600 short → CRITICAL status")


# ============================================================
# Test full daily close pipeline (synthetic data)
# ============================================================

def test_full_daily_close_synthetic():
    """Run full daily close with synthetic data."""
    d = date(2025, 12, 31)
    txns = [
        make_txn(placa=f"A0000{i}A", litros=Decimal("30.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("419.70"), medio_pago=MedioPago.EFECTIVO, local_date=d)
        for i in range(10)
    ] + [
        make_txn(placa=f"A0001{i}A", litros=Decimal("20.0000"), pvp=Decimal("13.99"),
                 total_mxn=Decimal("279.80"), medio_pago=MedioPago.TARJETA_DEBITO, local_date=d)
        for i in range(5)
    ]

    result = run_daily_close(
        station_id=3,
        close_date=d,
        transactions=txns,
        compusafe_efectivo=Decimal("4197.00"),  # 10 × $419.70 = $4,197
        banco_monto=Decimal("4197.00"),
        banco_ingreso=Decimal("4197.00"),
        tpv_tiras=Decimal("1399.00"),           # 5 × $279.80 TPV = $1,399
    )

    assert result.station_id == 3
    assert result.close_date == d
    assert result.gasup.num_cargas == 15
    assert result.gasup.total_litros == Decimal("400.00")  # 10×30 + 5×20

    # Should be OK or WARNING (not CRITICAL)
    assert result.status in (CloseStatus.OK, CloseStatus.WARNING, CloseStatus.PARTIAL), \
        f"Expected OK/WARNING/PARTIAL, got {result.status}"
    assert result.whatsapp_message != ""

    print(f"  OK: Full pipeline — {result.status.value}, {len(result.checks)} checks")
    print(f"  WhatsApp preview (first 3 lines):")
    for line in result.whatsapp_message.split("\n")[:3]:
        print(f"    {line}")


# ============================================================
# Test WhatsApp message format
# ============================================================

def test_whatsapp_format():
    """WhatsApp message should include all key sections."""
    d = date(2025, 12, 31)
    txns = [make_txn(local_date=d) for _ in range(5)]

    result = run_daily_close(
        station_id=3,
        close_date=d,
        transactions=txns,
        compusafe_efectivo=Decimal("1748.75"),
        banco_monto=Decimal("1748.75"),
        banco_ingreso=Decimal("1748.75"),
    )

    msg = result.whatsapp_message
    assert "CIERRE DIARIO" in msg
    assert "Nacozari" in msg or "Pensión" in msg
    assert "31/12/2025" in msg
    assert "Ventas del día" in msg
    assert "Reconciliación" in msg
    assert "Estado:" in msg

    print(f"  OK: WhatsApp message format validated")


# ============================================================
# Test with REAL GasUp data (integration test)
# ============================================================

def test_gasup_block_from_real_data():
    """Build GasUp block from real parsed CSV data for a known station."""
    if not DATASETS_DIR.exists():
        print("  SKIP: Dataset directory not found")
        return

    # Parse a recent file
    f = DATASETS_DIR / "2025 MAY-JUL AGS Combis.csv"
    if not f.exists():
        print(f"  SKIP: {f.name} not found")
        return

    from app.parsers.gasup import parse_csv
    result = parse_csv(f)

    # Pick the first date with enough transactions for station 3 (Nacozari)
    from collections import Counter
    date_station_counts = Counter()
    for txn in result.transactions:
        if txn.station_id == 3:
            date_station_counts[txn.timestamp_local.date()] += 1

    if not date_station_counts:
        print("  SKIP: No Nacozari transactions found")
        return

    # Pick busiest date
    best_date, count = date_station_counts.most_common(1)[0]
    print(f"  Testing with {best_date} ({count} Nacozari txns)")

    block = build_gasup_block(result.transactions, best_date, station_id=3)

    assert block.num_cargas == count, f"Expected {count} cargas, got {block.num_cargas}"
    assert block.total_litros > 0, "Should have some litros"
    assert block.total_mxn > 0, "Should have some revenue"

    # Sanity: avg litros should be reasonable (10-50 lt)
    avg = block.avg_litros_per_carga
    assert Decimal("5") < avg < Decimal("60"), f"Avg litros {avg} seems wrong"

    print(f"  OK: Real data — {block.num_cargas} cargas, {block.total_litros} lt, "
          f"${block.total_mxn}, avg {avg} lt/carga")


# ============================================================
# Run all tests
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Daily Close Reconciliation Tests")
    print("=" * 60)

    tests = [
        ("GasUp block (synthetic)", test_gasup_block_basic),
        ("GasUp block (empty)", test_gasup_block_empty),
        ("GasUp block (date/station filter)", test_gasup_block_filters_by_date_and_station),
        ("Cash summary", test_cash_summary),
        ("Cash summary (NatGas Dec 31 values)", test_cash_summary_with_real_natgas_values),
        ("Bank block", test_bank_block),
        ("TPV audit", test_tpv_audit),
        ("Checks: all pass", test_checks_all_pass),
        ("Checks: TPV warning", test_checks_detect_warning),
        ("Checks: bank critical", test_checks_detect_critical),
        ("Full pipeline (synthetic)", test_full_daily_close_synthetic),
        ("WhatsApp format", test_whatsapp_format),
        ("GasUp block (REAL data)", test_gasup_block_from_real_data),
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
            print(f"  ERROR: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("=" * 60)
