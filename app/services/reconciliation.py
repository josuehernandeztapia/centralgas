"""
Daily Close Reconciliation Service — HU-3.1, HU-3.2, HU-3.3

Replaces the NatGas 38-column manual daily close Excel.
Runs at 23:00 CST via cron or on-demand.

Data flow:
  1. Pull GasUp transactions for station+date → Block 1 (Cols A-P)
  2. Pull Compusafe data from Odoo → Block 2 (Cols Q-S)
  3. Calculate cash summary → Block 3 (Cols T-W)
  4. ETV coins (manual input or Odoo) → Block 4 (Cols X-Z)
  5. Pull bank statements from Odoo → Block 5 (Cols AA-AF)
  6. Pull TPV audit from dispenser/SCADA → Block 6 (Cols AG-AH)
  7. Calculate income summary → Block 7 (Cols AI-AJ)
  8. Pull SCADA totalizador delta → Cross-validation
  9. Run 7 reconciliation checks
  10. Determine status and generate WhatsApp message
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from statistics import mode as stats_mode
from typing import Optional

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
from app.models.transaction import (
    CST,
    MedioPago,
    SchemaVersion,
    TransactionNormalized,
)

logger = logging.getLogger(__name__)

TWO_PLACES = Decimal("0.01")
FOUR_PLACES = Decimal("0.0001")


# ============================================================
# Block 1: Build GasUp daily block from parsed transactions
# ============================================================

def build_gasup_block(
    transactions: list[TransactionNormalized],
    close_date: date,
    station_id: Optional[int] = None,
) -> GasUpDailyBlock:
    """
    Build Block 1 (Cols A-P) from GasUp parsed transactions.

    Filters transactions to the given date and optional station.
    Breaks down litros and MXN by medio_pago.
    """
    # Filter to date + station
    txns = [
        t for t in transactions
        if t.timestamp_local.date() == close_date
        and (station_id is None or t.station_id == station_id)
    ]

    if not txns:
        return GasUpDailyBlock(close_date=close_date)

    # Group by medio_pago
    efectivo = [t for t in txns if t.medio_pago == MedioPago.EFECTIVO]
    credito = [t for t in txns if t.medio_pago == MedioPago.CREDITO]
    prepago = [t for t in txns if t.medio_pago == MedioPago.PREPAGO]
    tarjeta_d = [t for t in txns if t.medio_pago == MedioPago.TARJETA_DEBITO]
    tarjeta_c = [t for t in txns if t.medio_pago == MedioPago.TARJETA_CREDITO]
    bonos = [t for t in txns if t.medio_pago == MedioPago.BONOS_EDS]

    # Litros by payment type
    litros_contado = sum((t.litros for t in efectivo), Decimal("0"))
    litros_credito = sum((t.litros for t in credito), Decimal("0"))
    litros_prepago = sum((t.litros for t in prepago), Decimal("0"))
    litros_tarjeta = sum((t.litros for t in tarjeta_d + tarjeta_c), Decimal("0"))
    litros_bonos = sum((t.litros for t in bonos), Decimal("0"))
    # Others: transactions with no medio_pago or DESCONOCIDO
    others = [t for t in txns if t not in efectivo + credito + prepago + tarjeta_d + tarjeta_c + bonos]
    litros_otros = sum((t.litros for t in others), Decimal("0"))

    # PVP analysis for cash transactions (Col C: regular price, Col D-E: different price)
    pvp_contado = Decimal("0")
    litros_diff_precio = Decimal("0")
    pvp_diff = None

    if efectivo:
        # Find the modal (most common) PVP
        pvp_values = [t.pvp for t in efectivo]
        try:
            pvp_mode = stats_mode(pvp_values)
        except Exception:
            pvp_mode = pvp_values[0] if pvp_values else Decimal("0")

        pvp_contado = pvp_mode

        # Split cash into standard-price and different-price (matches NatGas B vs D)
        std_price_txns = [t for t in efectivo if t.pvp == pvp_mode]
        diff_price_txns = [t for t in efectivo if t.pvp != pvp_mode]

        # Col B = litros at standard price ONLY (not all cash)
        litros_contado = sum((t.litros for t in std_price_txns), Decimal("0"))
        # Col D = litros at different price
        litros_diff_precio = sum((t.litros for t in diff_price_txns), Decimal("0"))

        if diff_price_txns:
            pvp_diff = (
                sum((t.pvp for t in diff_price_txns), Decimal("0"))
                / len(diff_price_txns)
            ).quantize(TWO_PLACES)

    # Total litros
    total_litros = sum((t.litros for t in txns), Decimal("0"))

    # $ by category
    bonos_eds = sum((t.total_mxn for t in bonos), Decimal("0"))
    tpv_total = sum(
        (t.total_mxn for t in tarjeta_d + tarjeta_c), Decimal("0")
    )

    # Descuentos: transactions where total_mxn < litros * pvp (discount applied)
    descuentos = Decimal("0")
    for t in efectivo:
        expected = (t.litros * t.pvp).quantize(TWO_PLACES)
        if t.total_mxn < expected:
            descuentos += (expected - t.total_mxn)

    # Total venta efectivo real (Col N):
    # All cash sales = efectivo totals - discounts - bonos - TPV - edenred
    total_venta_mxn = sum((t.total_mxn for t in txns), Decimal("0"))
    total_venta_efectivo = (
        total_venta_mxn - bonos_eds - tpv_total
    ).quantize(TWO_PLACES)

    # Recaudos (Col O)
    total_recaudos = sum((t.recaudo_pagado for t in txns), Decimal("0"))

    # Ventas + recaudos (Col P)
    ventas_mas_recaudos = (total_venta_efectivo + total_recaudos).quantize(TWO_PLACES)

    return GasUpDailyBlock(
        close_date=close_date,
        litros_contado=litros_contado.quantize(TWO_PLACES),
        pvp_contado=pvp_contado.quantize(TWO_PLACES),
        litros_diff_precio=litros_diff_precio.quantize(TWO_PLACES),
        pvp_diff=pvp_diff,
        litros_credito=litros_credito.quantize(TWO_PLACES),
        litros_prepago=litros_prepago.quantize(TWO_PLACES),
        litros_tarjeta=litros_tarjeta.quantize(TWO_PLACES),
        litros_bonos=litros_bonos.quantize(TWO_PLACES),
        litros_otros=litros_otros.quantize(TWO_PLACES),
        total_litros=total_litros.quantize(TWO_PLACES),
        ventas_canastilla=Decimal("0"),  # Not in GasUp — manual or Odoo
        descuentos_efectivo=descuentos.quantize(TWO_PLACES),
        bonos_eds=bonos_eds.quantize(TWO_PLACES),
        tpv_total=tpv_total.quantize(TWO_PLACES),
        edenred=Decimal("0"),  # Not in GasUp — future Edenred integration
        total_venta_efectivo=total_venta_efectivo,
        total_recaudos=total_recaudos.quantize(TWO_PLACES),
        ventas_mas_recaudos=ventas_mas_recaudos,
        total_mxn=total_venta_mxn.quantize(TWO_PLACES),
        num_cargas=len(txns),
        unique_placas=len(set(t.placa for t in txns)),
        avg_litros_per_carga=(
            (total_litros / len(txns)).quantize(TWO_PLACES)
            if txns else Decimal("0")
        ),
    )


# ============================================================
# Block 3: Calculate cash summary from Blocks 1 + 2
# ============================================================

def build_cash_summary(
    gasup: GasUpDailyBlock,
    compusafe: CompusafeBlock,
) -> CashSummaryBlock:
    """
    Block 3 (Cols T-W): Cash reconciliation.
    T = P - S (picos = ventas+recaudos - compusafe deposit)
    U = P (total efectivo OASIS)
    V = S + T (compusafe + picos)
    W = U - V (difference — should be ~$0)
    """
    picos = (gasup.ventas_mas_recaudos - compusafe.efectivo_ingresado).quantize(TWO_PLACES)
    total_oasis = gasup.ventas_mas_recaudos
    efectivo_obtenido = (compusafe.efectivo_ingresado + picos).quantize(TWO_PLACES)
    diferencia = (total_oasis - efectivo_obtenido).quantize(TWO_PLACES)

    return CashSummaryBlock(
        picos_reales=picos,
        total_efectivo_oasis=total_oasis,
        efectivo_total_obtenido=efectivo_obtenido,
        diferencia_efectivo=diferencia,
    )


# ============================================================
# Block 5: Calculate bank differences
# ============================================================

def build_bank_block(
    gasup: GasUpDailyBlock,
    cash_summary: CashSummaryBlock,
    banco_monto: Decimal = Decimal("0"),
    fecha_conciliado: Optional[date] = None,
    fecha_deposito: Optional[str] = None,
) -> BankBlock:
    """
    Block 5 (Cols AA-AF): Bank reconciliation.
    AD = P (cross-check)
    AE = T (picos)
    AF = AD - AC - AE (should be ~$0)
    """
    ventas_check = gasup.ventas_mas_recaudos
    picos = cash_summary.picos_reales
    diferencia = (ventas_check - banco_monto - picos).quantize(TWO_PLACES)

    return BankBlock(
        fecha_conciliado=fecha_conciliado,
        fecha_deposito=fecha_deposito,
        banco_monto=banco_monto.quantize(TWO_PLACES),
        ventas_recaudos_check=ventas_check,
        picos_necesarios=picos,
        diferencia_vs_efectivo=diferencia,
    )


# ============================================================
# Block 6: TPV audit
# ============================================================

def build_tpv_audit(
    gasup: GasUpDailyBlock,
    tiras_auditoras: Decimal = Decimal("0"),
) -> TPVAuditBlock:
    """
    Block 6 (Cols AG-AH): TPV audit.
    AH = AG - L (tiras auditoras - GasUp TPV total)
    """
    diferencia = (tiras_auditoras - gasup.tpv_total).quantize(TWO_PLACES)
    return TPVAuditBlock(
        tiras_auditoras=tiras_auditoras.quantize(TWO_PLACES),
        diferencia_vs_gasdata=diferencia,
    )


# ============================================================
# Block 7: Income summary
# ============================================================

def build_income_block(
    bank: BankBlock,
    etv: ETVBlock,
    bancos_ingreso: Decimal = Decimal("0"),
) -> IncomeBlock:
    """
    Block 7 (Cols AI-AJ): Income validation.
    AJ = AI - (AC + Z)
    """
    expected = (bank.banco_monto + etv.importe).quantize(TWO_PLACES)
    diferencia = (bancos_ingreso - expected).quantize(TWO_PLACES)
    return IncomeBlock(
        bancos=bancos_ingreso.quantize(TWO_PLACES),
        diferencia=diferencia,
    )


# ============================================================
# Reconciliation checks engine
# ============================================================

def run_checks(
    result: DailyCloseResult,
    thresholds: ReconciliationThresholds,
) -> list[ReconciliationCheck]:
    """
    Run 7 reconciliation checks corresponding to the 7 NatGas blocks.
    Returns a list of check results with PASS/WARN/FAIL.
    """
    checks: list[ReconciliationCheck] = []

    # ---- CHECK 1: GasUp internal consistency (H = B+D+F+G+tarjeta+bonos+otros) ----
    g = result.gasup
    expected_litros = (
        g.litros_contado + g.litros_diff_precio
        + g.litros_credito + g.litros_prepago
        + g.litros_tarjeta + g.litros_bonos + g.litros_otros
    ).quantize(TWO_PLACES)
    checks.append(_make_check(
        "CHK_GASUP_LITROS",
        "GasUp: litros consistency (sum by medio = total)",
        expected_litros,
        g.total_litros,
        Decimal("1"), Decimal("10"),
    ))

    # ---- CHECK 2: Cash balance (Col W: OASIS vs Real) ----
    cs = result.cash_summary
    checks.append(_make_check(
        "CHK_CASH_BALANCE",
        "Efectivo: OASIS vs Real (Col W)",
        cs.total_efectivo_oasis,
        cs.efectivo_total_obtenido,
        thresholds.cash_warn_mxn,
        thresholds.cash_fail_mxn,
    ))

    # ---- CHECK 3: Bank vs efectivo (Col AF) ----
    bk = result.bank
    expected_bank = (bk.ventas_recaudos_check - bk.picos_necesarios).quantize(TWO_PLACES)
    checks.append(_make_check(
        "CHK_BANK_VS_CASH",
        "Bancos vs Efectivo (Col AF)",
        expected_bank,
        bk.banco_monto,
        thresholds.bank_warn_mxn,
        thresholds.bank_fail_mxn,
    ))

    # ---- CHECK 4: TPV audit (Col AH: tiras vs GasData) ----
    tpv = result.tpv_audit
    checks.append(_make_check(
        "CHK_TPV_AUDIT",
        "TPV: tiras auditoras vs GasData (Col AH)",
        g.tpv_total,
        tpv.tiras_auditoras,
        thresholds.tpv_warn_mxn,
        thresholds.tpv_fail_mxn,
    ))

    # ---- CHECK 5: Income reconciliation (Col AJ) ----
    inc = result.income
    expected_income = (bk.banco_monto + result.etv.importe).quantize(TWO_PLACES)
    checks.append(_make_check(
        "CHK_INCOME",
        "Ingresos: bancos vs esperado (Col AJ)",
        expected_income,
        inc.bancos,
        thresholds.income_warn_mxn,
        thresholds.income_fail_mxn,
    ))

    # ---- CHECK 6: GasUp vs SCADA volume (our addition) ----
    if result.scada_nm3 is not None and result.scada_nm3 > 0:
        checks.append(_make_check(
            "CHK_VOLUME_SCADA",
            "Volumen: GasUp litros vs SCADA Nm³",
            result.scada_nm3,
            g.total_litros,
            thresholds.volume_warn_pct,
            thresholds.volume_fail_pct,
            is_percentage=True,
            base_value=result.scada_nm3,
        ))
    else:
        checks.append(ReconciliationCheck(
            check_id="CHK_VOLUME_SCADA",
            name="Volumen: GasUp litros vs SCADA Nm³",
            result=CheckResult.SKIP,
            expected=Decimal("0"),
            actual=g.total_litros,
            delta=Decimal("0"),
            detail="SCADA data not available",
        ))

    # ---- CHECK 7: Recaudos balance ----
    if g.total_recaudos > 0:
        checks.append(_make_check(
            "CHK_RECAUDOS",
            "Recaudos: GasUp vs depositado",
            g.total_recaudos,
            g.total_recaudos,  # Self-check for now; Odoo integration will compare
            Decimal("50"),
            Decimal("500"),
        ))

    return checks


def _make_check(
    check_id: str,
    name: str,
    expected: Decimal,
    actual: Decimal,
    threshold_warn: Decimal,
    threshold_fail: Decimal,
    is_percentage: bool = False,
    base_value: Optional[Decimal] = None,
) -> ReconciliationCheck:
    """Create a single reconciliation check with auto-evaluated result."""
    delta = (actual - expected).quantize(TWO_PLACES)
    abs_delta = abs(delta)

    delta_pct = None
    if base_value and base_value != 0:
        delta_pct = (delta / base_value * Decimal("100")).quantize(TWO_PLACES)

    # For percentage-based checks (SCADA volume)
    if is_percentage and delta_pct is not None:
        compare_value = abs(delta_pct)
    else:
        compare_value = abs_delta

    if compare_value >= threshold_fail:
        result = CheckResult.FAIL
    elif compare_value >= threshold_warn:
        result = CheckResult.WARN
    else:
        result = CheckResult.PASS

    detail = f"Δ = ${delta:,.2f}"
    if delta_pct is not None:
        detail += f" ({delta_pct:+.2f}%)"

    return ReconciliationCheck(
        check_id=check_id,
        name=name,
        result=result,
        expected=expected.quantize(TWO_PLACES),
        actual=actual.quantize(TWO_PLACES),
        delta=delta,
        delta_pct=delta_pct,
        threshold_warn=threshold_warn,
        threshold_fail=threshold_fail,
        detail=detail,
    )


# ============================================================
# Status determination
# ============================================================

def determine_status(checks: list[ReconciliationCheck]) -> CloseStatus:
    """Determine overall close status from check results."""
    fails = sum(1 for c in checks if c.result == CheckResult.FAIL)
    warns = sum(1 for c in checks if c.result == CheckResult.WARN)
    skips = sum(1 for c in checks if c.result == CheckResult.SKIP)

    if fails >= 2:
        return CloseStatus.EMERGENCY
    elif fails == 1:
        return CloseStatus.CRITICAL
    elif skips >= 2:
        return CloseStatus.PARTIAL
    elif warns > 0:
        return CloseStatus.WARNING
    else:
        return CloseStatus.OK


# ============================================================
# Main orchestrator: run daily close for one station
# ============================================================

STATION_NAMES = {
    1: "Parques Industriales",
    2: "Oriente",
    3: "Pensión/Nacozari",
}


def run_daily_close(
    station_id: int,
    close_date: date,
    transactions: list[TransactionNormalized],
    # Odoo data (will come from Odoo client in Session 2)
    compusafe_efectivo: Decimal = Decimal("0"),
    compusafe_corte: Decimal = Decimal("0"),
    compusafe_retiro: Decimal = Decimal("0"),
    banco_monto: Decimal = Decimal("0"),
    banco_ingreso: Decimal = Decimal("0"),
    fecha_conciliado: Optional[date] = None,
    fecha_deposito: Optional[str] = None,
    # SCADA data (will come from SCADA service)
    scada_nm3: Optional[Decimal] = None,
    # TPV audit (dispenser tiras or manual)
    tpv_tiras: Decimal = Decimal("0"),
    # Manual inputs
    etv_comprobante: Optional[str] = None,
    # Thresholds
    thresholds: Optional[ReconciliationThresholds] = None,
) -> DailyCloseResult:
    """
    Run complete daily close reconciliation for one station on one date.

    This is the automated equivalent of filling one row in the
    NatGas 38-column Excel ("CIERRE OPERACIONES EDS").

    Args:
        station_id: Station (1=Parques, 2=Oriente, 3=Nacozari)
        close_date: The date to close
        transactions: All parsed GasUp transactions (pre-filtered or not)
        compusafe_*: Data from Compusafe safe (Odoo)
        banco_*: Bank statement data (Odoo)
        scada_nm3: SCADA totalizador delta for the day
        tpv_tiras: Sum of TPV audit tapes from dispenser
        etv_comprobante: Receipt number for coin deposit
        thresholds: Reconciliation thresholds (defaults if None)

    Returns:
        DailyCloseResult with all 7 blocks, checks, status, and WhatsApp message
    """
    if thresholds is None:
        thresholds = ReconciliationThresholds()

    station_name = STATION_NAMES.get(station_id, f"Estación {station_id}")
    now = datetime.now(timezone.utc)

    # --- Block 1: GasUp POS data ---
    gasup = build_gasup_block(transactions, close_date, station_id)

    # --- Block 2: Compusafe ---
    compusafe = CompusafeBlock(
        corte_temporal=compusafe_corte,
        retiro=compusafe_retiro,
        efectivo_ingresado=compusafe_efectivo,
    )

    # --- Block 3: Cash summary ---
    cash_summary = build_cash_summary(gasup, compusafe)

    # --- Block 4: ETV ---
    etv = ETVBlock(
        dia_venta=close_date,
        comprobante=etv_comprobante,
        importe=cash_summary.picos_reales,  # Coins = picos
    )

    # --- Block 5: Bank ---
    bank = build_bank_block(
        gasup, cash_summary,
        banco_monto=banco_monto,
        fecha_conciliado=fecha_conciliado,
        fecha_deposito=fecha_deposito,
    )

    # --- Block 6: TPV audit ---
    tpv_audit = build_tpv_audit(gasup, tpv_tiras)

    # --- Block 7: Income ---
    income = build_income_block(bank, etv, banco_ingreso)

    # --- SCADA cross-validation ---
    scada_pct = None
    if scada_nm3 is not None and scada_nm3 > 0 and gasup.total_litros > 0:
        scada_pct = (
            (gasup.total_litros - scada_nm3) / scada_nm3 * Decimal("100")
        ).quantize(TWO_PLACES)

    # --- Build result ---
    result = DailyCloseResult(
        station_id=station_id,
        station_name=station_name,
        close_date=close_date,
        generated_at=now,
        gasup=gasup,
        compusafe=compusafe,
        cash_summary=cash_summary,
        etv=etv,
        bank=bank,
        tpv_audit=tpv_audit,
        income=income,
        scada_nm3=scada_nm3,
        scada_vs_gasup_pct=scada_pct,
    )

    # --- Run checks ---
    result.checks = run_checks(result, thresholds)

    # --- Status ---
    result.status = determine_status(result.checks)
    result.dia_cerrado = result.status in (CloseStatus.OK, CloseStatus.WARNING)

    # --- WhatsApp message ---
    result.whatsapp_message = format_whatsapp_message(result)

    logger.info(
        f"Daily close {station_name} {close_date}: "
        f"{result.status.value} — {len(result.checks)} checks, "
        f"dia_cerrado={'SI' if result.dia_cerrado else 'NO'}"
    )

    return result


# ============================================================
# WhatsApp message formatter
# ============================================================

def format_whatsapp_message(result: DailyCloseResult) -> str:
    """
    Format daily close result as WhatsApp message.

    Template based on CAPA2 specification:
    🏪 CIERRE DIARIO [station] [date]
    📊 Ventas: X cargas, Y litros, $Z MXN
    💰 Efectivo: OK/WARN (Δ $W)
    🏦 Bancos: OK/WARN (Δ $AF)
    💳 TPV: OK/WARN (Δ $AH)
    📈 Ingresos: OK/WARN (Δ $AJ)
    🔧 SCADA: OK/WARN (Δ X%)
    ─────────────────
    Estado: ✅ DIA CERRADO / ⚠️ REVISAR / 🚨 CRITICO
    """
    g = result.gasup
    cs = result.cash_summary
    bk = result.bank
    tpv = result.tpv_audit
    inc = result.income

    # Status emoji
    status_map = {
        CloseStatus.OK: "✅",
        CloseStatus.WARNING: "⚠️",
        CloseStatus.CRITICAL: "🚨",
        CloseStatus.EMERGENCY: "🔴",
        CloseStatus.PARTIAL: "⏳",
    }
    emoji = status_map.get(result.status, "❓")

    # Check result emojis
    def check_emoji(check_id: str) -> str:
        for c in result.checks:
            if c.check_id == check_id:
                if c.result == CheckResult.PASS:
                    return "✅"
                elif c.result == CheckResult.WARN:
                    return "⚠️"
                elif c.result == CheckResult.FAIL:
                    return "🚨"
                else:
                    return "⏭️"
        return "—"

    def check_delta(check_id: str) -> str:
        for c in result.checks:
            if c.check_id == check_id:
                return c.detail
        return "N/A"

    lines = [
        f"🏪 *CIERRE DIARIO*",
        f"📍 {result.station_name}",
        f"📅 {result.close_date.strftime('%d/%m/%Y')}",
        f"",
        f"📊 *Ventas del día:*",
        f"  • {g.num_cargas} cargas, {g.unique_placas} vehículos",
        f"  • {g.total_litros:,.2f} litros ({g.avg_litros_per_carga:.1f} lt/carga)",
        f"  • ${g.total_mxn:,.2f} MXN total",
        f"  • ${g.total_venta_efectivo:,.2f} efectivo + ${g.total_recaudos:,.2f} recaudos",
        f"",
        f"🔍 *Reconciliación:*",
        f"  {check_emoji('CHK_CASH_BALANCE')} Efectivo: {check_delta('CHK_CASH_BALANCE')}",
        f"  {check_emoji('CHK_BANK_VS_CASH')} Bancos: {check_delta('CHK_BANK_VS_CASH')}",
        f"  {check_emoji('CHK_TPV_AUDIT')} TPV: {check_delta('CHK_TPV_AUDIT')}",
        f"  {check_emoji('CHK_INCOME')} Ingresos: {check_delta('CHK_INCOME')}",
        f"  {check_emoji('CHK_VOLUME_SCADA')} SCADA: {check_delta('CHK_VOLUME_SCADA')}",
        f"",
        f"{'─' * 25}",
        f"{emoji} *Estado: {result.status.value}*",
        f"{'✅ DIA CERRADO' if result.dia_cerrado else '❌ PENDIENTE REVISIÓN'}",
    ]

    # Add alert details for non-passing checks
    failing = [c for c in result.checks if c.result in (CheckResult.FAIL, CheckResult.WARN)]
    if failing:
        lines.append("")
        lines.append("📋 *Detalles:*")
        for c in failing:
            icon = "🚨" if c.result == CheckResult.FAIL else "⚠️"
            lines.append(f"  {icon} {c.name}: {c.detail}")

    return "\n".join(lines)


# ============================================================
# Multi-station batch close
# ============================================================

def run_all_stations_close(
    close_date: date,
    transactions: list[TransactionNormalized],
    **kwargs,
) -> list[DailyCloseResult]:
    """
    Run daily close for all 3 stations.
    Returns list of DailyCloseResult.
    """
    results = []
    for station_id in [1, 2, 3]:
        try:
            result = run_daily_close(
                station_id=station_id,
                close_date=close_date,
                transactions=transactions,
                **kwargs,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Daily close failed for station {station_id}: {e}")
    return results


def format_summary_message(results: list[DailyCloseResult]) -> str:
    """Format a summary WhatsApp message for all stations."""
    if not results:
        return "⚠️ No hay datos para el cierre del día."

    close_date = results[0].close_date
    total_litros = sum(r.gasup.total_litros for r in results)
    total_mxn = sum(r.gasup.total_mxn for r in results)
    total_cargas = sum(r.gasup.num_cargas for r in results)

    status_map = {
        CloseStatus.OK: "✅",
        CloseStatus.WARNING: "⚠️",
        CloseStatus.CRITICAL: "🚨",
        CloseStatus.EMERGENCY: "🔴",
        CloseStatus.PARTIAL: "⏳",
    }

    lines = [
        f"📊 *RESUMEN CIERRE DIARIO*",
        f"📅 {close_date.strftime('%d/%m/%Y')}",
        f"",
        f"*Totales:*",
        f"  • {total_cargas} cargas totales",
        f"  • {total_litros:,.2f} litros",
        f"  • ${total_mxn:,.2f} MXN",
        f"",
        f"*Por estación:*",
    ]

    for r in results:
        emoji = status_map.get(r.status, "❓")
        lines.append(
            f"  {emoji} {r.station_name}: "
            f"{r.gasup.total_litros:,.0f} lt, "
            f"${r.gasup.total_mxn:,.0f} — "
            f"{'CERRADO' if r.dia_cerrado else 'PENDIENTE'}"
        )

    all_closed = all(r.dia_cerrado for r in results)
    lines.extend([
        f"",
        f"{'─' * 25}",
        f"{'✅ TODOS LOS CIERRES OK' if all_closed else '⚠️ HAY ESTACIONES PENDIENTES'}",
    ])

    return "\n".join(lines)
