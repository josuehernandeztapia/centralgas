"""
Reconciliation models — Maps NatGas 38-column daily close to Central Gas agent.

NatGas "CIERRE OPERACIONES EDS" has 7 reconciliation blocks across 38 columns:
  Block 1 (A-P):  OASIS POS manual — date, litros by payment, totals, recaudos
  Block 2 (Q-S):  Compusafe cash machine — corte, retiro, efectivo ingresado
  Block 3 (T-W):  Cash summary — picos reales, OASIS total, efectivo obtenido, DIFERENCIAS
  Block 4 (X-Z):  ETV coins — día venta, comprobante, importe
  Block 5 (AA-AF): Finance/Banks — conciliación, depósitos, DIFERENCIAS VS EFECTIVO
  Block 6 (AG-AH): TPV audit — suma tiras auditoras, vs GasData
  Block 7 (AI-AJ): Income — bancos, DIFERENCIAS
  Col AL:          DIA CERRADO HO (SI/NO)

Our agent replaces all 7 blocks by pulling data from:
  - GasUp CSV parser (Capa 1) → Blocks 1, 3, 6
  - SCADA totalizador (Capa 2) → cross-validation of volumes
  - Odoo ERP (Capa 3) → Blocks 2, 5, 7
  - Physical cash (Block 4 ETV) stays manual but agent validates amounts
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ============================================================
# Enums
# ============================================================

class CloseStatus(str, Enum):
    """Daily close reconciliation status."""
    OK = "OK"                   # All checks pass (deltas < thresholds)
    WARNING = "WARNING"         # Some checks off by >$50 but <$500
    CRITICAL = "CRITICAL"       # Delta >$500 or >2% volume discrepancy
    EMERGENCY = "EMERGENCY"     # Delta >$5,000 or >5% — posible fraude
    PARTIAL = "PARTIAL"         # Missing data source (Odoo down, SCADA offline)


class CheckResult(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"              # Data source unavailable


# ============================================================
# Column mapping: NatGas Excel → Our data sources
# ============================================================

"""
COMPLETE 38-COLUMN MAPPING — NatGas "CIERRE OPERACIONES EDS" to Central Gas Agent

Col | NatGas Header                        | Our Source                          | Field/Calculation
----|--------------------------------------|-------------------------------------|------------------------------------------
 A  | FECHA DE CONCILIACIÓN DIARIA         | Input parameter                     | close_date
 B  | LITROS CONTADO                       | GasUp: SUM(litros) WHERE medio_pago=EFECTIVO | gasup_litros_contado
 C  | $ PV                                 | GasUp: AVG(pvp) WHERE medio_pago=EFECTIVO    | gasup_pvp_contado
 D  | LITROS CONTADO DIFERENTE PRECIO      | GasUp: SUM(litros) WHERE pvp != mode(pvp)    | gasup_litros_diff_precio
 E  | $ PV DIFERENTE CONTADO               | GasUp: pvp of those transactions              | gasup_pvp_diff
 F  | LITROS CRÉDITO                       | GasUp: SUM(litros) WHERE medio_pago=CREDITO  | gasup_litros_credito
 G  | LITROS PREPAGO                       | GasUp: SUM(litros) WHERE medio_pago=PREPAGO  | gasup_litros_prepago
 H  | TOTAL LITROS VENDIDOS                | GasUp: SUM(litros) = B + D + F + G           | gasup_total_litros
 I  | TOTAL VENTAS CANASTILLA              | Not in GasUp (convenience store)              | ventas_canastilla (manual/Odoo)
 J  | Descuentos Efectivo                  | GasUp: identify discount transactions         | descuentos_efectivo
 K  | BONOS EDS (Tarjetas de Puntos)       | GasUp: SUM WHERE medio_pago=BONOS_EDS        | bonos_eds
 L  | TPV                                  | GasUp: SUM WHERE medio_pago IN (TD, TC)       | tpv_total
 M  | MONEDERO ELECTRONICO EDENRED         | GasUp: SUM WHERE medio_pago contains EDENRED  | edenred
 N  | TOTAL DE VENTA EFECTIVO REAL         | Calculated: I - J - K - L - M + H*PVP         | total_venta_efectivo
 O  | TOTAL RECAUDOS                       | GasUp: SUM(recaudo_pagado)                    | total_recaudos
 P  | VENTAS EFECTIVO + RECAUDOS           | N + O                                          | ventas_mas_recaudos
 Q  | CORTE TEMPORAL                       | Odoo: Compusafe safe reading (POS Z-read)     | compusafe_corte
 R  | RETIRO                               | Odoo: Compusafe withdrawal                    | compusafe_retiro
 S  | Efectivo ingresado a Compusafe       | Odoo: Compusafe total deposited               | compusafe_efectivo
 T  | PICOS REALES (conteo)                | P - S (loose change not deposited)            | picos_reales
 U  | TOTAL EFECTIVO (OASIS)               | = P (cross-check)                              | total_efectivo_oasis
 V  | EFECTIVO TOTAL OBTENIDO              | S + T (Compusafe + coins)                      | efectivo_total_obtenido
 W  | Diferencias EFECTIVO (OASIS vs Real) | U - V (should be ~$0)                          | diferencia_efectivo
 X  | DIA DE VENTA                         | = A (date reference for ETV)                   | etv_dia_venta
 Y  | COMPROBANTE                          | ETV deposit receipt number (manual)            | etv_comprobante
 Z  | IMPORTE                              | = T (coins sent to ETV)                        | etv_importe
AA  | Conciliado en bancos (fecha)         | Odoo: bank reconciliation date                | banco_fecha_conciliado
AB  | Fecha depósito bancos                | Odoo: deposit dates (can be split)            | banco_fecha_deposito
AC  | Bancos ($)                           | Odoo: bank statement amount                   | banco_monto
AD  | Ventas Efectivo + Recaudos2          | = P (cross-check against bank)                | ventas_recaudos_check
AE  | Picos necesarios                     | = T (coins that explain the gap)              | picos_necesarios
AF  | DIFERENCIAS VS EFECTIVO              | AD - AC - AE (should be ~$0)                  | diferencia_vs_efectivo
AG  | Suma de las 3 tiras auditoras        | SCADA/Dispenser: totalizer delta              | tpv_tiras_auditoras
AH  | Vs GD                               | AG - L (tiras vs GasData TPV)                 | tpv_diferencia_gd
AI  | BANCOS                               | Odoo: total bank income for date              | ingresos_bancos
AJ  | DIFERENCIAS                          | AI - (AC + Z) or AI - expected                | diferencia_ingresos
--  | (col AK not used)                    | —                                              | —
AL  | DIA CERRADO HO                       | All checks pass → "SI"                        | dia_cerrado
"""


# ============================================================
# Block 1: OASIS/GasUp POS data (Cols A-P)
# ============================================================

class GasUpDailyBlock(BaseModel):
    """Block 1: Data extracted from GasUp parser for a single station+date.
    Maps to NatGas columns A through P."""

    close_date: date                                          # Col A
    litros_contado: Decimal = Decimal("0")                    # Col B
    pvp_contado: Decimal = Decimal("0")                       # Col C
    litros_diff_precio: Decimal = Decimal("0")                # Col D
    pvp_diff: Optional[Decimal] = None                        # Col E
    litros_credito: Decimal = Decimal("0")                    # Col F
    litros_prepago: Decimal = Decimal("0")                    # Col G
    total_litros: Decimal = Decimal("0")                      # Col H
    ventas_canastilla: Decimal = Decimal("0")                 # Col I (manual/Odoo)
    descuentos_efectivo: Decimal = Decimal("0")               # Col J
    bonos_eds: Decimal = Decimal("0")                         # Col K
    tpv_total: Decimal = Decimal("0")                         # Col L
    edenred: Decimal = Decimal("0")                           # Col M
    total_venta_efectivo: Decimal = Decimal("0")              # Col N
    total_recaudos: Decimal = Decimal("0")                    # Col O
    ventas_mas_recaudos: Decimal = Decimal("0")               # Col P

    # Litros from other payment methods (tarjeta, bonos, etc.)
    # NatGas H = B + D + F + G, but our GasUp data has more medio_pago types
    litros_tarjeta: Decimal = Decimal("0")                    # TD + TC litros
    litros_bonos: Decimal = Decimal("0")                      # Bonos EDS litros
    litros_otros: Decimal = Decimal("0")                      # Unknown/other

    # Additional details not in NatGas but useful for us
    total_mxn: Decimal = Decimal("0")                         # SUM(total_mxn) all txns
    num_cargas: int = 0
    unique_placas: int = 0
    avg_litros_per_carga: Decimal = Decimal("0")


# ============================================================
# Block 2: Compusafe / Cash machine (Cols Q-S)
# ============================================================

class CompusafeBlock(BaseModel):
    """Block 2: Compusafe safe machine data — from Odoo or manual entry.
    Maps to NatGas columns Q through S."""

    corte_temporal: Decimal = Decimal("0")                    # Col Q (cumulative Z-read)
    retiro: Decimal = Decimal("0")                            # Col R (cash withdrawal)
    efectivo_ingresado: Decimal = Decimal("0")                # Col S (cash deposited)


# ============================================================
# Block 3: Cash summary (Cols T-W) — CALCULATED
# ============================================================

class CashSummaryBlock(BaseModel):
    """Block 3: Cash reconciliation summary.
    Maps to NatGas columns T through W. All derived/calculated."""

    picos_reales: Decimal = Decimal("0")                      # Col T: P - S
    total_efectivo_oasis: Decimal = Decimal("0")              # Col U: = P
    efectivo_total_obtenido: Decimal = Decimal("0")           # Col V: S + T
    diferencia_efectivo: Decimal = Decimal("0")               # Col W: U - V


# ============================================================
# Block 4: ETV coins deposit (Cols X-Z) — mostly manual
# ============================================================

class ETVBlock(BaseModel):
    """Block 4: ETV coin deposit tracking.
    Maps to NatGas columns X through Z."""

    dia_venta: Optional[date] = None                          # Col X
    comprobante: Optional[str] = None                         # Col Y (receipt number)
    importe: Decimal = Decimal("0")                           # Col Z: = T (coins)


# ============================================================
# Block 5: Finance/Banks (Cols AA-AF)
# ============================================================

class BankBlock(BaseModel):
    """Block 5: Bank reconciliation data — from Odoo bank statements.
    Maps to NatGas columns AA through AF."""

    fecha_conciliado: Optional[date] = None                   # Col AA
    fecha_deposito: Optional[str] = None                      # Col AB (can be "date1 & date2")
    banco_monto: Decimal = Decimal("0")                       # Col AC
    ventas_recaudos_check: Decimal = Decimal("0")             # Col AD: = P
    picos_necesarios: Decimal = Decimal("0")                  # Col AE: = T
    diferencia_vs_efectivo: Decimal = Decimal("0")            # Col AF: AD - AC - AE


# ============================================================
# Block 6: TPV audit tapes (Cols AG-AH)
# ============================================================

class TPVAuditBlock(BaseModel):
    """Block 6: TPV (card terminal) audit from dispenser tapes.
    Maps to NatGas columns AG and AH."""

    tiras_auditoras: Decimal = Decimal("0")                   # Col AG
    diferencia_vs_gasdata: Decimal = Decimal("0")             # Col AH: AG - GasUp.tpv_total


# ============================================================
# Block 7: Income summary (Cols AI-AJ)
# ============================================================

class IncomeBlock(BaseModel):
    """Block 7: Total income validation.
    Maps to NatGas columns AI and AJ."""

    bancos: Decimal = Decimal("0")                            # Col AI
    diferencia: Decimal = Decimal("0")                        # Col AJ: AI - expected


# ============================================================
# Individual reconciliation check
# ============================================================

class ReconciliationCheck(BaseModel):
    """Result of a single reconciliation check."""
    check_id: str                       # e.g. "CHK_CASH_BALANCE"
    name: str                           # Human-readable name
    result: CheckResult
    expected: Decimal
    actual: Decimal
    delta: Decimal                      # actual - expected
    delta_pct: Optional[Decimal] = None # delta / expected * 100
    threshold_warn: Decimal = Decimal("50")    # $ warning threshold
    threshold_fail: Decimal = Decimal("500")   # $ fail threshold
    detail: str = ""


# ============================================================
# Complete daily close result
# ============================================================

class DailyCloseResult(BaseModel):
    """Complete daily close reconciliation for one station on one date.
    This is the automated equivalent of the NatGas 38-column Excel row."""

    # Identity
    station_id: int
    station_name: str
    close_date: date
    generated_at: datetime

    # 7 blocks (mapped to NatGas columns)
    gasup: GasUpDailyBlock                    # Cols A-P
    compusafe: CompusafeBlock                 # Cols Q-S
    cash_summary: CashSummaryBlock            # Cols T-W
    etv: ETVBlock                             # Cols X-Z
    bank: BankBlock                           # Cols AA-AF
    tpv_audit: TPVAuditBlock                  # Cols AG-AH
    income: IncomeBlock                       # Cols AI-AJ

    # SCADA cross-validation (our addition — NatGas doesn't have this)
    scada_nm3: Optional[Decimal] = None       # Delta totalizador SCADA
    scada_vs_gasup_pct: Optional[Decimal] = None  # (gasup_litros - scada_nm3) / gasup * 100

    # Reconciliation checks (5-7 checks)
    checks: list[ReconciliationCheck] = Field(default_factory=list)

    # Overall status
    status: CloseStatus = CloseStatus.PARTIAL
    dia_cerrado: bool = False                 # Col AL: all checks pass

    # Formatted output
    whatsapp_message: str = ""                # Pre-formatted WhatsApp message

    def worst_check(self) -> CheckResult:
        """Return the worst check result."""
        if any(c.result == CheckResult.FAIL for c in self.checks):
            return CheckResult.FAIL
        if any(c.result == CheckResult.WARN for c in self.checks):
            return CheckResult.WARN
        if any(c.result == CheckResult.SKIP for c in self.checks):
            return CheckResult.SKIP
        return CheckResult.PASS


# ============================================================
# Thresholds (calibrated from NatGas data analysis)
# ============================================================

class ReconciliationThresholds(BaseModel):
    """Configurable thresholds per check.
    Defaults calibrated from NatGas daily close data:
      - Col W (cash diff): typically ±$0.55 MXN (centavos rounding)
      - Col AF (bank diff): same as W (=P-AC-AE)
      - Col AH (TPV diff): typically $0-$38 (tira rounding)
      - Col AJ (income diff): ±$1,441 MXN (multi-day deposits)
    """

    # Cash balance (Col W: OASIS vs real)
    cash_warn_mxn: Decimal = Decimal("10")       # >$10 warn
    cash_fail_mxn: Decimal = Decimal("100")      # >$100 fail (normally <$1)

    # Bank reconciliation (Col AF: vs efectivo)
    bank_warn_mxn: Decimal = Decimal("50")
    bank_fail_mxn: Decimal = Decimal("500")

    # TPV audit (Col AH: tiras vs GasData)
    tpv_warn_mxn: Decimal = Decimal("50")
    tpv_fail_mxn: Decimal = Decimal("500")

    # Income (Col AJ: bancos vs expected)
    income_warn_mxn: Decimal = Decimal("200")    # Multi-day deposits are normal
    income_fail_mxn: Decimal = Decimal("2000")

    # Volume: GasUp vs SCADA (our addition)
    volume_warn_pct: Decimal = Decimal("2")      # >2% warn
    volume_fail_pct: Decimal = Decimal("5")      # >5% fail — possible leak or meter drift

    # GasUp litros vs Odoo journal (A vs D check)
    gasup_vs_odoo_warn_mxn: Decimal = Decimal("100")
    gasup_vs_odoo_fail_mxn: Decimal = Decimal("1000")
