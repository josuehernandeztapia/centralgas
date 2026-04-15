"""
Odoo 17 XML-RPC Client — HU-2.1, HU-2.2

Handles:
  - Authentication + session management
  - Journal entry creation (asientos contables automáticos)
  - Client sync (331 placas → res.partner)
  - Compusafe / Bank data fetch for daily close blocks 2, 5, 7
  - Offline queue + replay when Odoo is down

Connection:
  - XML-RPC to Odoo 17: /xmlrpc/2/common (auth) + /xmlrpc/2/object (CRUD)
  - Retry 3x with exponential backoff
  - Queue in PostgreSQL if Odoo down >15 min
"""

from __future__ import annotations

import logging
import time
import xmlrpc.client
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from collections import deque

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

@dataclass
class OdooConfig:
    """Odoo connection configuration."""
    url: str = "http://localhost:8069"
    db: str = "central_gas"
    username: str = "admin"
    password: str = "admin"
    max_retries: int = 3
    retry_base_delay: float = 1.0       # seconds, doubles each retry
    timeout: int = 30                    # seconds per XML-RPC call
    queue_max_size: int = 10000          # max offline queue entries


# ============================================================
# Account mapping — Central Gas chart of accounts
# ============================================================

class CuentaContable(str, Enum):
    """Plan de cuentas Central Gas — mapped to Odoo account codes."""
    # Activos
    BANCOS = "102.01.001"               # Bancos nacionales
    CAJA = "102.01.002"                 # Caja chica / Compusafe
    CXC_CLIENTES = "105.01.001"         # Cuentas por cobrar clientes
    PREPAGO_CLIENTES = "106.01.001"     # Anticipos de clientes (prepago)

    # Ingresos
    INGRESO_VENTA_GNC = "401.01.001"    # Ingresos por venta de GNC
    INGRESO_RECAUDOS = "401.02.001"     # Ingresos por recaudos

    # Impuestos
    IVA_TRASLADADO = "216.01.001"       # IVA trasladado 16%
    IVA_COBRADO = "216.01.002"          # IVA efectivamente cobrado

    # Gastos (for reconciliation adjustments)
    COMISIONES_BANCARIAS = "601.01.001"  # Comisiones bancarias
    DIFERENCIAS_REDONDEO = "702.01.001"  # Diferencias por redondeo


# Mapping: MedioPago → cuenta contable de débito
MEDIO_PAGO_CUENTA = {
    "EFECTIVO": CuentaContable.CAJA,
    "TARJETA_DEBITO": CuentaContable.BANCOS,
    "TARJETA_CREDITO": CuentaContable.BANCOS,
    "CREDITO": CuentaContable.CXC_CLIENTES,
    "PREPAGO": CuentaContable.PREPAGO_CLIENTES,
    "BONOS_EDS": CuentaContable.CAJA,
    "DESCONOCIDO": CuentaContable.CAJA,
}


# ============================================================
# Offline queue entry
# ============================================================

class QueueAction(str, Enum):
    CREATE_MOVE = "create_move"
    CREATE_PARTNER = "create_partner"
    UPDATE_PARTNER = "update_partner"
    RECONCILE = "reconcile"


@dataclass
class QueueEntry:
    """Entry in the offline queue when Odoo is unreachable."""
    action: QueueAction
    payload: dict
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    attempts: int = 0
    last_error: Optional[str] = None


# ============================================================
# Odoo Client
# ============================================================

class OdooClient:
    """
    XML-RPC client for Odoo 17.

    Usage:
        client = OdooClient(OdooConfig(url="http://odoo:8069", db="central_gas"))
        client.authenticate()

        # Create journal entry from transaction
        move_id = client.create_journal_entry(transaction)

        # Sync client to Odoo
        partner_id = client.sync_client(placa="A12345A", nombre="Juan Pérez", ...)

        # Fetch Compusafe data for daily close
        compusafe = client.get_compusafe_data(station_id=3, close_date=date(2026, 1, 15))
    """

    def __init__(self, config: Optional[OdooConfig] = None):
        self.config = config or OdooConfig()
        self.uid: Optional[int] = None
        self._common: Optional[xmlrpc.client.ServerProxy] = None
        self._object: Optional[xmlrpc.client.ServerProxy] = None
        self._connected = False
        self._offline_queue: deque[QueueEntry] = deque(maxlen=self.config.queue_max_size)
        self._account_cache: dict[str, int] = {}  # code → account_id
        self._partner_cache: dict[str, int] = {}   # placa → partner_id
        self._journal_cache: dict[str, int] = {}   # code → journal_id

    # ---- Connection ----

    def authenticate(self) -> int:
        """Authenticate with Odoo and return uid."""
        try:
            self._common = xmlrpc.client.ServerProxy(
                f"{self.config.url}/xmlrpc/2/common",
                allow_none=True,
            )
            self._object = xmlrpc.client.ServerProxy(
                f"{self.config.url}/xmlrpc/2/object",
                allow_none=True,
            )

            self.uid = self._common.authenticate(
                self.config.db,
                self.config.username,
                self.config.password,
                {},
            )

            if not self.uid:
                raise ConnectionError("Odoo authentication failed — check credentials")

            self._connected = True
            logger.info(f"Odoo authenticated: uid={self.uid} @ {self.config.url}/{self.config.db}")
            return self.uid

        except Exception as e:
            self._connected = False
            logger.error(f"Odoo auth failed: {e}")
            raise

    @property
    def is_connected(self) -> bool:
        return self._connected and self.uid is not None

    def _execute(self, model: str, method: str, *args, **kwargs) -> Any:
        """Execute Odoo XML-RPC call with retry logic."""
        if not self.is_connected:
            raise ConnectionError("Not authenticated — call authenticate() first")

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                result = self._object.execute_kw(
                    self.config.db,
                    self.uid,
                    self.config.password,
                    model,
                    method,
                    args,
                    kwargs if kwargs else {},
                )
                return result

            except (ConnectionError, xmlrpc.client.Fault, OSError, TimeoutError) as e:
                last_error = e
                delay = self.config.retry_base_delay * (2 ** attempt)
                logger.warning(
                    f"Odoo call failed (attempt {attempt + 1}/{self.config.max_retries}): "
                    f"{model}.{method} — {e}. Retrying in {delay}s..."
                )
                if attempt < self.config.max_retries - 1:
                    time.sleep(delay)

        # All retries exhausted
        self._connected = False
        raise ConnectionError(
            f"Odoo unreachable after {self.config.max_retries} retries: {last_error}"
        )

    def _search(self, model: str, domain: list, **kwargs) -> list[int]:
        return self._execute(model, "search", domain, **kwargs)

    def _read(self, model: str, ids: list[int], fields: list[str]) -> list[dict]:
        return self._execute(model, "read", ids, {"fields": fields})

    def _search_read(self, model: str, domain: list, fields: list[str], **kwargs) -> list[dict]:
        return self._execute(model, "search_read", domain, {"fields": fields, **kwargs})

    def _create(self, model: str, vals: dict) -> int:
        return self._execute(model, "create", [vals])

    def _write(self, model: str, ids: list[int], vals: dict) -> bool:
        return self._execute(model, "write", ids, vals)

    # ---- Account / Journal resolution ----

    def _resolve_account(self, code: str) -> int:
        """Find account.account id by code, with caching."""
        if code in self._account_cache:
            return self._account_cache[code]

        accounts = self._search_read(
            "account.account",
            [("code", "=", code)],
            ["id"],
            limit=1,
        )
        if not accounts:
            raise ValueError(f"Account not found in Odoo: {code}")

        account_id = accounts[0]["id"]
        self._account_cache[code] = account_id
        return account_id

    def _resolve_journal(self, code: str = "MISC") -> int:
        """Find account.journal id by code."""
        if code in self._journal_cache:
            return self._journal_cache[code]

        journals = self._search_read(
            "account.journal",
            [("code", "=", code)],
            ["id"],
            limit=1,
        )
        if not journals:
            # Fallback: find any miscellaneous journal
            journals = self._search_read(
                "account.journal",
                [("type", "=", "general")],
                ["id"],
                limit=1,
            )
        if not journals:
            raise ValueError(f"Journal not found: {code}")

        journal_id = journals[0]["id"]
        self._journal_cache[code] = journal_id
        return journal_id

    # ============================================================
    # HU-2.1: Journal Entry Creation (Asientos Contables)
    # ============================================================

    def create_journal_entry(
        self,
        station_id: int,
        close_date: date,
        medio_pago: str,
        total_mxn: Decimal,
        ingreso_neto: Decimal,
        iva: Decimal,
        placa: str,
        recaudo: Decimal = Decimal("0"),
        ref: str = "",
    ) -> Optional[int]:
        """
        Create a journal entry (account.move) in Odoo for a transaction.

        Debit/Credit structure:
          DEBIT  Bancos/Caja/CxC  →  total_mxn  (depends on medio_pago)
          CREDIT Ingresos GNC     →  ingreso_neto
          CREDIT IVA Trasladado   →  iva
          DEBIT  Bancos           →  recaudo  (if recaudo > 0)
          CREDIT Ingreso Recaudos →  recaudo

        Returns: Odoo move_id or None if queued offline.
        """
        cuenta_debito = MEDIO_PAGO_CUENTA.get(medio_pago, CuentaContable.CAJA)

        move_vals = {
            "move_type": "entry",
            "journal_id": None,  # Resolved below
            "date": str(close_date),
            "ref": ref or f"GNC {placa} {close_date} {medio_pago}",
            "line_ids": [],
        }

        # Build journal items
        lines = []

        # Debit: payment method account
        lines.append((0, 0, {
            "account_id": None,  # Resolved to cuenta_debito
            "name": f"Venta GNC {placa} — {medio_pago}",
            "debit": float(total_mxn),
            "credit": 0.0,
        }))

        # Credit: Ingreso venta GNC (sin IVA)
        lines.append((0, 0, {
            "account_id": None,  # Resolved to INGRESO_VENTA_GNC
            "name": f"Ingreso GNC {placa}",
            "debit": 0.0,
            "credit": float(ingreso_neto),
        }))

        # Credit: IVA Trasladado
        lines.append((0, 0, {
            "account_id": None,  # Resolved to IVA_TRASLADADO
            "name": f"IVA 16% GNC {placa}",
            "debit": 0.0,
            "credit": float(iva),
        }))

        # Recaudo (if any)
        if recaudo > 0:
            lines.append((0, 0, {
                "account_id": None,  # BANCOS
                "name": f"Recaudo {placa}",
                "debit": float(recaudo),
                "credit": 0.0,
            }))
            lines.append((0, 0, {
                "account_id": None,  # INGRESO_RECAUDOS
                "name": f"Ingreso recaudo {placa}",
                "debit": 0.0,
                "credit": float(recaudo),
            }))

        # Try to create in Odoo, queue if offline
        try:
            # Resolve account IDs
            journal_id = self._resolve_journal("MISC")
            lines[0][2]["account_id"] = self._resolve_account(cuenta_debito.value)
            lines[1][2]["account_id"] = self._resolve_account(CuentaContable.INGRESO_VENTA_GNC.value)
            lines[2][2]["account_id"] = self._resolve_account(CuentaContable.IVA_TRASLADADO.value)
            if recaudo > 0:
                lines[3][2]["account_id"] = self._resolve_account(CuentaContable.BANCOS.value)
                lines[4][2]["account_id"] = self._resolve_account(CuentaContable.INGRESO_RECAUDOS.value)

            move_vals["journal_id"] = journal_id
            move_vals["line_ids"] = lines

            move_id = self._create("account.move", move_vals)
            logger.info(f"Created journal entry {move_id}: {placa} ${total_mxn} {medio_pago}")
            return move_id

        except ConnectionError:
            # Queue for later
            self._enqueue(QueueAction.CREATE_MOVE, {
                "station_id": station_id,
                "close_date": str(close_date),
                "medio_pago": medio_pago,
                "total_mxn": str(total_mxn),
                "ingreso_neto": str(ingreso_neto),
                "iva": str(iva),
                "placa": placa,
                "recaudo": str(recaudo),
                "ref": ref,
            })
            return None

    def create_daily_batch_entry(
        self,
        station_id: int,
        close_date: date,
        totals_by_medio: dict[str, dict],
        total_recaudos: Decimal = Decimal("0"),
    ) -> Optional[int]:
        """
        Create a SINGLE journal entry for the entire day's sales of a station.
        More efficient than one-per-transaction.

        totals_by_medio: {
            "EFECTIVO": {"total_mxn": Decimal, "ingreso_neto": Decimal, "iva": Decimal, "count": int},
            "TARJETA_DEBITO": {...},
            ...
        }
        """
        lines = []

        for medio_pago, totals in totals_by_medio.items():
            total = Decimal(str(totals["total_mxn"]))
            neto = Decimal(str(totals["ingreso_neto"]))
            iva = Decimal(str(totals["iva"]))
            count = totals.get("count", 0)
            cuenta = MEDIO_PAGO_CUENTA.get(medio_pago, CuentaContable.CAJA)

            if total <= 0:
                continue

            # Debit: payment method
            lines.append((0, 0, {
                "account_id": None,
                "name": f"{medio_pago} ({count} cargas) — {close_date}",
                "debit": float(total),
                "credit": 0.0,
                "_account_code": cuenta.value,
            }))

            # Credit: Ingreso
            lines.append((0, 0, {
                "account_id": None,
                "name": f"Ingreso GNC {medio_pago} — {close_date}",
                "debit": 0.0,
                "credit": float(neto),
                "_account_code": CuentaContable.INGRESO_VENTA_GNC.value,
            }))

            # Credit: IVA
            lines.append((0, 0, {
                "account_id": None,
                "name": f"IVA 16% {medio_pago} — {close_date}",
                "debit": 0.0,
                "credit": float(iva),
                "_account_code": CuentaContable.IVA_TRASLADADO.value,
            }))

        # Recaudos
        if total_recaudos > 0:
            lines.append((0, 0, {
                "account_id": None,
                "name": f"Recaudos del día — {close_date}",
                "debit": float(total_recaudos),
                "credit": 0.0,
                "_account_code": CuentaContable.BANCOS.value,
            }))
            lines.append((0, 0, {
                "account_id": None,
                "name": f"Ingreso recaudos — {close_date}",
                "debit": 0.0,
                "credit": float(total_recaudos),
                "_account_code": CuentaContable.INGRESO_RECAUDOS.value,
            }))

        if not lines:
            logger.warning(f"No lines to post for station {station_id} on {close_date}")
            return None

        try:
            journal_id = self._resolve_journal("MISC")

            # Resolve all account IDs
            for line_tuple in lines:
                code = line_tuple[2].pop("_account_code")
                line_tuple[2]["account_id"] = self._resolve_account(code)

            from app.services.reconciliation import STATION_NAMES
            station_name = STATION_NAMES.get(station_id, f"Est. {station_id}")

            move_vals = {
                "move_type": "entry",
                "journal_id": journal_id,
                "date": str(close_date),
                "ref": f"Cierre diario {station_name} — {close_date}",
                "line_ids": lines,
            }

            move_id = self._create("account.move", move_vals)
            logger.info(
                f"Created daily batch entry {move_id}: "
                f"{station_name} {close_date}, {len(lines)} lines"
            )
            return move_id

        except ConnectionError:
            self._enqueue(QueueAction.CREATE_MOVE, {
                "type": "daily_batch",
                "station_id": station_id,
                "close_date": str(close_date),
                "totals_by_medio": {
                    k: {kk: str(vv) for kk, vv in v.items()}
                    for k, v in totals_by_medio.items()
                },
                "total_recaudos": str(total_recaudos),
            })
            return None

    # ============================================================
    # HU-2.2: Client Sync (placas → res.partner)
    # ============================================================

    def sync_client(
        self,
        placa: str,
        nombre: Optional[str] = None,
        telefono: Optional[str] = None,
        rfc: Optional[str] = None,
        segmento: str = "VAGONETA",
        gasup_id: Optional[int] = None,
        consumo_prom_lt: Optional[float] = None,
        tendencia: Optional[str] = None,
        is_prospecto: bool = False,
    ) -> Optional[int]:
        """
        Sync a single client to Odoo as res.partner.
        Creates if new, updates if existing.

        Returns: Odoo partner_id or None if queued.
        """
        try:
            # Check if partner exists by placa (stored in ref field)
            existing = self._search_read(
                "res.partner",
                [("ref", "=", placa)],
                ["id", "name"],
                limit=1,
            )

            vals = {
                "ref": placa,
                "name": nombre or f"Vagoneta {placa}",
                "phone": telefono,
                "vat": rfc,
                "customer_rank": 1,
                "comment": f"Segmento: {segmento}. GasUp ID: {gasup_id}. "
                           f"Tendencia: {tendencia}. Consumo: {consumo_prom_lt} lt/mes.",
            }

            # Tags
            tag_names = [f"GNC_{segmento}"]
            if is_prospecto:
                tag_names.append("PROSPECTO_DIA1")
            if tendencia:
                tag_names.append(f"TEND_{tendencia}")

            # Remove None values
            vals = {k: v for k, v in vals.items() if v is not None}

            if existing:
                partner_id = existing[0]["id"]
                self._write("res.partner", [partner_id], vals)
                logger.info(f"Updated partner {partner_id}: {placa}")
            else:
                partner_id = self._create("res.partner", vals)
                logger.info(f"Created partner {partner_id}: {placa} — {nombre}")

            self._partner_cache[placa] = partner_id
            return partner_id

        except ConnectionError:
            self._enqueue(QueueAction.CREATE_PARTNER, {
                "placa": placa, "nombre": nombre, "telefono": telefono,
                "rfc": rfc, "segmento": segmento, "gasup_id": gasup_id,
                "consumo_prom_lt": consumo_prom_lt, "tendencia": tendencia,
                "is_prospecto": is_prospecto,
            })
            return None

    def sync_all_clients(self, clients: list[dict]) -> dict:
        """
        Batch sync all clients from the Central Gas client database.

        clients: list of dicts with keys matching sync_client params.
        Returns: {"created": int, "updated": int, "queued": int, "errors": int}
        """
        stats = {"created": 0, "updated": 0, "queued": 0, "errors": 0}

        for client_data in clients:
            try:
                placa = client_data["placa"]
                existing = self._search_read(
                    "res.partner",
                    [("ref", "=", placa)],
                    ["id"],
                    limit=1,
                )

                result = self.sync_client(**client_data)
                if result is None:
                    stats["queued"] += 1
                elif existing:
                    stats["updated"] += 1
                else:
                    stats["created"] += 1

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error syncing {client_data.get('placa', '?')}: {e}")

        logger.info(
            f"Client sync complete: {stats['created']} created, "
            f"{stats['updated']} updated, {stats['queued']} queued, "
            f"{stats['errors']} errors"
        )
        return stats

    def get_partner_id(self, placa: str) -> Optional[int]:
        """Resolve placa to Odoo partner_id, with cache."""
        if placa in self._partner_cache:
            return self._partner_cache[placa]

        try:
            partners = self._search_read(
                "res.partner",
                [("ref", "=", placa)],
                ["id"],
                limit=1,
            )
            if partners:
                self._partner_cache[placa] = partners[0]["id"]
                return partners[0]["id"]
        except ConnectionError:
            pass
        return None

    # ============================================================
    # Daily Close data fetchers (Blocks 2, 5, 7)
    # ============================================================

    def get_compusafe_data(
        self, station_id: int, close_date: date
    ) -> dict:
        """
        Fetch Compusafe safe data from Odoo for daily close Block 2.

        Looks for a journal entry tagged with the Compusafe journal
        for the given station and date.

        Returns: {"corte_temporal": Decimal, "retiro": Decimal, "efectivo_ingresado": Decimal}
        """
        try:
            # Search for Compusafe-tagged entries
            entries = self._search_read(
                "account.move",
                [
                    ("date", "=", str(close_date)),
                    ("ref", "like", f"Compusafe%station_{station_id}"),
                    ("state", "=", "posted"),
                ],
                ["id", "amount_total", "ref"],
            )

            if not entries:
                # Try alternative: search by journal code
                entries = self._search_read(
                    "account.move",
                    [
                        ("date", "=", str(close_date)),
                        ("journal_id.code", "=", "COMP"),
                    ],
                    ["id", "amount_total", "ref"],
                )

            if entries:
                total = sum(e.get("amount_total", 0) for e in entries)
                return {
                    "corte_temporal": Decimal("0"),  # Updated from Compusafe Z-read
                    "retiro": Decimal(str(total)),
                    "efectivo_ingresado": Decimal(str(total)),
                }

        except ConnectionError:
            logger.warning(f"Cannot fetch Compusafe data — Odoo offline")

        return {
            "corte_temporal": Decimal("0"),
            "retiro": Decimal("0"),
            "efectivo_ingresado": Decimal("0"),
        }

    def get_bank_data(
        self, station_id: int, close_date: date
    ) -> dict:
        """
        Fetch bank statement data from Odoo for daily close Block 5.

        Returns: {
            "banco_monto": Decimal,
            "fecha_conciliado": Optional[date],
            "fecha_deposito": Optional[str],
            "banco_ingreso": Decimal,
        }
        """
        try:
            # Search bank statement lines for the date
            lines = self._search_read(
                "account.bank.statement.line",
                [
                    ("date", "=", str(close_date)),
                    ("journal_id.type", "=", "bank"),
                ],
                ["id", "amount", "date", "payment_ref", "is_reconciled"],
            )

            if lines:
                total = sum(l.get("amount", 0) for l in lines)
                reconciled_dates = [
                    l["date"] for l in lines
                    if l.get("is_reconciled")
                ]
                deposit_dates = list(set(l["date"] for l in lines))

                return {
                    "banco_monto": Decimal(str(abs(total))),
                    "fecha_conciliado": (
                        date.fromisoformat(max(reconciled_dates))
                        if reconciled_dates else None
                    ),
                    "fecha_deposito": " & ".join(sorted(set(str(d) for d in deposit_dates))),
                    "banco_ingreso": Decimal(str(abs(total))),
                }

        except ConnectionError:
            logger.warning(f"Cannot fetch bank data — Odoo offline")

        return {
            "banco_monto": Decimal("0"),
            "fecha_conciliado": None,
            "fecha_deposito": None,
            "banco_ingreso": Decimal("0"),
        }

    def get_tpv_total(self, station_id: int, close_date: date) -> Decimal:
        """
        Fetch TPV (card terminal) total from Odoo POS session.
        This is the sum of the 3 tiras auditoras equivalent.
        """
        try:
            payments = self._search_read(
                "account.payment",
                [
                    ("date", "=", str(close_date)),
                    ("payment_method_line_id.name", "in", ["Tarjeta", "POS"]),
                ],
                ["id", "amount"],
            )
            if payments:
                return Decimal(str(sum(p.get("amount", 0) for p in payments)))
        except ConnectionError:
            pass
        return Decimal("0")

    def get_daily_close_data(
        self, station_id: int, close_date: date
    ) -> dict:
        """
        Fetch ALL Odoo data needed for daily close in one call.
        Combines Compusafe, Bank, and TPV data.

        Returns dict ready to unpack into run_daily_close() kwargs.
        """
        compusafe = self.get_compusafe_data(station_id, close_date)
        bank = self.get_bank_data(station_id, close_date)
        tpv = self.get_tpv_total(station_id, close_date)

        return {
            "compusafe_efectivo": compusafe["efectivo_ingresado"],
            "compusafe_corte": compusafe["corte_temporal"],
            "compusafe_retiro": compusafe["retiro"],
            "banco_monto": bank["banco_monto"],
            "banco_ingreso": bank["banco_ingreso"],
            "fecha_conciliado": bank["fecha_conciliado"],
            "fecha_deposito": bank["fecha_deposito"],
            "tpv_tiras": tpv,
        }

    # ============================================================
    # HU-3.2: Reconciliation check A = D
    # ============================================================

    def get_odoo_daily_total(self, station_id: int, close_date: date) -> Decimal:
        """
        Get total MXN from all journal entries for a station on a date.
        This is source D in the A=D reconciliation check.
        """
        try:
            entries = self._search_read(
                "account.move",
                [
                    ("date", "=", str(close_date)),
                    ("ref", "like", f"%{close_date}%"),
                    ("state", "=", "posted"),
                    ("move_type", "=", "entry"),
                ],
                ["id", "amount_total"],
            )
            return Decimal(str(sum(e.get("amount_total", 0) for e in entries)))
        except ConnectionError:
            return Decimal("0")

    # ============================================================
    # Offline queue management
    # ============================================================

    def _enqueue(self, action: QueueAction, payload: dict):
        """Add an action to the offline queue."""
        entry = QueueEntry(action=action, payload=payload)
        self._offline_queue.append(entry)
        logger.warning(
            f"Odoo offline — queued {action.value} "
            f"(queue size: {len(self._offline_queue)})"
        )

    @property
    def queue_size(self) -> int:
        return len(self._offline_queue)

    def replay_queue(self) -> dict:
        """
        Replay all queued actions after Odoo reconnects.

        Returns: {"replayed": int, "failed": int, "remaining": int}
        """
        if not self.is_connected:
            try:
                self.authenticate()
            except Exception:
                return {
                    "replayed": 0,
                    "failed": 0,
                    "remaining": len(self._offline_queue),
                }

        stats = {"replayed": 0, "failed": 0, "remaining": 0}
        retry_queue: deque[QueueEntry] = deque()

        while self._offline_queue:
            entry = self._offline_queue.popleft()
            entry.attempts += 1

            try:
                if entry.action == QueueAction.CREATE_MOVE:
                    p = entry.payload
                    if p.get("type") == "daily_batch":
                        self.create_daily_batch_entry(
                            station_id=p["station_id"],
                            close_date=date.fromisoformat(p["close_date"]),
                            totals_by_medio={
                                k: {kk: Decimal(vv) for kk, vv in v.items()}
                                for k, v in p["totals_by_medio"].items()
                            },
                            total_recaudos=Decimal(p["total_recaudos"]),
                        )
                    else:
                        self.create_journal_entry(
                            station_id=p["station_id"],
                            close_date=date.fromisoformat(p["close_date"]),
                            medio_pago=p["medio_pago"],
                            total_mxn=Decimal(p["total_mxn"]),
                            ingreso_neto=Decimal(p["ingreso_neto"]),
                            iva=Decimal(p["iva"]),
                            placa=p["placa"],
                            recaudo=Decimal(p["recaudo"]),
                            ref=p.get("ref", ""),
                        )

                elif entry.action == QueueAction.CREATE_PARTNER:
                    self.sync_client(**entry.payload)

                stats["replayed"] += 1

            except Exception as e:
                entry.last_error = str(e)
                if entry.attempts < 3:
                    retry_queue.append(entry)
                else:
                    stats["failed"] += 1
                    logger.error(
                        f"Queue entry permanently failed after {entry.attempts} attempts: "
                        f"{entry.action.value} — {e}"
                    )

        # Put retries back
        self._offline_queue = retry_queue
        stats["remaining"] = len(self._offline_queue)

        logger.info(
            f"Queue replay: {stats['replayed']} replayed, "
            f"{stats['failed']} failed, {stats['remaining']} remaining"
        )
        return stats

    # ============================================================
    # Credit management (HU-2.1 extension)
    # ============================================================

    def update_client_credit(self, placa: str, amount: Decimal, operation: str = "charge"):
        """
        Update client credit balance in Odoo.
        operation: "charge" (add to debt) or "payment" (reduce debt)
        """
        partner_id = self.get_partner_id(placa)
        if not partner_id:
            logger.warning(f"Cannot update credit for {placa} — partner not found")
            return

        try:
            current = self._read("res.partner", [partner_id], ["credit"])
            current_credit = Decimal(str(current[0].get("credit", 0)))

            if operation == "charge":
                new_credit = current_credit + amount
            elif operation == "payment":
                new_credit = max(Decimal("0"), current_credit - amount)
            else:
                raise ValueError(f"Unknown operation: {operation}")

            self._write("res.partner", [partner_id], {"credit": float(new_credit)})
            logger.info(f"Updated credit {placa}: ${current_credit} → ${new_credit} ({operation})")

        except ConnectionError:
            self._enqueue(QueueAction.UPDATE_PARTNER, {
                "placa": placa, "amount": str(amount), "operation": operation,
            })

    def decrement_prepago(self, placa: str, amount: Decimal):
        """Decrement prepaid balance for a client."""
        self.update_client_credit(placa, amount, operation="payment")
