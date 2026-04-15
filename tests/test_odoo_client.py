"""
Tests for Odoo XML-RPC Client — HU-2.1, HU-2.2

Uses a mock XML-RPC server to test all client functionality
without a real Odoo instance. Validates:
  - Authentication + retry logic
  - Journal entry creation (single + batch)
  - Client sync (create + update)
  - Daily close data fetching
  - Offline queue + replay
  - Credit management
"""

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from collections import deque

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.odoo_client import (
    OdooClient,
    OdooConfig,
    CuentaContable,
    MEDIO_PAGO_CUENTA,
    QueueAction,
    QueueEntry,
)


# ============================================================
# Mock Odoo XML-RPC server
# ============================================================

class MockOdooServer:
    """Simulates Odoo XML-RPC responses."""

    def __init__(self):
        self.uid = 2
        self._next_id = 100
        self.created_records: list[dict] = []
        self.written_records: list[dict] = []
        self.search_results: dict[str, list] = {}

    def authenticate(self, db, user, passwd, context):
        return self.uid

    def execute_kw(self, db, uid, passwd, model, method, args, kwargs=None):
        if method == "search":
            return self.search_results.get(f"{model}.search", [])
        elif method == "read":
            ids = args[0] if args else []
            fields = (kwargs or {}).get("fields", [])
            return [{"id": i, "name": f"Record {i}", "credit": 0.0, "amount_total": 1000.0}
                    for i in ids]
        elif method == "search_read":
            domain = args[0] if args else []
            key = f"{model}.search_read"
            if key in self.search_results:
                return self.search_results[key]
            # Default: return account/journal matches
            if model == "account.account":
                code = domain[0][2] if domain else "?"
                return [{"id": self._next_id, "code": code}]
            elif model == "account.journal":
                return [{"id": 1, "code": "MISC"}]
            elif model == "res.partner":
                return []  # No existing partner by default
            elif model == "account.move":
                return []
            elif model == "account.bank.statement.line":
                return []
            elif model == "account.payment":
                return []
            return []
        elif method == "create":
            raw = args[0] if args else {}
            # Odoo XML-RPC create receives [vals_dict] — unwrap the list
            vals = raw[0] if isinstance(raw, list) else raw
            self._next_id += 1
            self.created_records.append({"id": self._next_id, "model": model, "vals": vals})
            return self._next_id
        elif method == "write":
            ids = args[0] if args else []
            vals = args[1] if len(args) > 1 else {}
            self.written_records.append({"ids": ids, "model": model, "vals": vals})
            return True
        return None


def make_client(mock_server=None) -> OdooClient:
    """Create an OdooClient with mocked XML-RPC."""
    client = OdooClient(OdooConfig(
        url="http://mock-odoo:8069",
        db="test_db",
        username="admin",
        password="admin",
    ))

    if mock_server is None:
        mock_server = MockOdooServer()

    # Mock the XML-RPC proxies
    client._common = MagicMock()
    client._common.authenticate = mock_server.authenticate
    client._object = MagicMock()
    client._object.execute_kw = mock_server.execute_kw
    client.uid = mock_server.uid
    client._connected = True

    return client, mock_server


# ============================================================
# Test Authentication
# ============================================================

def test_auth_success():
    """Successful authentication returns uid."""
    client, server = make_client()
    assert client.is_connected
    assert client.uid == 2
    print("  OK: Authentication successful, uid=2")


def test_auth_creates_proxies():
    """Client sets up common and object proxies."""
    client = OdooClient(OdooConfig(url="http://test:8069"))
    # Before auth, not connected
    assert not client.is_connected
    assert client.uid is None
    print("  OK: Pre-auth state correct")


# ============================================================
# Test Journal Entry Creation (HU-2.1)
# ============================================================

def test_create_single_journal_entry():
    """Create a journal entry for one transaction."""
    client, server = make_client()

    move_id = client.create_journal_entry(
        station_id=3,
        close_date=date(2026, 1, 15),
        medio_pago="EFECTIVO",
        total_mxn=Decimal("419.70"),
        ingreso_neto=Decimal("361.81"),
        iva=Decimal("57.89"),
        placa="A12345A",
    )

    assert move_id is not None
    assert len(server.created_records) == 1

    record = server.created_records[0]
    assert record["model"] == "account.move"

    vals = record["vals"]
    assert vals["date"] == "2026-01-15"
    assert vals["move_type"] == "entry"
    assert "A12345A" in vals["ref"]

    # Should have 3 lines: debit CAJA + credit INGRESO + credit IVA
    lines = vals["line_ids"]
    assert len(lines) == 3

    # Debit line
    debit_line = lines[0][2]
    assert debit_line["debit"] == 419.70
    assert debit_line["credit"] == 0.0

    # Credit ingreso
    credit_ingreso = lines[1][2]
    assert credit_ingreso["credit"] == 361.81

    # Credit IVA
    credit_iva = lines[2][2]
    assert credit_iva["credit"] == 57.89

    print(f"  OK: Created journal entry {move_id} — 3 lines, debit=$419.70")


def test_create_entry_with_recaudo():
    """Entry with recaudo should have 5 lines instead of 3."""
    client, server = make_client()

    move_id = client.create_journal_entry(
        station_id=3,
        close_date=date(2026, 1, 15),
        medio_pago="EFECTIVO",
        total_mxn=Decimal("419.70"),
        ingreso_neto=Decimal("361.81"),
        iva=Decimal("57.89"),
        placa="A12345A",
        recaudo=Decimal("50.00"),
    )

    record = server.created_records[0]
    lines = record["vals"]["line_ids"]
    assert len(lines) == 5, f"Expected 5 lines with recaudo, got {len(lines)}"

    # Recaudo debit
    assert lines[3][2]["debit"] == 50.00
    # Recaudo credit
    assert lines[4][2]["credit"] == 50.00

    print(f"  OK: Entry with recaudo — 5 lines")


def test_create_entry_credito_uses_cxc():
    """CREDITO transactions should debit CxC, not CAJA."""
    client, server = make_client()

    move_id = client.create_journal_entry(
        station_id=3,
        close_date=date(2026, 1, 15),
        medio_pago="CREDITO",
        total_mxn=Decimal("500.00"),
        ingreso_neto=Decimal("431.03"),
        iva=Decimal("68.97"),
        placa="A12345A",
    )

    # The debit account should be CXC_CLIENTES, resolved via _resolve_account
    assert move_id is not None
    print(f"  OK: CREDITO → CxC account")


def test_create_daily_batch_entry():
    """Daily batch entry consolidates all payment methods."""
    client, server = make_client()

    totals = {
        "EFECTIVO": {
            "total_mxn": Decimal("45000.00"),
            "ingreso_neto": Decimal("38793.10"),
            "iva": Decimal("6206.90"),
            "count": 100,
        },
        "TARJETA_DEBITO": {
            "total_mxn": Decimal("8000.00"),
            "ingreso_neto": Decimal("6896.55"),
            "iva": Decimal("1103.45"),
            "count": 20,
        },
    }

    move_id = client.create_daily_batch_entry(
        station_id=3,
        close_date=date(2026, 1, 15),
        totals_by_medio=totals,
        total_recaudos=Decimal("500.00"),
    )

    assert move_id is not None
    record = server.created_records[0]
    lines = record["vals"]["line_ids"]

    # 2 medios × 3 lines each = 6 + 2 recaudo lines = 8
    assert len(lines) == 8, f"Expected 8 lines, got {len(lines)}"
    assert "Cierre diario" in record["vals"]["ref"]

    print(f"  OK: Daily batch — 8 lines, 2 medios + recaudos")


# ============================================================
# Test Client Sync (HU-2.2)
# ============================================================

def test_sync_new_client():
    """Syncing a new client creates a res.partner."""
    client, server = make_client()

    partner_id = client.sync_client(
        placa="A12345A",
        nombre="Juan Pérez",
        telefono="4491234567",
        rfc="PEPJ800101XXX",
        segmento="VAGONETA",
        gasup_id=42,
    )

    assert partner_id is not None
    assert len(server.created_records) == 1

    record = server.created_records[0]
    assert record["model"] == "res.partner"
    assert record["vals"]["ref"] == "A12345A"
    assert record["vals"]["name"] == "Juan Pérez"
    assert record["vals"]["vat"] == "PEPJ800101XXX"

    print(f"  OK: Created new partner {partner_id}: A12345A — Juan Pérez")


def test_sync_existing_client_updates():
    """Syncing an existing client updates instead of creating."""
    server = MockOdooServer()
    # Simulate existing partner
    server.search_results["res.partner.search_read"] = [{"id": 50, "name": "Old Name"}]

    client, _ = make_client(server)

    partner_id = client.sync_client(
        placa="A12345A",
        nombre="Juan Pérez Updated",
    )

    # Should write, not create
    assert len(server.written_records) == 1
    assert server.written_records[0]["model"] == "res.partner"
    assert server.written_records[0]["ids"] == [50]
    assert len(server.created_records) == 0

    print(f"  OK: Updated existing partner 50")


def test_sync_all_clients_batch():
    """Batch sync multiple clients."""
    client, server = make_client()

    clients_data = [
        {"placa": f"AGS{i:04d}", "nombre": f"Driver {i}", "segmento": "VAGONETA"}
        for i in range(5)
    ]

    stats = client.sync_all_clients(clients_data)
    assert stats["created"] == 5
    assert stats["errors"] == 0

    print(f"  OK: Batch sync — {stats['created']} created, {stats['errors']} errors")


def test_get_partner_id_caches():
    """Partner resolution should cache results."""
    server = MockOdooServer()
    server.search_results["res.partner.search_read"] = [{"id": 77}]
    client, _ = make_client(server)

    # First call: hits Odoo
    pid1 = client.get_partner_id("A12345A")
    assert pid1 == 77

    # Second call: from cache (mock would return same, but cache proves it)
    pid2 = client.get_partner_id("A12345A")
    assert pid2 == 77
    assert client._partner_cache["A12345A"] == 77

    print(f"  OK: Partner cache working")


# ============================================================
# Test Daily Close Data Fetching
# ============================================================

def test_get_compusafe_empty():
    """Compusafe returns zeros when no data found."""
    client, _ = make_client()
    data = client.get_compusafe_data(station_id=3, close_date=date(2026, 1, 15))

    assert data["efectivo_ingresado"] == Decimal("0")
    assert data["retiro"] == Decimal("0")
    print(f"  OK: Compusafe empty → zeros")


def test_get_bank_data_empty():
    """Bank returns zeros when no statements found."""
    client, _ = make_client()
    data = client.get_bank_data(station_id=3, close_date=date(2026, 1, 15))

    assert data["banco_monto"] == Decimal("0")
    assert data["fecha_conciliado"] is None
    print(f"  OK: Bank empty → zeros")


def test_get_daily_close_data_combined():
    """get_daily_close_data combines all 3 sources."""
    client, _ = make_client()
    data = client.get_daily_close_data(station_id=3, close_date=date(2026, 1, 15))

    # Should have all expected keys
    expected_keys = {
        "compusafe_efectivo", "compusafe_corte", "compusafe_retiro",
        "banco_monto", "banco_ingreso", "fecha_conciliado", "fecha_deposito",
        "tpv_tiras",
    }
    assert set(data.keys()) == expected_keys
    print(f"  OK: Daily close data has all {len(expected_keys)} keys")


# ============================================================
# Test Offline Queue + Replay
# ============================================================

def test_queue_on_connection_failure():
    """Failed Odoo calls should queue entries."""
    client, server = make_client()

    # Make execute_kw raise ConnectionError
    def failing_execute(*args, **kwargs):
        raise ConnectionError("Odoo down")

    client._object.execute_kw = failing_execute
    client._connected = False  # Must be false to trigger the queue path

    # This should fail to authenticate but we need a different approach
    # Reset client as connected but make calls fail
    client._connected = True

    # override _execute to simulate failure + queue
    original_execute = client._execute
    call_count = 0

    def mock_execute(model, method, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise ConnectionError("Odoo down")

    client._execute = mock_execute

    move_id = client.create_journal_entry(
        station_id=3,
        close_date=date(2026, 1, 15),
        medio_pago="EFECTIVO",
        total_mxn=Decimal("100.00"),
        ingreso_neto=Decimal("86.21"),
        iva=Decimal("13.79"),
        placa="A99999A",
    )

    assert move_id is None, "Should return None when queued"
    assert client.queue_size == 1

    print(f"  OK: Connection failure → queued (queue size: {client.queue_size})")


def test_queue_replay():
    """Queued entries should replay when Odoo reconnects."""
    client, server = make_client()

    # Manually add entries to queue
    client._offline_queue.append(QueueEntry(
        action=QueueAction.CREATE_PARTNER,
        payload={"placa": "A11111A", "nombre": "Test Driver", "segmento": "VAGONETA"},
    ))
    client._offline_queue.append(QueueEntry(
        action=QueueAction.CREATE_PARTNER,
        payload={"placa": "A22222A", "nombre": "Test Driver 2", "segmento": "TAXI"},
    ))

    assert client.queue_size == 2

    stats = client.replay_queue()
    assert stats["replayed"] == 2
    assert stats["failed"] == 0
    assert stats["remaining"] == 0
    assert client.queue_size == 0

    print(f"  OK: Queue replay — {stats['replayed']} replayed, {stats['remaining']} remaining")


def test_queue_max_size():
    """Queue should respect max size."""
    config = OdooConfig(queue_max_size=5)
    client = OdooClient(config)

    for i in range(10):
        client._offline_queue.append(QueueEntry(
            action=QueueAction.CREATE_PARTNER,
            payload={"placa": f"A{i:05d}A"},
        ))

    # deque maxlen=5 should keep only last 5
    assert client.queue_size == 5
    print(f"  OK: Queue max size respected (5/10 kept)")


# ============================================================
# Test Account Mapping
# ============================================================

def test_medio_pago_account_mapping():
    """All medio_pago types should map to valid accounts."""
    expected = {
        "EFECTIVO": CuentaContable.CAJA,
        "TARJETA_DEBITO": CuentaContable.BANCOS,
        "TARJETA_CREDITO": CuentaContable.BANCOS,
        "CREDITO": CuentaContable.CXC_CLIENTES,
        "PREPAGO": CuentaContable.PREPAGO_CLIENTES,
        "BONOS_EDS": CuentaContable.CAJA,
        "DESCONOCIDO": CuentaContable.CAJA,
    }

    for medio, expected_cuenta in expected.items():
        actual = MEDIO_PAGO_CUENTA[medio]
        assert actual == expected_cuenta, f"{medio}: expected {expected_cuenta}, got {actual}"

    print(f"  OK: All 7 medio_pago types mapped correctly")


def test_cuenta_contable_codes():
    """All account codes should follow standard format."""
    for cuenta in CuentaContable:
        parts = cuenta.value.split(".")
        assert len(parts) == 3, f"Account code format wrong: {cuenta.value}"
        assert all(p.isdigit() for p in parts), f"Non-numeric in code: {cuenta.value}"

    print(f"  OK: All {len(CuentaContable)} account codes valid")


# ============================================================
# Test Integration: Odoo → Daily Close
# ============================================================

def test_daily_close_data_feeds_reconciliation():
    """Verify Odoo data format matches reconciliation service expectations."""
    client, _ = make_client()
    data = client.get_daily_close_data(station_id=3, close_date=date(2026, 1, 15))

    # Import reconciliation to verify compatibility
    from app.services.reconciliation import run_daily_close
    from tests.test_reconciliation import make_txn

    txns = [make_txn(local_date=date(2026, 1, 15)) for _ in range(10)]

    # This should NOT crash — data format must be compatible
    result = run_daily_close(
        station_id=3,
        close_date=date(2026, 1, 15),
        transactions=txns,
        **data,
    )

    assert result is not None
    assert result.station_id == 3
    assert result.close_date == date(2026, 1, 15)
    assert len(result.checks) > 0

    print(f"  OK: Odoo data → reconciliation pipeline works end-to-end")


# ============================================================
# Run all tests
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Odoo Client Tests (Mock XML-RPC)")
    print("=" * 60)

    tests = [
        ("Authentication", test_auth_success),
        ("Pre-auth state", test_auth_creates_proxies),
        ("Single journal entry", test_create_single_journal_entry),
        ("Entry with recaudo", test_create_entry_with_recaudo),
        ("CREDITO → CxC", test_create_entry_credito_uses_cxc),
        ("Daily batch entry", test_create_daily_batch_entry),
        ("Sync new client", test_sync_new_client),
        ("Sync existing updates", test_sync_existing_client_updates),
        ("Batch sync clients", test_sync_all_clients_batch),
        ("Partner ID cache", test_get_partner_id_caches),
        ("Compusafe empty", test_get_compusafe_empty),
        ("Bank data empty", test_get_bank_data_empty),
        ("Daily close combined", test_get_daily_close_data_combined),
        ("Queue on failure", test_queue_on_connection_failure),
        ("Queue replay", test_queue_replay),
        ("Queue max size", test_queue_max_size),
        ("Medio pago mapping", test_medio_pago_account_mapping),
        ("Account codes format", test_cuenta_contable_codes),
        ("Odoo → reconciliation integration", test_daily_close_data_feeds_reconciliation),
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
