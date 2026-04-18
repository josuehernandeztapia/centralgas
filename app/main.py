"""
Central Gas Agent — FastAPI Application Entry Point

Phase 1 endpoints (manual ingestion mode):
  GET  /health                  → Liveness/readiness probe (Fly.io)
  GET  /                        → Service info
  POST /upload-excel            → Upload GasUp Excel report (.xls or .xlsx)
                                  Routes to the right parser based on report_type
  GET  /transactions            → List parsed transactions
  GET  /transactions/{id}       → Get single transaction detail
  GET  /stats                   → Quick stats (count, total LEQ, total recaudo)
  GET  /sobreprecio/distribution → Histogram of sobreprecio values

Phase 2+ endpoints (auto-ingestion mode):
  POST /webhook/gasup           → Receive Consware webhook push (future)
  POST /trigger/wrapper         → Manually trigger HeadOffice scrape (admin)
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.auth import require_auth

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "info").upper(),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

app = FastAPI(
    title="Central Gas Agent API",
    description="GasUp ingestion + retention + reconciliation engine for Central Gas",
    version="0.1.0-phase1",
)


# ============================================================
# Static files + dashboard
# ============================================================
# Mount /static for any future static assets (CSS, images, etc.)
_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/dashboard", include_in_schema=False)
async def dashboard(_: str = Depends(require_auth)):
    """Serve the single-file React dashboard (app/static/dashboard.html)."""
    dashboard_path = _STATIC_DIR / "dashboard.html"
    if not dashboard_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard not deployed")
    return FileResponse(str(dashboard_path), media_type="text/html")


# ============================================================
# Health & info endpoints
# ============================================================

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    phase: str
    timestamp: str


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Liveness/readiness probe used by Fly.io and docker-compose."""
    return HealthResponse(
        status="ok",
        service="central-gas-agent",
        version="0.1.0-phase1",
        phase="manual_ingestion",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/")
async def root():
    return {
        "service": "Central Gas AI Agent",
        "phase": "1 - Manual Ingestion (validation mode)",
        "stations": ["ECG-01 Nacozari", "ECG-02 Ojo Caliente", "ECG-03 Peñuelas"],
        "endpoints": {
            "health": "/health",
            "dashboard": "/dashboard",
            "upload": "POST /upload-excel",
            "transactions": "GET /transactions",
            "stats": "GET /stats",
            "sobreprecio": "GET /sobreprecio/distribution",
            "docs": "/docs",
        },
    }


# ============================================================
# Phase 1: Manual Excel upload
# ============================================================

ALLOWED_REPORT_TYPES = {"ventas_detalladas", "recaudos_financiera"}
ALLOWED_EXTENSIONS = {".xls", ".xlsx"}
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB cap


class UploadResponse(BaseModel):
    status: str
    filename: str
    report_type: str
    estacion_id: Optional[str]
    rows_parsed: int
    rows_inserted: int
    rows_skipped: int
    warnings: list[str]
    detected_format: str


@app.post("/upload-excel", response_model=UploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_excel(
    file: UploadFile = File(..., description="GasUp Excel report (.xls or .xlsx)"),
    report_type: str = Form(..., description="ventas_detalladas | recaudos_financiera"),
    estacion_id: Optional[str] = Form(None, description="ECG-01 | ECG-02 | ECG-03 (optional, can be inferred)"),
    financiera_filter: Optional[str] = Form(
        None,
        description=(
            "For recaudos_financiera: CSV of financiera names to keep. "
            "Empty/None = process ALL (recommended for Phase 1 validation). "
            "Use 'CONDUCTORES,CENTRAL GAS' to filter to CMU/CG only when in production."
        ),
    ),
    _: str = Depends(require_auth),
) -> UploadResponse:
    """
    Receive a manual upload of a GasUp report and feed it into the connector.

    Phase 1 flow:
      1. Validate file extension and size
      2. Save to temp file (xls/xlsx detected via magic bytes)
      3. Route to gasup_connector.ingest_excel_report() with calibrated parsers
      4. Return parse summary

    Note: Phase 1 does NOT execute downstream actions (no Odoo CFDI, no WhatsApp).
    The agent only ingests + analyzes for validation purposes.
    """
    # --- Validate report type ---
    if report_type not in ALLOWED_REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"report_type must be one of {sorted(ALLOWED_REPORT_TYPES)}",
        )

    # --- Validate extension ---
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Only {sorted(ALLOWED_EXTENSIONS)} files accepted. Got: {suffix or '(no extension)'}",
        )

    # --- Read into memory (with cap) ---
    contents = await file.read()
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Max {MAX_UPLOAD_BYTES // (1024*1024)} MB.",
        )
    if len(contents) == 0:
        raise HTTPException(status_code=400, detail="Empty file uploaded.")

    # --- Detect actual format via magic bytes ---
    is_ole2 = contents[:4] == b"\xd0\xcf\x11\xe0"
    actual_suffix = ".xls" if is_ole2 else ".xlsx"
    detected_format = "OLE2 (.xls)" if is_ole2 else "OOXML (.xlsx)"

    # --- Parse financiera_filter ---
    filter_set: Optional[set[str]]
    if financiera_filter is None or financiera_filter.strip() == "":
        filter_set = None  # None = process all (Phase 1 default)
    else:
        filter_set = {f.strip().upper() for f in financiera_filter.split(",") if f.strip()}

    # --- Save to temp file with correct extension ---
    warnings: list[str] = []
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(suffix=actual_suffix, delete=False) as tmp:
            tmp.write(contents)
            tmp_path = Path(tmp.name)

        if actual_suffix != suffix:
            warnings.append(
                f"Filename had extension {suffix} but content is {detected_format}. Using actual format."
            )

        # --- Lazy import to keep startup fast and avoid circular imports ---
        import hashlib
        from app.services.gasup_connector import GasUpConnector
        from app.db.transactions import bulk_insert_transactions

        connector = GasUpConnector()
        # Connector parses into self._transactions list; returns count.
        count = connector.ingest_excel_report(
            filepath=tmp_path,
            report_type=report_type,
            estacion_id=estacion_id or "",
            financiera_filter=filter_set,
        )

        # Persist parsed transactions to Postgres (Neon).
        # NOTE: only ventas_detalladas gets persisted as transactions rows.
        # recaudos_financiera has a different shape (no placa, no timestamp per row —
        # it's payment records grouped by financiera + factura code). Persisting those
        # as "transactions" would pollute the table. Phase 2 will wire a separate
        # `financiera_recaudos` table and a join-by-factura enrichment job.
        source_hash = hashlib.sha256(contents).hexdigest()[:16]
        if report_type == "ventas_detalladas":
            persist = bulk_insert_transactions(
                transactions=connector._transactions,
                source_file=file.filename or "unknown",
                source_hash=source_hash,
            )
        else:
            persist = {
                "inserted": 0, "skipped": 0, "errors": 0,
                "errors_detail": [], "skipped_invalid": 0,
            }
            warnings.append(
                f"report_type='{report_type}' parsed OK ({count} rows) but NOT persisted. "
                "Only ventas_detalladas is wired to the transactions table in Phase 1. "
                "Recaudos/other formats will get their own table in Phase 2."
            )

        if persist["errors"] > 0:
            warnings.append(
                f"persist errors: {persist['errors']} batches failed. "
                f"first: {persist['errors_detail'][0] if persist['errors_detail'] else 'n/a'}"
            )
        if persist.get("skipped_invalid", 0) > 0:
            warnings.append(
                f"filtered {persist['skipped_invalid']} invalid rows "
                f"(missing timestamp/placa — likely blank/subtotal lines in the .xls)"
            )

        # rows_skipped now includes BOTH ON CONFLICT skips AND invalid-row skips
        # for visibility in the response. They're separated in `warnings`.
        total_skipped = persist["skipped"] + persist.get("skipped_invalid", 0)

        return UploadResponse(
            status="ok",
            filename=file.filename or "unknown",
            report_type=report_type,
            estacion_id=estacion_id,
            rows_parsed=count,
            rows_inserted=persist["inserted"],
            rows_skipped=total_skipped,
            warnings=warnings,
            detected_format=detected_format,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Upload processing failed")
        raise HTTPException(status_code=500, detail=f"Parse failed: {e}")
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


# ============================================================
# Phase 1: Read endpoints (Postgres-backed when wired up)
# ============================================================

@app.get("/transactions")
async def get_transactions(
    limit: int = 100,
    offset: int = 0,
    estacion: Optional[str] = None,
    placa: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    _: str = Depends(require_auth),
):
    """
    List transactions from Postgres with pagination + filters.

    Query params:
      - limit (default 100, max 1000)
      - offset (default 0)
      - estacion (substring match on station_natgas, case-insensitive)
      - placa (exact match, normalized to uppercase)
      - date_from / date_to (YYYY-MM-DD; range is [from, to+1day) in local time)
    """
    from app.db.queries import list_transactions
    try:
        return list_transactions(
            limit=limit, offset=offset,
            estacion=estacion, placa=placa,
            date_from=date_from, date_to=date_to,
        )
    except Exception as e:
        logger.exception("list_transactions failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


@app.get("/stats")
async def quick_stats(_: str = Depends(require_auth)):
    """Aggregate KPIs across the transactions table."""
    from app.db.queries import aggregate_stats
    try:
        return aggregate_stats()
    except Exception as e:
        logger.exception("aggregate_stats failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


@app.get("/transactions.csv")
async def export_transactions_csv(
    estacion: Optional[str] = None,
    placa: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100000,
    _: str = Depends(require_auth),
):
    """
    Stream transactions as CSV (HU-DASH-2.4).

    Respects the same filters as /transactions. Default cap 100k rows to
    avoid blowing up memory on very wide queries.
    """
    import csv
    import io
    from app.db.queries import list_transactions

    try:
        data = list_transactions(
            limit=min(limit, 100000), offset=0,
            estacion=estacion, placa=placa,
            date_from=date_from, date_to=date_to,
        )
    except Exception as e:
        logger.exception("export csv failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")

    rows = data.get("rows", [])
    cols = [
        "id", "source_file", "station_natgas", "timestamp_local", "placa",
        "litros", "pvp", "total_mxn", "recaudo_valor", "recaudo_pagado",
        "medio_pago", "kg", "nm3", "ingreso_neto", "iva",
    ]

    def _gen():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

        for r in rows:
            writer.writerow({c: r.get(c, "") for c in cols})
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    fname_parts = ["transactions"]
    if date_from: fname_parts.append(f"from-{date_from}")
    if date_to:   fname_parts.append(f"to-{date_to}")
    if estacion:  fname_parts.append(estacion.replace(" ", "_"))
    fname = "_".join(fname_parts) + ".csv"

    return StreamingResponse(
        _gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/sobreprecio/distribution")
async def sobreprecio_distribution_endpoint(
    buckets: int = 12,
    _: str = Depends(require_auth),
):
    """
    Histogram + descriptive stats for the per-LEQ surcharge field
    (recaudo_pagado column, which holds tx.sobreprecio from the parser).

    Query params:
      - buckets: number of histogram bins (default 12, max 100)
    """
    from app.db.queries import sobreprecio_distribution
    try:
        return sobreprecio_distribution(buckets=buckets)
    except Exception as e:
        logger.exception("sobreprecio_distribution failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# /api/recaudos — CMU collections (derived from transactions)
# ============================================================

@app.get("/api/recaudos")
async def api_recaudos(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    placa: Optional[str] = None,
    limit: int = 5000,
    offset: int = 0,
    _: str = Depends(require_auth),
):
    """
    CMU recaudos report — derived from transactions WHERE recaudo > 0.

    Returns:
      - kpis: placas_activas, total_litros, total_recaudado, tarifa_promedio
      - by_placa: summary per plate (cargas, litros, tarifa, total)
      - by_day: summary per day (cargas, placas, litros, recaudado)
      - rows: paginated detail rows matching NatGas→CMU format

    This replaces the weekly Excel that NatGas sends to CMU
    with a daily API + dashboard.
    """
    from app.db.queries import list_recaudos
    try:
        return list_recaudos(
            date_from=date_from, date_to=date_to,
            placa=placa, limit=limit, offset=offset,
        )
    except Exception as e:
        logger.exception("list_recaudos failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


@app.get("/api/recaudos.csv")
async def api_recaudos_csv(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    placa: Optional[str] = None,
    limit: int = 100000,
    _: str = Depends(require_auth),
):
    """
    Export CMU recaudos as CSV — compatible with the NatGas→CMU Excel format.
    """
    import csv
    import io
    from app.db.queries import list_recaudos

    try:
        data = list_recaudos(
            date_from=date_from, date_to=date_to,
            placa=placa, limit=min(limit, 100000), offset=0,
        )
    except Exception as e:
        logger.exception("recaudos csv failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")

    rows = data.get("rows", [])
    cols = [
        "placa", "conductor", "litros", "tarifa_leq",
        "cantidad_recaudo", "fecha_hora_venta", "fecha_venta",
        "estacion", "pvp", "total_mxn", "id_placa_recaudo",
    ]

    def _gen():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)
        for r in rows:
            writer.writerow({c: r.get(c, "") for c in cols})
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    fname_parts = ["recaudos_cmu"]
    if date_from: fname_parts.append(f"from-{date_from}")
    if date_to:   fname_parts.append(f"to-{date_to}")
    fname = "_".join(fname_parts) + ".csv"

    return StreamingResponse(
        _gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ============================================================
# /api/stations/:name — station drill-down (HU-DASH-2.1)
# ============================================================

@app.get("/api/stations/{station_name}")
async def api_station_detail(
    station_name: str,
    _: str = Depends(require_auth),
):
    """Drill-down for a single station: monthly volume, top placas, heatmap."""
    from app.db.queries import station_detail
    try:
        return station_detail(station_name)
    except Exception as e:
        logger.exception("station_detail failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# /api/placas/:placa — placa drill-down (HU-DASH-2.2)
# ============================================================

@app.get("/api/placas/{placa}")
async def api_placa_detail(
    placa: str,
    _: str = Depends(require_auth),
):
    """Drill-down for a single placa: lifetime stats, monthly trend, stations, recent txns."""
    from app.db.queries import placa_detail
    try:
        return placa_detail(placa)
    except Exception as e:
        logger.exception("placa_detail failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# /api/search — global search (HU-DASH-2.5)
# ============================================================

@app.get("/api/search")
async def api_search(
    q: str = "",
    limit: int = 20,
    _: str = Depends(require_auth),
):
    """Search placas and stations by query string."""
    from app.db.queries import global_search
    try:
        return global_search(q, limit=min(limit, 50))
    except Exception as e:
        logger.exception("global_search failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# /api/retention — inactive placas alerts (HU-DASH-3.3)
# ============================================================

@app.get("/api/retention")
async def api_retention(
    days: int = 7,
    min_cargas: int = 3,
    _: str = Depends(require_auth),
):
    """Placas that were active but haven't refueled in N days."""
    from app.db.queries import retention_alerts
    try:
        return retention_alerts(days_inactive=days, min_cargas=min_cargas)
    except Exception as e:
        logger.exception("retention_alerts failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# /api/health-scores — Customer Health Score Engine
# ============================================================

@app.post("/api/health-scores/compute")
async def api_compute_health_scores(
    _: str = Depends(require_auth),
):
    """Compute health scores for all active placas. Run daily via cron or on-demand."""
    from app.db.queries import compute_health_scores
    try:
        result = compute_health_scores()
        return result
    except Exception as e:
        logger.exception("compute_health_scores failed")
        raise HTTPException(status_code=500, detail=f"compute failed: {e}")


@app.get("/api/health-scores")
async def api_health_scores(
    classification: str = "",
    limit: int = 200,
    _: str = Depends(require_auth),
):
    """Get latest health scores, optionally filtered by classification."""
    from app.db.queries import get_health_scores
    try:
        return get_health_scores(
            classification=classification or None,
            limit=min(limit, 500),
        )
    except Exception as e:
        logger.exception("get_health_scores failed")
        raise HTTPException(status_code=500, detail=f"query failed: {e}")


# ============================================================
# Phase 2 placeholder endpoints (return 501 for now)
# ============================================================

@app.post("/webhook/gasup", status_code=501)
async def gasup_webhook():
    return JSONResponse(
        status_code=501,
        content={"detail": "Webhook receiver not enabled in Phase 1. Use POST /upload-excel."},
    )


@app.post("/trigger/wrapper", status_code=501)
async def trigger_wrapper():
    return JSONResponse(
        status_code=501,
        content={"detail": "HeadOffice wrapper not enabled in Phase 1. Manual upload only."},
    )
