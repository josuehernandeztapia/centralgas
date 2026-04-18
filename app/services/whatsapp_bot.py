"""
WhatsApp Bot — Keyword-based conversational bot for conductores.

Receives incoming WhatsApp messages via Twilio webhook, matches keywords,
queries Neon DB, and responds via Twilio API.

Keywords:
  puntos   → CMU benefits summary, cargas acumuladas
  factura  → Pending cargas to invoice, triggers CFDI in Odoo
  saldo    → CMU financial product status
  ahorro   → GNV vs gasoline savings comparison
  rfc      → Query or register RFC for auto-invoicing
  referir  → Generate unique referral link
  ayuda    → List of available keywords + human contact

Architecture:
  Twilio webhook → POST /webhook/whatsapp → parse keyword → query DB → respond
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger("whatsapp_bot")


# ============================================================
# DB: wa_messages table + logging
# ============================================================

def _get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL env var not set")
    import psycopg2
    return psycopg2.connect(db_url)


def ensure_wa_messages_table():
    """Create wa_messages table if it doesn't exist."""
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS wa_messages (
                        id SERIAL PRIMARY KEY,
                        direction VARCHAR(10) NOT NULL DEFAULT 'in',
                        wa_from VARCHAR(30) NOT NULL,
                        wa_to VARCHAR(30) NOT NULL,
                        placa VARCHAR(20),
                        keyword VARCHAR(30),
                        body TEXT,
                        response TEXT,
                        twilio_sid VARCHAR(64),
                        status VARCHAR(20) DEFAULT 'received',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_wa_from ON wa_messages(wa_from);
                    CREATE INDEX IF NOT EXISTS idx_wa_placa ON wa_messages(placa);
                    CREATE INDEX IF NOT EXISTS idx_wa_created ON wa_messages(created_at DESC);
                """)
    finally:
        conn.close()


def log_message(
    direction: str,
    wa_from: str,
    wa_to: str,
    body: str,
    placa: str = None,
    keyword: str = None,
    response: str = None,
    twilio_sid: str = None,
    status: str = "received",
):
    """Log a WhatsApp message to the wa_messages table."""
    try:
        conn = _get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO wa_messages
                            (direction, wa_from, wa_to, placa, keyword, body, response, twilio_sid, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (direction, wa_from, wa_to, placa, keyword, body, response, twilio_sid, status))
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to log wa_message: {e}")


# ============================================================
# Lookup: phone → placa(s)
# ============================================================

def lookup_placa_by_phone(phone: str) -> Optional[dict]:
    """
    Find client by phone number. Returns the client with most recent activity.
    Phone is normalized: strip whatsapp: prefix, keep +52...
    """
    # Normalize phone
    phone_clean = phone.replace("whatsapp:", "").strip()
    # Try exact match first, then try without country code
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Try matching telefono field (which may have various formats)
            cur.execute("""
                SELECT c.placa, c.nombre, c.telefono, c.segmento, c.modelo_vehiculo,
                       c.fecha_conversion, c.consumo_prom_lt, c.eds_principal
                FROM clients c
                WHERE REPLACE(REPLACE(REPLACE(c.telefono, ' ', ''), '-', ''), '+', '')
                    LIKE '%' || REPLACE(REPLACE(REPLACE(%s, ' ', ''), '-', ''), '+', '')
                ORDER BY c.placa
                LIMIT 5
            """, (phone_clean[-10:],))  # Match last 10 digits
            rows = cur.fetchall()
            if not rows:
                return None
            # Return first match with enriched data
            r = rows[0]
            return {
                "placa": r[0],
                "nombre": r[1],
                "telefono": r[2],
                "segmento": r[3],
                "modelo": r[4],
                "fecha_conversion": str(r[5]) if r[5] else None,
                "consumo_prom": float(r[6]) if r[6] else None,
                "eds_principal": r[7],
                "all_placas": [row[0] for row in rows],
            }
    finally:
        conn.close()


# ============================================================
# Keyword handlers
# ============================================================

def _serialize(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def handle_puntos(placa: str, client: dict) -> str:
    """Handler for 'puntos' keyword — CMU benefits summary."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_cargas,
                    COALESCE(SUM(litros), 0) AS total_litros,
                    COALESCE(SUM(total_mxn), 0) AS total_gastado,
                    COALESCE(SUM(recaudo_valor), 0) AS total_recaudo,
                    MAX(timestamp_local) AS ultima_carga
                FROM transactions
                WHERE placa = %s
            """, (placa,))
            r = cur.fetchone()
            cargas = r[0] or 0
            litros = float(r[1] or 0)
            gastado = float(r[2] or 0)
            recaudo = float(r[3] or 0)
            ultima = r[4]

            # Health score
            cur.execute("""
                SELECT score_total, classification
                FROM health_scores
                WHERE placa = %s
                ORDER BY score_date DESC LIMIT 1
            """, (placa,))
            hs = cur.fetchone()
            score = float(hs[0]) if hs else None
            clasif = hs[1] if hs else "sin calcular"
    finally:
        conn.close()

    nombre = client.get("nombre") or placa
    lines = [
        f"⛽ *{nombre}* ({placa})",
        f"",
        f"📊 *Resumen de actividad:*",
        f"• Cargas totales: {cargas:,}",
        f"• Litros acumulados: {litros:,.0f} LEQ",
        f"• Total gastado: ${gastado:,.0f} MXN",
    ]
    if recaudo > 0:
        lines.append(f"• Recaudo CMU: ${recaudo:,.0f} MXN")
    if score is not None:
        emoji = "🟢" if score >= 80 else "🟡" if score >= 60 else "🟠" if score >= 40 else "🔴"
        lines.append(f"")
        lines.append(f"{emoji} Health Score: *{score}* ({clasif})")
    if ultima:
        lines.append(f"")
        lines.append(f"📅 Última carga: {ultima.strftime('%d/%m/%Y %H:%M')}")

    return "\n".join(lines)


def handle_ahorro(placa: str, client: dict) -> str:
    """Handler for 'ahorro' — GNV vs gasoline savings."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(litros), 0) AS total_litros,
                    COALESCE(SUM(total_mxn), 0) AS total_gnv
                FROM transactions
                WHERE placa = %s
            """, (placa,))
            r = cur.fetchone()
            litros_gnv = float(r[0] or 0)
            total_gnv = float(r[1] or 0)
    finally:
        conn.close()

    # 1 LEQ GNV ≈ 1.2 litros gasolina equivalente
    # Precio promedio gasolina Magna: ~$24.50/litro (Abril 2026 aprox)
    GASOLINA_PRECIO = 24.50
    FACTOR_EQUIVALENCIA = 1.2
    litros_gasolina_equiv = litros_gnv * FACTOR_EQUIVALENCIA
    costo_gasolina = litros_gasolina_equiv * GASOLINA_PRECIO
    ahorro = costo_gasolina - total_gnv
    pct = (ahorro / costo_gasolina * 100) if costo_gasolina > 0 else 0

    nombre = client.get("nombre") or placa
    lines = [
        f"💰 *Ahorro de {nombre}* ({placa})",
        f"",
        f"⛽ Litros GNV cargados: {litros_gnv:,.0f} LEQ",
        f"💵 Gastado en GNV: ${total_gnv:,.0f} MXN",
        f"",
        f"🔴 Si hubieras usado gasolina:",
        f"   {litros_gasolina_equiv:,.0f} litros × ${GASOLINA_PRECIO:.2f} = ${costo_gasolina:,.0f} MXN",
        f"",
        f"✅ *Ahorro total: ${ahorro:,.0f} MXN ({pct:.0f}%)*",
        f"",
        f"🌱 Además, redujiste ~{litros_gnv * 0.021:,.0f} kg de CO₂",
    ]
    return "\n".join(lines)


def handle_factura(placa: str, client: dict) -> str:
    """Handler for 'factura' — pending invoices / CFDI info."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Get recent cargas (last 10)
            cur.execute("""
                SELECT timestamp_local, station_natgas, litros, total_mxn
                FROM transactions
                WHERE placa = %s
                ORDER BY timestamp_local DESC
                LIMIT 10
            """, (placa,))
            cargas = cur.fetchall()

            # Check if RFC is registered
            cur.execute("""
                SELECT rfc FROM clients WHERE placa = %s
            """, (placa,))
            rfc_row = cur.fetchone()
    finally:
        conn.close()

    has_rfc = rfc_row and rfc_row[0] and rfc_row[0].strip()

    lines = [f"🧾 *Facturación — {placa}*", ""]

    if not has_rfc:
        lines.append("⚠️ No tienes RFC registrado.")
        lines.append("Envía: *rfc XAXX010101000* para registrarlo.")
        lines.append("Una vez registrado, podrás facturar automáticamente.")
        lines.append("")

    if cargas:
        lines.append(f"📋 Últimas {len(cargas)} cargas:")
        for c in cargas[:5]:
            fecha = c[0].strftime("%d/%m") if c[0] else "?"
            est = (c[1] or "")[:20]
            litros = float(c[2] or 0)
            total = float(c[3] or 0)
            lines.append(f"  • {fecha} | {est} | {litros:.0f}L | ${total:.0f}")
        lines.append("")
        if has_rfc:
            lines.append("📩 La facturación automática vía Odoo está en preparación.")
            lines.append("Pronto recibirás tu CFDI automáticamente después de cada carga.")
        else:
            lines.append("Registra tu RFC para activar la facturación automática.")
    else:
        lines.append("No se encontraron cargas recientes.")

    return "\n".join(lines)


def handle_saldo(placa: str, client: dict) -> str:
    """Handler for 'saldo' — CMU financial product status."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS cargas_cmu,
                    COALESCE(SUM(recaudo_valor), 0) AS total_recaudo,
                    COALESCE(AVG(recaudo_pagado), 0) AS tarifa_prom,
                    MAX(timestamp_local) AS ultima_carga
                FROM transactions
                WHERE placa = %s AND recaudo_pagado > 0
            """, (placa,))
            r = cur.fetchone()
            cargas_cmu = r[0] or 0
            total_recaudo = float(r[1] or 0)
            tarifa_prom = float(r[2] or 0)
            ultima = r[3]
    finally:
        conn.close()

    nombre = client.get("nombre") or placa
    if cargas_cmu == 0:
        return (
            f"📊 *Saldo CMU — {nombre}* ({placa})\n\n"
            f"No tienes producto financiero CMU activo.\n"
            f"Pregunta en tu estación por las opciones de ahorro y financiamiento."
        )

    lines = [
        f"📊 *Saldo CMU — {nombre}* ({placa})",
        f"",
        f"💳 Producto CMU activo",
        f"• Cargas con recaudo: {cargas_cmu:,}",
        f"• Tarifa promedio: ${tarifa_prom:.1f}/LEQ",
        f"• Total recaudado: ${total_recaudo:,.0f} MXN",
    ]
    if ultima:
        lines.append(f"• Última carga CMU: {ultima.strftime('%d/%m/%Y')}")

    lines.append("")
    lines.append("📞 Para detalle de tu producto, contacta a tu asesor CMU.")

    return "\n".join(lines)


def handle_rfc(placa: str, client: dict, message_body: str) -> str:
    """Handler for 'rfc' — query or register RFC."""
    # Check if they're providing an RFC
    parts = message_body.strip().split()
    new_rfc = None
    if len(parts) >= 2:
        candidate = parts[1].upper().strip()
        # Basic RFC validation: 12-13 alphanumeric chars
        if 12 <= len(candidate) <= 13 and candidate.isalnum():
            new_rfc = candidate

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if new_rfc:
                # Update RFC
                cur.execute("""
                    UPDATE clients SET rfc = %s WHERE placa = %s
                """, (new_rfc, placa))
                conn.commit()
                return (
                    f"✅ *RFC actualizado*\n\n"
                    f"Placa: {placa}\n"
                    f"RFC: {new_rfc}\n\n"
                    f"A partir de ahora, tus facturas se generarán con este RFC automáticamente."
                )
            else:
                # Query current RFC
                cur.execute("SELECT rfc FROM clients WHERE placa = %s", (placa,))
                row = cur.fetchone()
                current_rfc = row[0] if row and row[0] else None

                if current_rfc:
                    return (
                        f"🧾 *Tu RFC registrado:* {current_rfc}\n\n"
                        f"Para cambiarlo, envía:\n"
                        f"*rfc XAXX010101000*"
                    )
                else:
                    return (
                        f"⚠️ No tienes RFC registrado para {placa}.\n\n"
                        f"Para registrarlo, envía:\n"
                        f"*rfc XAXX010101000*\n\n"
                        f"(Reemplaza con tu RFC real de 12 o 13 caracteres)"
                    )
    finally:
        conn.close()


def handle_ayuda(placa: str, client: dict) -> str:
    """Handler for 'ayuda' — list commands + human contact."""
    nombre = client.get("nombre") or "conductor"
    return (
        f"👋 Hola *{nombre}*, soy el asistente de Central Gas.\n\n"
        f"📋 *Comandos disponibles:*\n"
        f"• *puntos* — Tu resumen de actividad y cargas\n"
        f"• *ahorro* — Cuánto has ahorrado vs gasolina\n"
        f"• *factura* — Tus cargas pendientes de facturar\n"
        f"• *saldo* — Estado de tu producto CMU\n"
        f"• *rfc* — Consultar o registrar tu RFC\n"
        f"• *ayuda* — Este menú\n\n"
        f"📞 ¿Necesitas hablar con una persona?\n"
        f"Escribe *humano* y te contactaremos."
    )


# ============================================================
# Main router
# ============================================================

KEYWORD_MAP = {
    "puntos": handle_puntos,
    "punto": handle_puntos,
    "cargas": handle_puntos,
    "ahorro": handle_ahorro,
    "ahorro": handle_ahorro,
    "ahorros": handle_ahorro,
    "factura": handle_factura,
    "facturas": handle_factura,
    "cfdi": handle_factura,
    "saldo": handle_saldo,
    "saldos": handle_saldo,
    "cmu": handle_saldo,
    "rfc": None,  # Special handler (needs full message body)
    "ayuda": handle_ayuda,
    "help": handle_ayuda,
    "hola": handle_ayuda,
    "hi": handle_ayuda,
    "menu": handle_ayuda,
}


def route_message(phone_from: str, phone_to: str, body: str, twilio_sid: str = None) -> str:
    """
    Main entry point: receive an incoming WhatsApp message and return a response.

    1. Lookup placa by phone number
    2. Parse keyword from message body
    3. Route to appropriate handler
    4. Log everything to wa_messages
    5. Return response text
    """
    ensure_wa_messages_table()

    body_clean = (body or "").strip()
    keyword_raw = body_clean.split()[0].lower() if body_clean else ""

    # Lookup client by phone
    client = lookup_placa_by_phone(phone_from)

    if not client:
        response = (
            "👋 Hola, soy el asistente de Central Gas.\n\n"
            "No encontré tu número registrado en nuestro sistema.\n"
            "Por favor, acude a tu estación Central Gas más cercana "
            "para registrar tu número y placa.\n\n"
            "📞 Si crees que es un error, escribe *humano* para contactar a un asesor."
        )
        log_message(
            direction="in", wa_from=phone_from, wa_to=phone_to,
            body=body_clean, keyword=keyword_raw,
            response=response, twilio_sid=twilio_sid, status="no_client",
        )
        return response

    placa = client["placa"]

    # Route keyword
    if keyword_raw == "rfc":
        response = handle_rfc(placa, client, body_clean)
        keyword = "rfc"
    elif keyword_raw == "humano" or keyword_raw == "persona":
        response = (
            f"📞 Entendido. Un asesor de Central Gas se pondrá en contacto contigo pronto.\n\n"
            f"Tu placa: {placa}\n"
            f"Tu nombre: {client.get('nombre', '—')}"
        )
        keyword = "humano"
    elif keyword_raw in KEYWORD_MAP:
        handler = KEYWORD_MAP[keyword_raw]
        if handler:
            response = handler(placa, client)
        else:
            response = handle_ayuda(placa, client)
        keyword = keyword_raw
    else:
        # Unrecognized — show help
        response = (
            f"🤔 No entendí \"{body_clean[:30]}\".\n\n"
            + handle_ayuda(placa, client)
        )
        keyword = "unknown"

    # Log
    log_message(
        direction="in", wa_from=phone_from, wa_to=phone_to,
        body=body_clean, placa=placa, keyword=keyword,
        response=response[:500], twilio_sid=twilio_sid, status="handled",
    )

    return response
