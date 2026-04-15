-- ============================================================
-- Central Gas Agent — Fly Postgres adapted schema
-- Auto-generated from init_db.sql (TimescaleDB stripped out)
-- ============================================================
-- DIFFERENCES vs init_db.sql:
--   1. CREATE EXTENSION timescaledb commented out (not available on Fly)
--   2. create_hypertable() call commented out (scada_readings = plain table)
--   3. Everything else identical
-- ============================================================

-- ============================================================
-- Central Gas Agent — Database Schema
-- PostgreSQL 16 + TimescaleDB
-- 7 tables + 1 materialized view
-- ============================================================

-- Enable TimescaleDB
-- TimescaleDB not available on Fly Postgres standard.
-- Re-enable when migrating to managed Postgres with TimescaleDB support.
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- 1. STATIONS — Catalogo de 3 estaciones
-- ============================================================
CREATE TABLE stations (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE,
    short_name      VARCHAR(20) NOT NULL,
    type            VARCHAR(10) NOT NULL CHECK (type IN ('hub', 'spoke')),
    address         TEXT,
    competitor      VARCHAR(100),
    lat             DECIMAL(10, 7),
    lon             DECIMAL(10, 7),
    plc_ip          VARCHAR(15),
    plc_port        INTEGER DEFAULT 102,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO stations (id, name, short_name, type, competitor, plc_ip) VALUES
    (1, 'Parques Industriales', 'PARQUES', 'hub', 'EDS Poniente, EDS Siglo XXI', NULL),
    (2, 'Oriente', 'ORIENTE', 'spoke', 'EDS Ojo Caliente', NULL),
    (3, 'Pension/Nacozari', 'NACOZARI', 'spoke', 'EDS Nacozari', '192.168.1.253');

-- Mapping de estaciones NatGas a nuestras estaciones
CREATE TABLE station_mapping (
    natgas_name     VARCHAR(100) PRIMARY KEY,
    station_id      INTEGER REFERENCES stations(id),
    notes           TEXT
);

INSERT INTO station_mapping (natgas_name, station_id, notes) VALUES
    ('EDS Nacozari', 3, 'Competidor directo Pension/Nacozari. 73.8% del volumen historico.'),
    ('EDS Siglo XXI', 1, 'Zona Parques Industriales. 9.6% del volumen.'),
    ('EDS José Maria Chávez', 1, 'Zona centro. 9.0% del volumen.'),
    ('EDS Jose Maria Chavez', 1, 'Variante sin tilde.'),
    ('EDS José María Chávez', 1, 'Variante con tildes completas.'),
    ('EDS Poniente', 1, 'Zona poniente. 5.3% del volumen.'),
    ('EDS OJO CALIENTE', 2, 'Zona oriente. 2.3% del volumen.'),
    ('EDS Ojo Caliente', 2, 'Variante mixta.');

-- ============================================================
-- 2. CLIENTS — Catalogo de clientes (331+ vagonetas)
-- ============================================================
CREATE TABLE clients (
    id              SERIAL PRIMARY KEY,
    placa           VARCHAR(20) NOT NULL UNIQUE,
    gasup_id        INTEGER,                    -- Id_placa de GasUp
    nombre          VARCHAR(200),
    telefono        VARCHAR(20),
    whatsapp        VARCHAR(20),
    email           VARCHAR(200),
    rfc             VARCHAR(13),
    modelo_vehiculo VARCHAR(20),                -- Ano modelo: 2015, 2018, etc
    fecha_conversion TIMESTAMPTZ,
    segmento        VARCHAR(30) DEFAULT 'VAGONETA', -- VAGONETA | TAXI | PARTICULAR
    tendencia       VARCHAR(20),                -- NUEVO_2025 | CRECIENDO | ESTABLE | BAJANDO | PERDIDO_2025
    estatus         VARCHAR(15) DEFAULT 'ACTIVO', -- ACTIVO | INACTIVO | PROSPECTO
    odoo_partner_id INTEGER,                    -- FK a res.partner en Odoo
    eds_principal   VARCHAR(50),                -- EDS donde mas carga
    consumo_prom_lt DECIMAL(8, 1),              -- Litros promedio mensual
    dias_sin_cargar INTEGER DEFAULT 0,
    venta_total_mxn DECIMAL(14, 2),
    notas           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_clients_placa ON clients(placa);
CREATE INDEX idx_clients_whatsapp ON clients(whatsapp);
CREATE INDEX idx_clients_tendencia ON clients(tendencia);

-- ============================================================
-- 3. TRANSACTIONS — Transacciones normalizadas del parser GasUp
-- ============================================================
CREATE TABLE transactions (
    id              BIGSERIAL PRIMARY KEY,
    source_file     VARCHAR(200),               -- Nombre del CSV origen
    source_hash     VARCHAR(64),                -- SHA256 del archivo para dedup
    source_row      INTEGER,                    -- Linea en el CSV original
    schema_version  VARCHAR(10) NOT NULL,       -- 'pre2023' o 'post2023'

    -- Datos originales normalizados
    station_id      INTEGER REFERENCES stations(id),
    station_natgas  VARCHAR(100),               -- Nombre original de la EDS en GasUp
    plaza           VARCHAR(50) DEFAULT 'AGUASCALIENTES',
    timestamp_utc   TIMESTAMPTZ NOT NULL,       -- Fecha/hora normalizada a UTC
    timestamp_local TIMESTAMPTZ NOT NULL,       -- Fecha/hora original CST (UTC-6)

    -- Vehiculo
    gasup_placa_id  INTEGER,                    -- Id_placa de GasUp
    placa           VARCHAR(20) NOT NULL,
    modelo          VARCHAR(20),                -- Ano modelo
    marca           VARCHAR(50),
    linea           VARCHAR(50),
    fecha_conversion TIMESTAMPTZ,

    -- Transaccion
    litros          DECIMAL(10, 4) NOT NULL,
    pvp             DECIMAL(8, 2) NOT NULL,     -- Precio por litro
    total_mxn       DECIMAL(12, 2) NOT NULL,    -- Total cobrado
    recaudo_valor   DECIMAL(10, 2) DEFAULT 0,   -- Valor recaudo (pre-2023)
    recaudo_pagado  DECIMAL(10, 2) DEFAULT 0,   -- Recaudo pagado (pre-2023 y post-2023)
    venta_mas_recaudo DECIMAL(12, 2),           -- Venta total + recaudo (pre-2023)
    medio_pago      VARCHAR(30),                -- EFECTIVO | PREPAGO | CREDITO | TARJETA_DEBITO | TARJETA_CREDITO | BONOS_EDS (NULL post-2023)
    segmento        VARCHAR(30),                -- Combis Colectivas | Vagonetas AGS (NULL post-2023)

    -- Campos derivados
    kg              DECIMAL(10, 4),             -- litros * 0.717
    nm3             DECIMAL(10, 4),             -- litros * 1.0 (en GasData 1lt = 1Nm3)
    ingreso_neto    DECIMAL(12, 2),             -- total_mxn / 1.16
    iva             DECIMAL(12, 2),             -- total_mxn - ingreso_neto

    -- Reconciliacion
    odoo_move_id    INTEGER,                    -- FK a account.move en Odoo. NULL si pendiente.
    reconciled      BOOLEAN DEFAULT FALSE,
    anomalies       JSONB DEFAULT '[]',         -- Array de anomalias detectadas

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indices para queries frecuentes
CREATE INDEX idx_txn_timestamp ON transactions(timestamp_utc);
CREATE INDEX idx_txn_station_date ON transactions(station_id, timestamp_utc);
CREATE INDEX idx_txn_placa ON transactions(placa);
CREATE INDEX idx_txn_placa_date ON transactions(placa, timestamp_utc);
CREATE INDEX idx_txn_source_hash ON transactions(source_hash);
-- Unique key for idempotent bulk inserts (used by ON CONFLICT in app/db/transactions.py)
CREATE UNIQUE INDEX uq_txn_source_hash_row ON transactions(source_hash, source_row);
CREATE INDEX idx_txn_reconciled ON transactions(reconciled) WHERE reconciled = FALSE;
CREATE INDEX idx_txn_anomalies ON transactions USING GIN (anomalies);

-- ============================================================
-- 4. SCADA_READINGS — Telemetria SCADA (TimescaleDB hypertable)
-- ============================================================
CREATE TABLE scada_readings (
    id              BIGSERIAL,
    station_id      INTEGER NOT NULL REFERENCES stations(id),
    source          VARCHAR(15) NOT NULL,       -- COMPRESSOR | DISPENSER
    variable        VARCHAR(30) NOT NULL,       -- P_entrada, T_aceite, TOTALIZER_QTY, etc
    register_addr   VARCHAR(15),                -- Direccion real: VW1034 / 40024 / M17.0
    raw_value       INTEGER NOT NULL,           -- Valor crudo del PLC/Modbus
    scaled_value    DECIMAL(12, 4) NOT NULL,    -- Valor con escala: raw * 0.01 (MPa) o * 0.1 (°C)
    unit            VARCHAR(10),                -- MPa, °C, cm, L, bar
    quality         SMALLINT DEFAULT 0,         -- 0=OK, 1=suspect, 2=bad
    timestamp_utc   TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable (partitioned by month)
-- TimescaleDB hypertable conversion disabled on Fly Postgres.
-- scada_readings remains a plain partitioned table for now.
-- Add manual partitioning by month when SCADA volume justifies it.
-- SELECT create_hypertable('scada_readings', 'timestamp_utc',
--     chunk_time_interval => INTERVAL '1 month',
--     if_not_exists => TRUE
-- );

CREATE INDEX idx_scada_station_var ON scada_readings(station_id, variable, timestamp_utc DESC);

-- ============================================================
-- 5. RECONCILIATION_RUNS — Resultado de cada reconciliacion diaria
-- ============================================================
CREATE TABLE reconciliation_runs (
    id              SERIAL PRIMARY KEY,
    station_id      INTEGER NOT NULL REFERENCES stations(id),
    run_date        DATE NOT NULL,
    status          VARCHAR(10) NOT NULL CHECK (status IN ('OK', 'WARNING', 'CRITICAL', 'EMERGENCY', 'PARTIAL')),

    -- Fuentes
    source_a_litros DECIMAL(12, 2),             -- Total GasUp litros vendidos
    source_a_mxn    DECIMAL(14, 2),             -- Total GasUp $ ventas
    source_c_nm3    DECIMAL(12, 2),             -- Delta totalizador SCADA (Nm3 comprimidos)
    source_d_mxn    DECIMAL(14, 2),             -- Total asientos Odoo ($)
    source_b_kg     DECIMAL(12, 3),             -- FUTURO: Total dispensario directo (kg). NULL.

    -- Deltas
    delta_a_vs_c_pct DECIMAL(6, 3),             -- (A - C) / A * 100
    delta_a_vs_d_mxn DECIMAL(12, 2),            -- A$ - D$

    -- Detalle
    checks_json     JSONB NOT NULL DEFAULT '{}', -- Resultado detallado de los 5-7 checks
    discrepancies   JSONB DEFAULT '[]',          -- Array de discrepancias
    resolved        BOOLEAN DEFAULT FALSE,
    resolution_note TEXT,

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(station_id, run_date)
);

-- ============================================================
-- 6. ALERTS — Historial de alertas emitidas
-- ============================================================
CREATE TABLE alerts (
    id              SERIAL PRIMARY KEY,
    station_id      INTEGER NOT NULL REFERENCES stations(id),
    source          VARCHAR(20) NOT NULL,       -- SCADA | RECONCILIATION | HEARTBEAT | SYSTEM | RETENTION
    severity        VARCHAR(10) NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL', 'EMERGENCY')),
    variable        VARCHAR(30),                -- Que variable disparo la alerta
    register_addr   VARCHAR(15),                -- M17.0, VW1034, etc
    value           DECIMAL(12, 4),             -- Valor que excedio el umbral
    threshold       DECIMAL(12, 4),             -- Umbral configurado
    message         TEXT NOT NULL,              -- Mensaje enviado por WhatsApp
    whatsapp_sent   BOOLEAN DEFAULT FALSE,
    whatsapp_sid    VARCHAR(50),                -- Twilio message SID
    acknowledged    BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    resolved        BOOLEAN DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    cooldown_until  TIMESTAMPTZ,                -- No re-alertar hasta esta hora
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_alerts_active ON alerts(station_id, variable, severity)
    WHERE resolved = FALSE;
CREATE INDEX idx_alerts_date ON alerts(created_at DESC);

-- ============================================================
-- 7. MAT_INVENTORY — Estado de cada MAT en el sistema
-- ============================================================
CREATE TABLE mat_inventory (
    mat_id          VARCHAR(10) PRIMARY KEY,     -- MAT-001, MAT-002, etc
    current_station INTEGER REFERENCES stations(id),
    status          VARCHAR(15) NOT NULL DEFAULT 'FULL'
                    CHECK (status IN ('FULL', 'IN_USE', 'DEPLETED', 'IN_TRANSIT', 'CHARGING')),
    pressure_bar    DECIMAL(6, 1),               -- Ultima presion conocida (SCADA)
    capacity_nm3    DECIMAL(10, 1) DEFAULT 9261, -- LUXI 12-tube: 9,261 Nm3
    estimated_remaining_nm3 DECIMAL(8, 1),
    estimated_autonomy_hours DECIMAL(6, 1),
    last_swap_at    TIMESTAMPTZ,
    last_charge_at  TIMESTAMPTZ,
    cycle_count     INTEGER DEFAULT 0,
    notes           TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed MATs (4 MATs: 2 por spoke)
INSERT INTO mat_inventory (mat_id, current_station, status) VALUES
    ('MAT-001', 2, 'FULL'),     -- Oriente, activo
    ('MAT-002', 2, 'FULL'),     -- Oriente, respaldo
    ('MAT-003', 3, 'FULL'),     -- Nacozari, activo
    ('MAT-004', 3, 'FULL');     -- Nacozari, respaldo

-- ============================================================
-- 8. DAILY_CLOSE — Vista materializada de cierre diario
-- ============================================================
CREATE MATERIALIZED VIEW daily_close AS
SELECT
    t.station_id,
    DATE(t.timestamp_local) AS close_date,
    COUNT(*)                AS total_cargas,
    COUNT(DISTINCT t.placa) AS unique_placas,
    SUM(t.litros)           AS total_litros,
    SUM(t.kg)               AS total_kg,
    SUM(t.nm3)              AS total_nm3,
    SUM(t.total_mxn)        AS total_mxn,
    SUM(t.ingreso_neto)     AS total_neto,
    SUM(t.iva)              AS total_iva,
    AVG(t.pvp)              AS avg_pvp,
    AVG(t.litros)           AS avg_litros_per_carga,
    r.status                AS reconc_status,
    COUNT(*) FILTER (WHERE jsonb_array_length(t.anomalies) > 0) AS anomaly_count
FROM transactions t
LEFT JOIN reconciliation_runs r
    ON r.station_id = t.station_id
    AND r.run_date = DATE(t.timestamp_local)
GROUP BY t.station_id, DATE(t.timestamp_local), r.status
WITH NO DATA;

CREATE UNIQUE INDEX idx_daily_close_pk ON daily_close(station_id, close_date);

-- ============================================================
-- File dedup tracking
-- ============================================================
CREATE TABLE processed_files (
    id              SERIAL PRIMARY KEY,
    file_name       VARCHAR(200) NOT NULL,
    file_hash       VARCHAR(64) NOT NULL UNIQUE,
    schema_version  VARCHAR(10) NOT NULL,
    row_count       INTEGER NOT NULL,
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CRE daily reports tracking
-- ============================================================
CREATE TABLE cre_reports (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL UNIQUE,
    total_nm3       DECIMAL(12, 2),
    stations_data   JSONB,
    email_sent      BOOLEAN DEFAULT FALSE,
    email_sent_at   TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
