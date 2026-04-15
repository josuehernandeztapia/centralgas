# Central Gas Dashboard — TO-BE Design + MVP Roadmap

## Audiencia

3 perfiles distintos:
1. **Tú (Josue)** — dueño, monitoreo estratégico, decisiones comerciales
2. **Operadores EDS** (Q3 2026) — día a día, upload archivos, ver su estación
3. **Demo/inversores/Consware/NatGas** — pantalla bonita que cuente la historia

Decisión arquitectónica: UNA sola app con diferentes vistas según rol (cuando agreguemos auth).
Por ahora, todo público bajo la misma URL.

---

## TO-BE Dashboard (visión completa, 6 niveles)

### Nivel 1: Executive Summary (home)
**Audiencia: tú + inversores**

- **KPI Cards** (hoy vs ayer vs mes): Ventas MXN, Litros, Sobreprecio capturado, Clientes únicos
- **Estatus Cierre Diario** por EDS: 🟢 OK / 🟡 Pendiente / 🔴 Discrepancia
- **Alertas Activas**: retención, SCADA, cumplimiento regulatorio
- **Mapa de 3 EDS** con indicador de salud en vivo

### Nivel 2: Operaciones
**Audiencia: operadores EDS + tú**

- **Por EDS** (ECG-01 Nacozari, ECG-02 Ojo Caliente, ECG-03 Peñuelas):
  - Volumen diario/mensual
  - Top 10 placas del día
  - Utilización (cargas/hora, hora pico)
  - Revenue + sobreprecio capturado
- **Cierre Diario**: tabla `reconciliation_runs` por estación+fecha, delta GasUp vs Odoo
- **SCADA vs GasUp**: diferencial litros reportados vs Nm3 comprimidos según telemetría (detecta robos/fugas/error medición)
- **MAT Inventory**: estado de 4 MATs (FULL/IN_USE/DEPLETED/CHARGING), autonomía estimada
- **Anomalías del día**: cargas >100L, montos negativos, placas desconocidas, alta frecuencia

### Nivel 3: Comercial / Retención
**Audiencia: tú + equipo comercial**

- **Clientes**: total placas activas, nuevas del mes, perdidas del mes
- **Retención**:
  - Placas con >7 días sin cargar (alert)
  - Placas con >30 días sin cargar Y saldo en Ahorro Renovación (CRÍTICO — exposición financiera)
  - Churn rate por segmento
- **Segmentación**:
  - Por segmento (vagoneta, taxi, combi, particular): count, avg litros/carga, avg ticket
  - Heatmap día/hora por segmento
- **"Las que robamos"**: placas que en histórico NatGas cargaban y en últimas N semanas solo cargan en Central Gas (market share gain)
- **Sobreprecio y Financieras**:
  - Distribución sobreprecio por financiera (28 financieras conocidas en GasUp)
  - Top financieras por volumen
  - Placas sin financiera asignada (potenciales prospectos)

### Nivel 4: Productos Financieros
**Audiencia: tú + equipo CMU/Conductores**

- **Ahorro Renovación**:
  - Participantes activos con saldo acumulado
  - Ritmo de acumulación (recaudo/semana)
  - 🚨 Alertas: "Fulano tiene $1,500 acumulado y no carga hace 10 días"
  - Proyección fecha de renovación por cliente
- **TANDA**:
  - Participantes, orden de turnos, contribuciones
  - Próximos pagos programados
- **Conversión GNV**:
  - Pipeline: prospectos → evaluación → conversión → entrega
  - Clientes en curso (Elvira Flores, etc.)
- **TSR (Time-Share Rental / Arrendamiento)**:
  - Flota rentada por modelo (Aveo2022, March Advance/Sense 2021)
  - Ingresos por unidad
  - Aging de rentas (días sin pago)
  - NAFE portfolio status

### Nivel 5: Regulatorio
**Audiencia: Josue + abogada + responsable HSE**

- **CRE**: status reporte diario enviado (🟢/🔴), últimos 30 días
- **SEMARNAT**: ERA/EVIS/MIA/MTD por estación, próximos vencimientos
- **PIPC Protección Civil**: status por EDS
- **SASISOPA**: checklist de cumplimiento
- **Calendario de vencimientos** con alertas proactivas

### Nivel 6: Finanzas (integración Odoo)
**Audiencia: tú + contador**

- **Estado de Resultados** (último mes vs presupuesto)
- **Balance** (principales cuentas)
- **Cuentas por Cobrar CMU** (saldo intercompañía)
- **Cash flow** (entradas/salidas del mes)
- **Covenants bancarios** (cuando haya deuda)

---

## MVP v1 — Lo que construimos HOY (2-3 hrs)

**Constraint**: solo datos disponibles en Neon ahora (31,016 ventas, sin clientes, sin SCADA, sin productos CMU indexados).

### Página única React single-file con 3 tabs:

#### Tab 1: Resumen
- **4 KPI cards**: Total transacciones, Total litros, Total revenue MXN, Total sobreprecio
- **Gráfica línea**: Ventas últimos 30 días (litros + MXN dual axis)
- **Gráfica barras**: Top 10 estaciones por volumen
- **Última actualización** (timestamp del último upload)

#### Tab 2: Transacciones
- **Tabla paginada** (50 rows/página): fecha, estación, placa, litros, pvp, total, medio_pago, sobreprecio
- **Filtros**: estación (dropdown), placa (input), rango de fechas
- **Orden**: por timestamp DESC
- **Export CSV** (bonus si da tiempo)

#### Tab 3: Sobreprecio
- **Stats box**: min, max, mean, median, p25/75/90/95/99, stddev
- **Histograma**: distribución con 12 buckets
- **Top financieras** (cuando tengamos campo — por ahora placeholder)

**Stack**: React 18 + Tailwind + recharts + lucide-react, todo en un single HTML/JSX file, servido desde FastAPI bajo `/dashboard` (static file).

**Hosting**: misma app Fly, misma URL. Cero infraestructura nueva.
**Auth**: ninguna (Phase 1 interno). Obscurity de la URL.

---

## Roadmap incremental post-MVP

### v2 (próxima sesión, 3-4 hrs)
**Objetivo: profundidad operacional**
- Vista por estación (drill-down): detalle de ECG-01, ECG-02, ECG-03
- Vista por placa: historial de cargas, tendencia, última ubicación
- Vista de segmentos (una vez clasifiquemos placas en DB)
- Upload drag-drop para .xls (reemplaza curl manual)

### v3 (cuando importemos productos financieros, 4-6 hrs)
**Objetivo: cruce comercial con operaciones**
- Importar catálogo de clientes CMU (Ahorro, TANDA, TSR, Conversión GNV) a tabla `clients`
- Vista "Productos Financieros" con saldos y participantes
- Cross-reference placas ↔ productos financieros
- Alertas de retención básicas:
  - Placa sin cargar >7 días
  - Placa con saldo Ahorro >$500 y sin cargar >10 días (CRÍTICO)
- "Las que robamos" (placas en histórico NatGas ahora solo en Central Gas)

### v4 (cuando arranque wrapper + WhatsApp en Fase 2, 6-8 hrs)
**Objetivo: dashboard en tiempo real**
- Live feed de transacciones (websocket o polling)
- Alertas push al dashboard + WhatsApp
- Cierre diario automatizado
- Anomalías en tiempo real

### v5 (cuando SCADA esté vivo en Fase 3)
**Objetivo: reconciliation engine**
- Telemetría SCADA en vivo (compresores, surtidores, presiones, niveles)
- Diferencial GasUp vs SCADA (detección de robos/fugas)
- MAT inventory dinámico
- Reconciliation runs automáticos

### v6 (cuando Odoo Central Gas timbre CFDIs, Fase 2+)
**Objetivo: integración contable**
- Estados financieros desde Odoo
- CxC intercompañía CMU
- Balance de cada EDS
- Margen por estación, por segmento

---

## Qué NO construimos en MVP (y por qué)

| Feature | Por qué no ahora | Depende de |
|---|---|---|
| Cierre diario | Tabla `reconciliation_runs` vacía | Wrapper Phase 2 + Odoo |
| SCADA dashboards | Tabla `scada_readings` vacía | HU-4.1 snap7 connector (Fase 3) |
| Alertas productos CMU | No tenemos catálogo de clientes en DB | v3 (importar desde AGS/) |
| "Las que robamos" | Requiere histórico NatGas + actual Central Gas cruzados | Central Gas operando (Q3 2026) |
| Reportes regulatorios | No están construidos | HU-8.1/8.2/8.3 (Fase 4) |
| Odoo integration | Central Gas aún no tiene CSD ni CFDIs | Pendiente compra CSD |

---

## Decisiones de diseño

1. **React single-file vs Next.js separado**: single-file. Razón: velocidad de iteración, sin otro repo, sin CI aparte, hosting gratis en mismo Fly.
2. **Tailwind vs CSS custom**: Tailwind. Razón: diseño consistente sin reinventar.
3. **Recharts vs Chart.js vs D3**: Recharts. Razón: componentes React idiomáticos, suficiente para las gráficas que necesitamos.
4. **Sin auth en v1**: acepto el riesgo por obscurity mientras Phase 1 es solo para mí. Al primer operador real → agregar auth (básica password via header, o Neon Auth que ya vimos).
5. **Nombres de estación en DB vs en CoA**: el dashboard muestra lo que está en la DB (EDS Nacozari, Siglo XXI, etc. — names de NatGas). Cuando unifiquemos con ECG-01/02/03 del catálogo de cuentas, el mapping se hace en `station_mapping`.

---

## Siguiente paso

Construir **MVP v1** → dashboard-single-file.jsx → servido en `https://central-gas-agent.fly.dev/dashboard`.

Commitear + push → GH Actions deploya automáticamente.
