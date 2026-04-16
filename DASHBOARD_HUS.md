# Central Gas Dashboard — User Stories (Historias de Usuario)

Roadmap incremental post-MVP v1. Cada HU incluye:
- **Rol** → quien lo usa
- **Historia** → qué quiere y para qué
- **Criterios de aceptación** → cómo sabemos que está hecho
- **Dependencias** → qué necesita antes
- **Esfuerzo** → estimación en horas

Codificación: `HU-DASH-{iteración}.{número}`

---

## ✅ v1 — MVP (completado)

- `/health`, `/`, `/upload-excel`, `/transactions`, `/stats`, `/sobreprecio/distribution`
- Dashboard React single-file con 3 tabs (Resumen / Transacciones / Sobreprecio)
- Rebrand con look & feel landing (dark navy + cyan/verde + Rajdhani)
- Logo constelación animado
- 31,016 filas reales persistidas en Neon Postgres
- CI/CD GitHub Actions → Fly.io

---

## v2 — Profundidad operacional (siguiente iteración, 4-6 hrs)

**Objetivo**: Convertir el dashboard actual de "ver agregados" a "investigar casos específicos".

### HU-DASH-2.1 — Drill-down por estación
**Rol**: Yo (Josue) + operador EDS
**Historia**: Como operador, quiero hacer click en una estación del Top 10 para ver su detalle (volumen diario, top placas del mes, horarios pico, utilización promedio) y entender su comportamiento.
**Criterios**:
- Click en barra del chart Top 10 → abre modal o navega a `/dashboard/estacion/:id`
- Vista por estación con: volumen/mes últimos 12 meses (línea), top 20 placas, heatmap día/hora, lista de anomalías
- Back a vista general preservando filtros
**Dependencias**: endpoint `/stations/:id/detail` nuevo
**Esfuerzo**: 3 hrs

### HU-DASH-2.2 — Drill-down por placa
**Rol**: Equipo comercial / yo
**Historia**: Como comercial, quiero ver el historial completo de una placa (todas sus cargas, estaciones visitadas, tendencia de volumen) para entender si es un cliente fiel, si está bajando, o si migró a competencia.
**Criterios**:
- Click en placa en tabla → abre detalle
- Muestra: timeline de cargas, total lifetime, litros/mes últimos 12 meses, estación preferida, medio pago dominante, alertas si >7 días sin cargar
- Link directo vía URL `/dashboard/placa/:plate`
**Dependencias**: endpoint `/placas/:placa/historial`
**Esfuerzo**: 3 hrs

### HU-DASH-2.3 — Upload drag-drop de .xls
**Rol**: Yo + futuros operadores
**Historia**: Como usuario no-técnico, quiero arrastrar un archivo .xls a una zona en el dashboard y que lo ingeste, sin tener que usar curl ni la terminal.
**Criterios**:
- Zona drag-drop visible en una pestaña "Upload" o como modal desde botón "+"
- Preview del archivo antes de confirmar (filename, tamaño, formato detectado)
- Selector de `report_type` (ventas_detalladas / recaudos_financiera)
- Progress bar durante upload
- Feedback claro post-upload: filas parseadas, insertadas, warnings
- Manejo de errores visibles (archivo corrupto, demasiado grande)
**Dependencias**: ninguna (endpoint ya existe)
**Esfuerzo**: 2 hrs

### HU-DASH-2.4 — Export CSV de transacciones
**Rol**: Analista / contabilidad
**Historia**: Como analista, quiero exportar los resultados filtrados a CSV para trabajarlos en Excel / mandar a contabilidad.
**Criterios**:
- Botón "Export CSV" en la vista de transacciones
- Exporta TODAS las filas que cumplen los filtros (no solo la página visible), con confirmación si son >10,000
- CSV válido UTF-8 con BOM (Excel lo abre bien)
- Incluye headers, encoding mexicano (acentos OK), monto con punto decimal
**Dependencias**: endpoint `/transactions/export` con stream CSV
**Esfuerzo**: 2 hrs

### HU-DASH-2.5 — Búsqueda global
**Rol**: Todos
**Historia**: Como usuario, quiero un search bar en el header que busque placas, estaciones, tickets y me lleve directo al detalle.
**Criterios**:
- Cmd/Ctrl+K abre búsqueda
- Autocomplete mientras escribo
- Resultados agrupados por tipo (placa, estación, ticket)
- Enter navega al primer resultado
**Dependencias**: endpoint `/search?q=` con ranking
**Esfuerzo**: 3 hrs

---

## v3 — Comercial + productos financieros (4-6 hrs)

**Objetivo**: Cruzar las transacciones operativas con los productos financieros de CMU (Ahorro Renovación, TANDA, TSR, Conversión GNV) para detectar retención y exposure.

### HU-DASH-3.1 — Importar catálogo de clientes CMU
**Rol**: Backend (prerequisito)
**Historia**: Como sistema, necesito el catálogo de participantes de cada producto CMU (placa, nombre, producto, saldo acumulado) importado a tabla `clients` para cruzar con transactions.
**Criterios**:
- Script `scripts/import_cmu_clients.py` que lee archivos de `AGS/Productos_Financieros/` (reportes semanales recaudo, carpetas Participantes)
- Parsea .xlsx semanales (S12.26, S13.26, etc.) + .docx de participantes individuales
- Popula `clients` table con: placa, nombre, producto (AHORRO/TANDA/TSR/CONVERSION), saldo, fecha_alta
- Idempotente: re-corre sin duplicar
- Log de qué se importó y qué se saltó
**Dependencias**: archivos en AGS/ (ya existen)
**Esfuerzo**: 3 hrs

### HU-DASH-3.2 — Vista "Productos Financieros"
**Rol**: Yo + equipo comercial
**Historia**: Como responsable comercial, quiero una pestaña que muestre los 4 productos CMU con sus participantes, saldos y status, para saber dónde invertimos mejor y quiénes son mis clientes.
**Criterios**:
- Nueva tab "Comercial"
- 4 secciones (Ahorro Renovación / TANDA / TSR / Conversión GNV)
- Cada sección: número de participantes activos, saldo total acumulado, top 5 por saldo, último recaudo recibido
- Click en participante → detalle (drill-down tipo HU-DASH-2.2)
**Dependencias**: HU-DASH-3.1 completada
**Esfuerzo**: 3 hrs

### HU-DASH-3.3 — Alertas de retención básicas
**Rol**: Equipo retención
**Historia**: Como equipo de retención, quiero ver una lista de placas activas que llevan >7 días sin cargar, priorizadas por volumen histórico, para contactarlas.
**Criterios**:
- Nueva sección "Alertas" en Resumen o tab dedicado
- Cálculo: placas con al menos 1 carga en últimos 90 días Y sin carga en últimos 7 días
- Ordenadas por litros promedio mensual DESC (las más valiosas primero)
- Columnas: placa, días sin cargar, volumen prom/mes, última estación, contacto (si tenemos en `clients`)
- Export CSV para pasar a WhatsApp blast
**Dependencias**: ninguna (usa solo `transactions`)
**Esfuerzo**: 2 hrs

### HU-DASH-3.4 — Alertas críticas de exposure financiero ⚠️
**Rol**: Yo + CMU finanzas
**Historia**: Como socio de CMU, quiero ver alertas cuando una placa tiene saldo acumulado en Ahorro Renovación PERO lleva >10 días sin cargar, porque representa riesgo de default / pérdida del cliente.
**Criterios**:
- Cruce `transactions` ↔ `clients` (producto=AHORRO, saldo>500)
- Badge CRÍTICO cuando saldo>$500 Y días_sin_cargar>10
- Badge WARNING cuando saldo>$200 Y días_sin_cargar>7
- Columnas: placa, nombre, saldo Ahorro, días sin cargar, último monto cargado
- Ordena por saldo DESC (la exposure más grande primero)
- Link directo a WhatsApp del cliente (cuando tengamos teléfono en `clients`)
**Dependencias**: HU-DASH-3.1 (catálogo CMU)
**Esfuerzo**: 3 hrs

### HU-DASH-3.5 — "Las que robamos" (market share migration)
**Rol**: Yo + inversores
**Historia**: Como socio, quiero ver placas que históricamente cargaban en NatGas y ahora cargan principalmente en Central Gas, para medir market share gain y celebrar victorias.
**Criterios**:
- Reporte que compara placas vistas en últimos N meses (histórico NatGas) vs últimas 4 semanas (nuevo Central Gas)
- Identifica placas con >70% de sus últimas 10 cargas en estaciones Central Gas (cuando hayamos definido cuáles son "nuestras")
- Vista de líneas: "migradas totalmente", "migrando", "compartidas 50/50", "leales a competencia"
- KPI grande: cuántas placas migraron este mes
**Dependencias**: operación real de Central Gas iniciada, mapping de estaciones "propias"
**Esfuerzo**: 4 hrs (complejo el análisis)

### HU-DASH-3.6 — Segmentación por tipo de vehículo
**Rol**: Analista
**Historia**: Como analista, quiero filtrar transacciones por segmento (vagoneta, taxi, combi, particular) para entender patrones por tipo de transporte.
**Criterios**:
- Clasificador de placas → segmento (por regex, por tabla de mapeo, o enriquecido desde GasUp)
- Filtro en tab Transacciones
- KPIs desglosados en Resumen por segmento
- Grafica de volumen por segmento
**Dependencias**: clasificación/enriquecimiento en `clients` o `transactions.segmento`
**Esfuerzo**: 3 hrs

---

## v4 — Real-time + alertas (cuando wrapper/WhatsApp activos, 6-8 hrs)

**Objetivo**: El dashboard deja de ser "consulta manual" para ser "centro de operaciones en vivo".

### HU-DASH-4.1 — Live feed de transacciones
**Rol**: Operador EDS
**Historia**: Como operador, quiero ver las transacciones entrar en tiempo real (<30s de latencia) para reaccionar rápido si algo no cuadra.
**Criterios**:
- WebSocket endpoint o polling cada 10s
- Nueva tab "Live" o widget en Resumen
- Row animation cuando llega una nueva transacción
- Pause/resume para no marear
**Dependencias**: wrapper Phase 2 ingestando
**Esfuerzo**: 4 hrs

### HU-DASH-4.2 — Panel de alertas activas
**Rol**: Operador + yo
**Historia**: Como operador, quiero un panel centralizado de alertas (SCADA, reconciliation, retention, system) con severidad, acción sugerida y botón de "reconocer".
**Criterios**:
- Nueva tab "Alertas"
- Filtros por severidad (INFO/WARNING/CRITICAL/EMERGENCY)
- Filtros por fuente (SCADA/RECONCILIATION/RETENTION/SYSTEM)
- Badge counter en header con total activas
- Acknowledge / resolve con nota
- Histórico (alertas resueltas últimos 30 días)
**Dependencias**: tabla `alerts` siendo populada
**Esfuerzo**: 3 hrs

### HU-DASH-4.3 — Integración WhatsApp desde UI
**Rol**: Yo + retención
**Historia**: Como usuario, quiero un botón "Contactar por WhatsApp" en alertas y en detalle de cliente que mande mensaje predefinido vía Twilio.
**Criterios**:
- Templates definidos (retención, alerta ahorro, felicitaciones por cumpleaños, etc.)
- Botón con preview del mensaje
- Confirmación antes de enviar
- Log en DB de mensajes enviados
- Respuestas visibles (si hay webhook entrante)
**Dependencias**: Twilio configurado + tabla `clients` con whatsapp
**Esfuerzo**: 3 hrs

### HU-DASH-4.4 — Cierre diario automatizado
**Rol**: Yo + contabilidad
**Historia**: Como responsable de cierre, quiero que cada noche (23:00) el sistema haga reconciliation automática (GasUp vs Odoo vs SCADA) y me despierte si hay discrepancias >threshold.
**Criterios**:
- Worker schedule a las 23:00 CST
- Compara: total GasUp vs total Odoo cierre vs total SCADA compression
- Delta aceptable: <0.5% (configurable)
- Escribe a `reconciliation_runs`
- Si OK: WhatsApp informativo "cierre OK", si WARNING/CRITICAL: alerta con detalle
- Vista en dashboard con histórico de cierres
**Dependencias**: Odoo integrado + SCADA activo
**Esfuerzo**: 6 hrs

### HU-DASH-4.5 — Anomalías en tiempo real
**Rol**: Operador
**Historia**: Como operador, quiero que se me notifique inmediatamente cuando hay una carga anómala (litros>100, monto negativo, placa desconocida, frecuencia alta).
**Criterios**:
- Detector corriendo en ingesta (ya existe la lógica `anomalies` JSONB)
- Push al dashboard vía WebSocket
- Notificación del navegador (Notification API)
- Log en `alerts`
- Dashboard muestra conteo del día
**Dependencias**: HU-DASH-4.1 (live feed) y 4.2 (alertas)
**Esfuerzo**: 3 hrs

---

## v5 — SCADA integration (cuando HU-4.1/4.2 listas, 8-10 hrs)

**Objetivo**: Ver la operación física (compresores, surtidores, presiones, niveles) en vivo desde el dashboard.

### HU-DASH-5.1 — Telemetría SCADA en vivo
**Rol**: Operador + yo
**Historia**: Como operador, quiero ver en el dashboard las lecturas de cada EDS (presión entrada/salida, temperaturas aceite/motor compresor, totalizador, alarmas) en tiempo real.
**Criterios**:
- Nueva tab "SCADA" o widget por estación
- Gauges visuales (presión, temperatura, flujo)
- Historia últimas 24h de cada variable
- Alarmas activas del PLC visible
- Código de colores por status
**Dependencias**: HU-4.1/4.2 snap7 connector + MQTT + tabla `scada_readings` poblada
**Esfuerzo**: 5 hrs

### HU-DASH-5.2 — Diferencial GasUp vs SCADA
**Rol**: Yo + auditor
**Historia**: Como socio, quiero ver el delta entre lo que reporta GasUp (litros vendidos) y lo que comprimió SCADA (Nm3), por estación y día, para detectar robo/fuga/error.
**Criterios**:
- Vista diaria por EDS: GasUp_LEQ vs SCADA_Nm3
- Delta absoluto y %
- Flag si delta > threshold (configurable, default 2%)
- Gráfica acumulada del mes
**Dependencias**: HU-5.1 + reconciliation runs
**Esfuerzo**: 3 hrs

### HU-DASH-5.3 — MAT inventory dinámico
**Rol**: Logística + operadores
**Historia**: Como encargado de logística, quiero ver en dashboard el estado de los 4 MATs (FULL/IN_USE/DEPLETED/CHARGING), su ubicación actual, autonomía estimada, para planear swaps.
**Criterios**:
- Widget en Resumen o tab "Logística"
- 4 cards (1 por MAT) con estado actual
- Trayectoria últimos 7 días (qué EDS visitó, cuántas horas ahí)
- Predicción de autonomía (Nm3 restantes / tasa consumo)
- Alertas si MAT va a <10% y no hay swap planeado
**Dependencias**: SCADA alimentando pressure/level + modelo de consumo
**Esfuerzo**: 4 hrs

---

## v6 — Finanzas / Odoo integration (4-6 hrs)

**Objetivo**: Ver el negocio en números contables, no solo operativos.

### HU-DASH-6.1 — Estado de Resultados del mes
**Rol**: Yo + contador
**Historia**: Como socio, quiero ver el P&L del mes corriente vs presupuesto directo en el dashboard, sin tener que entrar a Odoo.
**Criterios**:
- Tab "Finanzas"
- Sección P&L: Ingresos, Costos, Gastos, Utilidad, por mes últimos 12
- Columnas: real, presupuesto, variación $, variación %
- Drill-down a cuenta contable cuando relevante
- Export PDF
**Dependencias**: Odoo Central Gas en operación + API integration
**Esfuerzo**: 4 hrs

### HU-DASH-6.2 — CxC / CxP CMU (comercial, no intercompañía)
**Rol**: Yo + CMU finanzas
**Historia**: Como socio, quiero ver saldos comerciales con CMU (lo que Central Gas le debe/cobra) en tiempo real.
**Criterios**:
- Widget en Finanzas
- Saldo CxC CMU (lo que CMU nos debe por cobranza de sobreprecio)
- Saldo CxP CMU (lo que debemos a CMU)
- Aging: 0-30, 31-60, 61-90, >90 días
- Link a conciliación
**Dependencias**: HU-6.1 + Odoo intercompañía configurado
**Esfuerzo**: 3 hrs

### HU-DASH-6.3 — Margen por estación y segmento
**Rol**: Yo + operaciones
**Historia**: Como socio, quiero saber cuál de las 3 EDS es más rentable, y cuál segmento (vagoneta/taxi/combi) deja más margen.
**Criterios**:
- Cruce de ventas (transactions) con costos (Odoo gastos operativos por estación)
- Margen bruto, operativo, neto por EDS
- Ranking por margen
- Heatmap EDS × segmento
**Dependencias**: HU-6.1 + clasificación segmentos (HU-3.6)
**Esfuerzo**: 4 hrs

---

## Cross-cutting (any time, prioridad variable)

### HU-DASH-X.1 — Autenticación básica ⚠️
**Rol**: Todos
**Historia**: Como dueño, quiero que el dashboard pida login antes de mostrar info (ahora está público bajo obscurity).
**Criterios**:
- Password único compartido vía HTTP Basic Auth, O
- Magic link email, O
- OAuth con Google (mismo que Neon/Fly)
- Sessions con expiry
**Dependencias**: ninguna
**Esfuerzo**: 2-4 hrs según método
**Prioridad**: ALTA antes de agregar data sensible (clientes CMU, saldos)

### HU-DASH-X.2 — Notificaciones push/email configurables
**Rol**: Yo
**Historia**: Como usuario, quiero configurar qué alertas me llegan por email vs WhatsApp vs solo dashboard.
**Criterios**:
- Settings page con matriz (alerta × canal)
- Preferencias por usuario (cuando haya multi-user)
- Templates configurables
**Dependencias**: HU-X.1 (auth) + Twilio + servicio email
**Esfuerzo**: 3 hrs

### HU-DASH-X.3 — Dark/Light mode toggle
**Rol**: Preferencias personales
**Historia**: Como usuario, quiero poder cambiar a light mode si estoy en oficina con mucha luz.
**Criterios**:
- Toggle en header
- Persistencia en localStorage
- Todo legible en ambos modos
**Dependencias**: ninguna
**Esfuerzo**: 2 hrs

### HU-DASH-X.4 — PWA (Progressive Web App) mobile
**Rol**: Yo en el teléfono
**Historia**: Como dueño, quiero instalar el dashboard como app en mi iPhone para consultarlo rápido sin Chrome.
**Criterios**:
- Manifest.json
- Service Worker (cache básico)
- Icono instalable
- Splash screen
**Dependencias**: ninguna
**Esfuerzo**: 2 hrs

### HU-DASH-X.5 — Reportes PDF generados
**Rol**: Yo → inversores / autoridades
**Historia**: Como socio, quiero exportar snapshots del dashboard como PDF profesional para mandar a inversionistas o regulatorio.
**Criterios**:
- Botón "Export PDF" en cada vista
- Template con logo, fecha, período, KPIs
- Gráficas embebidas (SVG)
- Multi-página si es necesario
**Dependencias**: librería PDF (puppeteer / jsPDF)
**Esfuerzo**: 4 hrs

### HU-DASH-X.6 — Audit log
**Rol**: Cumplimiento
**Historia**: Como responsable de seguridad, quiero un log de quién hizo qué en el sistema (uploads, deletes, config changes) para auditoría.
**Criterios**:
- Tabla `audit_log` con actor, acción, timestamp, metadata
- Vista en admin
- Export CSV/PDF
**Dependencias**: HU-X.1 (auth)
**Esfuerzo**: 3 hrs

---

## Priorización sugerida (próximas 3 iteraciones)

Recomendación basada en impacto × esfuerzo × dependencias:

### 🔥 Iteración inmediata (próxima sesión, ~6 hrs)
1. HU-DASH-X.1 — **Auth básica** (CRÍTICO antes de meter data sensible) — 2 hrs
2. HU-DASH-2.3 — **Upload drag-drop** (elimina fricción diaria) — 2 hrs
3. HU-DASH-2.4 — **Export CSV** (quick win, útil ya) — 2 hrs

### 📅 Iteración 2 (después, ~8 hrs)
4. HU-DASH-3.1 — **Importar catálogo CMU** (habilita todas las features comerciales) — 3 hrs
5. HU-DASH-2.1 — **Drill-down por estación** — 3 hrs
6. HU-DASH-2.2 — **Drill-down por placa** — 3 hrs (se aprovecha la lógica de 2.1)

### 📅 Iteración 3 (cuando 3.1 esté listo, ~10 hrs)
7. HU-DASH-3.4 — **Alertas críticas exposure Ahorro Renovación** (muy alto valor de negocio) — 3 hrs
8. HU-DASH-3.2 — **Vista Productos Financieros** — 3 hrs
9. HU-DASH-3.3 — **Alertas retención básicas** — 2 hrs
10. HU-DASH-3.6 — **Segmentación por vehículo** — 3 hrs

### 📅 Iteraciones 4+ dependen de:
- Wrapper Phase 2 activo → HU-4.x (live feed, alertas real-time)
- SCADA connector (HU-backend-4.1/4.2) → HU-5.x (telemetría)
- Central Gas operando + Odoo timbrando → HU-6.x (finanzas)

---

## Total estimado por iteración

| Iteración | HUs | Horas | Feature highlight |
|---|---|---|---|
| v1 (done) | 5 | 8 | MVP con 3 tabs + rebrand |
| v2 | 5 | 13 | Drill-downs + upload UI + export |
| v3 | 6 | 18 | Productos financieros CMU + retention |
| v4 | 5 | 19 | Real-time + alertas WhatsApp |
| v5 | 3 | 12 | SCADA telemetría |
| v6 | 3 | 11 | Finanzas Odoo |
| Cross-cutting | 6 | 16 | Auth, PDF, PWA, audit |
| **Total roadmap** | **33 HUs** | **~97 hrs** | |

Si trabajamos 4 hrs/semana en esto, son ~6 meses hasta tener todo el TO-BE. Más que alcanza para llegar a Q3 2026 cuando inicie operación real de Central Gas con features robustos.
