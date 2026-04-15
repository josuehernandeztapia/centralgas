# Despliegue Fase 1 — Fly.io

Guía paso a paso para subir el Central Gas Agent a Fly.io en modo Fase 1
(ingestión manual de Excel, sin operación real todavía).

---

## Pre-requisitos

- [ ] Cuenta Fly.io activa (ya tienes ✅)
- [ ] `flyctl` instalado localmente: `brew install flyctl` (Mac) o `curl -L https://fly.io/install.sh | sh`
- [ ] Estar autenticado: `fly auth login`
- [ ] Anthropic API key disponible (la del agente)
- [ ] (Opcional) credenciales Twilio para WhatsApp — no obligatorio en Fase 1

---

## Paso 1 — Verificar el código local

Desde la raíz del repo (`central-gas-agent/`):

```bash
# Que los archivos clave existan
ls Dockerfile fly.toml app/main.py requirements.txt .dockerignore

# Que las dependencias incluyan xlrd y python-multipart
grep -E "xlrd|multipart" requirements.txt
```

Si todo aparece, listo.

---

## Paso 2 — Crear la app en Fly.io

```bash
fly apps create central-gas-agent --org personal
```

Si el nombre `central-gas-agent` ya está tomado globalmente, ajusta a algo único
como `central-gas-agent-<tus-iniciales>` y actualiza el campo `app =` en `fly.toml`.

---

## Paso 3 — Provisionar Postgres administrado

**Opción A (recomendada) — Managed Postgres** (nuevo servicio soportado por Fly):

```bash
fly mpg create --name central-gas-db --region dfw
```

**Opción B (fallback) — Unmanaged Postgres** (legacy, tú administras backups):

```bash
fly postgres create \
  --name central-gas-db \
  --region dfw \
  --initial-cluster-size 1 \
  --vm-size shared-cpu-1x \
  --volume-size 10
```

> ⚠️ **Región**: Fly NO tiene datacenter en México. La más cercana a Aguascalientes
> es `dfw` (Dallas, Texas) con latencia ~60-80ms. Otras opciones cercanas: `mia` (Miami),
> `lax` (Los Angeles), `sjc` (San Jose).

**Anota la salida** — Fly te muestra la connection string una sola vez.
Se ve algo así:

```
postgres://centralgas:LARGO_PASSWORD_AQUI@central-gas-db.flycast:5432/centralgas?sslmode=disable
```

Cópiala completa.

---

## Paso 4 — Conectar Postgres a la app

```bash
fly postgres attach central-gas-db --app central-gas-agent
```

Esto crea automáticamente la variable secreta `DATABASE_URL` en la app.

---

## Paso 5 — Configurar secrets

```bash
# Anthropic API key (obligatorio)
fly secrets set ANTHROPIC_API_KEY="sk-ant-api03-XXXXXXXXXXXX" --app central-gas-agent

# Odoo (cuando esté listo Central Gas como empresa 2)
fly secrets set \
  ODOO_URL="https://erp.tudominio.com" \
  ODOO_DB="centralgas_erp" \
  ODOO_USER="admin@centralgas.mx" \
  ODOO_PASSWORD="XXXXX" \
  --app central-gas-agent

# WhatsApp Twilio (opcional Fase 1)
fly secrets set \
  TWILIO_ACCOUNT_SID="ACxxxx" \
  TWILIO_AUTH_TOKEN="xxxxx" \
  TWILIO_WHATSAPP_FROM="whatsapp:+14155238886" \
  JOSUE_WHATSAPP="whatsapp:+52449XXXXXXX" \
  --app central-gas-agent
```

Verificar:

```bash
fly secrets list --app central-gas-agent
```

---

## Paso 6 — Primer deploy

```bash
fly deploy --app central-gas-agent
```

Va a:
1. Construir la imagen Docker (multi-stage, ~3-5 min la primera vez)
2. Empujarla al registry de Fly
3. Lanzar 1 máquina en QRO
4. Hacer health check en `/health`

Cuando termine verás algo como:

```
✓ Machine xxxxx [app] update succeeded
✓ Deployed
Visit your newly deployed app at https://central-gas-agent.fly.dev
```

---

## Paso 7 — Verificar que está vivo

```bash
# Health check
curl https://central-gas-agent.fly.dev/health

# Info raíz
curl https://central-gas-agent.fly.dev/

# Docs interactivos (Swagger UI)
open https://central-gas-agent.fly.dev/docs
```

Respuesta esperada de `/health`:

```json
{
  "status": "ok",
  "service": "central-gas-agent",
  "version": "0.1.0-phase1",
  "phase": "manual_ingestion",
  "timestamp": "2026-04-14T..."
}
```

---

## Paso 8 — Probar upload de Excel real

Subir uno de los reportes de NatGas que ya tienes:

```bash
# ventas_detalladas (el grande, ~31k filas)
curl -X POST https://central-gas-agent.fly.dev/upload-excel \
  -F "file=@/ruta/a/ventas_detalladas_natgas.xls" \
  -F "report_type=ventas_detalladas"

# recaudos_financiera (procesar TODAS las financieras en Fase 1)
curl -X POST https://central-gas-agent.fly.dev/upload-excel \
  -F "file=@/ruta/a/recaudos_financiera_natgas.xls" \
  -F "report_type=recaudos_financiera"
```

Respuesta esperada:

```json
{
  "status": "ok",
  "filename": "ventas_detalladas_natgas.xls",
  "report_type": "ventas_detalladas",
  "rows_parsed": 31016,
  "rows_inserted": 31016,
  "rows_skipped": 0,
  "warnings": [],
  "detected_format": "OLE2 (.xls)"
}
```

---

## Comandos útiles día a día

```bash
# Logs en vivo
fly logs --app central-gas-agent

# SSH al contenedor (debug)
fly ssh console --app central-gas-agent

# Métricas
fly status --app central-gas-agent

# Escalar memoria si se queda corto
fly scale memory 2048 --app central-gas-agent

# Forzar 1 máquina siempre encendida (cuando entre operación)
fly scale count 1 --app central-gas-agent

# Conectar a Postgres
fly postgres connect --app central-gas-db

# Ver secrets configurados (no muestra valores)
fly secrets list --app central-gas-agent

# Re-deploy después de cambios
fly deploy --app central-gas-agent

# Rollback al deploy anterior si algo se rompe
fly releases --app central-gas-agent
fly releases rollback <version> --app central-gas-agent
```

---

## Costos estimados Fase 1

| Recurso | Config | Costo/mes |
|---|---|---|
| App VM | shared-cpu-1x, 1GB, auto-stop | ~$2-4 USD |
| Postgres | shared-cpu-1x, 1GB, 10GB disk | ~$15 USD |
| Bandwidth | <10GB/mes | $0 (incluido) |
| **Total** | | **~$17-19 USD/mes** |

Auto-stop machine apaga la VM cuando no hay tráfico → ahorro real cuando solo
subes Excel ocasionalmente.

---

## Migración a Hetzner cuando convenga

Si en algún momento quieres bajar el costo a ~$5 USD/mes (Hetzner CX22):

1. `pg_dump` desde Fly Postgres
2. Crear Hetzner CX22, instalar Postgres + Docker
3. `docker-compose up -d` con el mismo `docker-compose.yml` ya existente
4. `psql` restore del dump
5. Apuntar dominio al nuevo IP
6. `fly apps destroy central-gas-agent` cuando confirmes que migró bien

El stack es portable porque todo está dockerizado.

---

## Próximos pasos después de Fase 1 funcionando

1. Wirear los endpoints `/transactions`, `/stats`, `/sobreprecio/distribution`
   contra Postgres (hoy son stubs).
2. Crear `app/api/routers/` con routers separados por dominio (clientes, transacciones, alertas).
3. Agregar autenticación básica (API key header) antes de exponer fuera de tu uso personal.
4. Cuando inicie operación: agregar el proceso `worker` en `fly.toml` y activar el wrapper GasUp.
