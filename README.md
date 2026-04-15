# Central Gas Agent

AI-powered ingestion + reconciliation engine for Central Gas's CNG operations in Aguascalientes (3 EDS).

## Stack

- **API**: FastAPI (Python 3.12) on Fly.io (DFW region)
- **Database**: Neon Postgres 17 (serverless, US East)
- **Parsers**: xlrd (.xls) + openpyxl (.xlsx) for GasUp HeadOffice reports
- **CI/CD**: GitHub Actions → `fly deploy`

## Phase 1 (current)

Manual ingestion mode for validation. Operators upload `.xls` reports
exported from GasUp HeadOffice via `POST /upload-excel`. The agent
parses with calibrated column mappings and persists to Postgres with
ON CONFLICT idempotency.

## API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness probe |
| GET | `/` | Service info + endpoint catalog |
| POST | `/upload-excel` | Upload GasUp report (.xls/.xlsx) |
| GET | `/transactions` | Paginated transaction list with filters |
| GET | `/stats` | Aggregate KPIs (totals, by estación, by medio_pago, by day) |
| GET | `/sobreprecio/distribution` | Histogram + percentiles of per-LEQ surcharge |
| GET | `/docs` | Swagger UI (interactive) |

Production URL: <https://central-gas-agent.fly.dev>

## Local development

```bash
cp .env.example .env  # fill in real values
docker-compose up -d
curl http://localhost:8000/health
```

## Deploy

Pushes to `main` auto-deploy via GitHub Actions.

Manual deploy from local machine:

```bash
fly deploy --app central-gas-agent
```

Migrations:

```bash
fly ssh console --app central-gas-agent --command "python3 /app/scripts/init_db.py"
fly ssh console --app central-gas-agent --command "python3 /app/scripts/migrate_widen_placa.py"
fly ssh console --app central-gas-agent --command "python3 /app/scripts/migrate_add_unique_idx.py"
```

## Phases

- **Phase 1** (current) — manual upload, in-memory + Postgres persistence
- **Phase 2** — auto-ingestion via wrapper (HeadOffice scrape) + WhatsApp alerts
- **Phase 3** — SCADA telemetry (Snap7) + reconciliation engine
- **Phase 4** — CRE/SASISOPA/SEMARNAT regulatory reporting

## License

Proprietary — Grupo Estación Central Gas S.A. de C.V.
