# Degenter Indexer Runbook

## Prerequisites
- Node.js >= 20.10
- Redis instance (default: `redis://localhost:6379`)
- Postgres/Timescale instance (set `TIMESCALE_DATABASE_URL`)
- ClickHouse instance (default: `http://localhost:8123`)
- (Optional) Docker for running the databases locally

## Environment variables
Create a root `.env` file. Key variables read by the services:
- `REDIS_URL` – Redis connection string.
- `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB` – ClickHouse connection.
- `TIMESCALE_DATABASE_URL` (or `DATABASE_URL`) – Timescale/Postgres connection.
- `SVC_NAME` – optional application_name for SQL sessions.

The loader at `common/load-env.js` reads the root `.env` and then each service-local `.env` if present.

## Install dependencies
```bash
npm install
```

## Prepare databases
### ClickHouse
Create the database (once) and apply the schema (tables live in the `degenter` DB):
```bash
clickhouse client -q "CREATE DATABASE IF NOT EXISTS degenter"
clickhouse client --multiquery < clickhouseschema.sql
```
The schema is also split into `schemas/clickhouse/*.sql` if you prefer to apply it piecewise:
```bash
cat schemas/clickhouse/*.sql | clickhouse client --multiquery
```
Tables cover tokens, pools, trades, price ticks, pool state, holders, leaderboards, and alerting data.

### Timescale/Postgres
Load `timescaleschema.sql` with `psql`:
```bash
psql "$TIMESCALE_DATABASE_URL" -f timescaleschema.sql
```

## Start the indexer locally
Run each service in its own terminal:
1) **Real-time ingester** – fetches blocks and emits raw payloads
```bash
npm run start:ingester-realtime
```
2) **Processor core** – parses payloads and publishes swap/liquidity events
```bash
npm run start:processor
```
3) **Timescale worker** – writes hot state (trades/prices/balances) to Postgres
```bash
npm run --workspace services/worker-timescale start
```
4) **ClickHouse worker** – batches archive writes to ClickHouse
```bash
npm run --workspace services/worker-clickhouse start
```

(Optional) Start any other workers you need (e.g., metadata, rollups) using the same `npm run --workspace services/<name> start` pattern.

## Health checks
- ClickHouse connectivity: `node -e "import('./common/db-clickhouse.js').then(m=>m.chPing())"`
- Timescale connectivity: `node -e "import('./common/db-timescale.js').then(m=>m.tsInit())"`
