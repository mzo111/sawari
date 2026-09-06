# Sawari v2 — working agreement

Real-time vessel tracking and ETA prediction for North Sea / Channel ports. This is a portfolio
project with a job-search deadline, not a startup. Optimize for a service that is
provably live, tested, and measured by **Sep 21, 2026** — not for feature count.

## Non-negotiables

1. **No estimated numbers, ever.** Every figure that reaches the README or a
   resume bullet comes from a measurement recorded in `PROGRESS.md`, with the
   command that produced it. If it wasn't measured, it doesn't get written down.
2. **Ingestion never stops.** Once the worker is deployed, uptime is the product.
   Changes that risk the ingest loop get tested locally first, always.
3. **No new dependencies without asking.** The stack below is pinned. If a task
   seems to need something new, say so and stop; don't add it.
4. **I have to defend this in an interview.** Explain the reasoning behind
   non-obvious code as you write it. Never hand me a design decision I can't
   reconstruct at a whiteboard with the laptop closed.

## Stack (pinned — do not relitigate mid-build)

Python 3.12 · FastAPI · asyncpg + SQLAlchemy 2.x · `websockets` for ingest ·
PostgreSQL 16 with TimescaleDB + PostGIS (`timescale/timescaledb-ha:pg16`) ·
Redis 7 · XGBoost · pytest + ruff · Docker Compose · GitHub Actions ·
Hetzner CX22 VPS with Caddy for TLS.

Data source: AISStream.io websocket feed. Bounding box: southern North Sea +
eastern Channel (49–56°N, 2.5°W–10°E). Chosen from measured AISStream density —
the Gulf has zero coverage; see `PROGRESS.md` 2026-09-06. Recorded in `DESIGN.md`.

## Anti-goals — do NOT build these

- User accounts, auth, or roles
- Kafka or any message broker beyond Redis
- Multi-region anything
- A React redesign (v1's dashboard gets **ported**, not reinvented)
- Mobile layout polish
- Any abstraction layer added "for later"

Scope creep dressed up as thoroughness is the known failure mode of this project.
If a task doesn't serve a checkpoint below, it doesn't get done.

## Definition of done

- [ ] Ingestion has run unattended 7+ consecutive days; messages/day is known
- [ ] REST + WebSocket API deployed at a real URL with OpenAPI docs
- [ ] ETA model beats a stated naive baseline; MAE + improvement % on a
      **temporal** holdout (never a random split — that leaks)
- [ ] 25+ meaningful tests green in GitHub Actions on every push; README badge
- [ ] Load test report: max sustained req/s and concurrent WS clients at p95 < 300ms
- [ ] Prometheus + Grafana dashboard live; screenshot in README
- [ ] README with architecture diagram and results tables; `DESIGN.md` with three
      trade-off write-ups
- [ ] Every `[X]` placeholder in the resume bullets replaced with a measured value

## Repo layout

```
ingest/   AIS websocket consumer + parser   (parser.py is pure — no I/O, no clock)
api/      FastAPI app, routers, websocket
ml/       features, baseline, train, eval, registry
db/       schema.sql, migrations, retention policy
web/      dashboard ported from v1
tests/    unit + integration
ops/      docker-compose, Dockerfiles, prometheus.yml, grafana/, caddy/
```

## Conventions

- `ingest/parser.py` stays pure. Any function that touches the network, the
  database, or `now()` belongs in `worker.py`, not the parser.
- Batched writes only. Never row-at-a-time inserts into `positions`.
- Dropped messages are counted by reason, not silently discarded — drop counts
  are a metric and a talking point.
- Run `ruff check .` and `pytest` before declaring anything finished.
- Schema changes are additive migrations in `db/`, not edits to `schema.sql`
  once the VPS is live.

## How to work with me

- Use plan mode for anything touching more than one file. Show the plan, wait.
- Small commits with real messages. One concern per commit.
- Say when something is uncertain rather than picking silently and moving on.
- If a checkpoint fails, tell me it failed. A green summary over a broken
  checkpoint is worse than useless here.
