"""Sawari v2 API.

REST over the positions hypertable, plus a WebSocket bridge from the Redis
'positions' channel the ingest worker publishes to.

  uvicorn api.main:app
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from .stream import Broadcaster
from .vessels import router as vessels_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("api")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
HEALTH_TIMEOUT = 2.0

# approximate_row_count is O(1) from stats; exact count(*) walks every chunk.
HEALTH_DB = """
SELECT max(time) AS last_write, approximate_row_count('positions') AS rows_approx
FROM positions
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.broadcaster = Broadcaster(REDIS_URL)
    app.state.broadcaster.start()
    log.info("api started")
    try:
        yield
    finally:
        await app.state.broadcaster.stop()
        await app.state.redis.aclose()
        await app.state.pool.close()
        log.info("api stopped")


app = FastAPI(title="Sawari v2 API", version="0.1.0", lifespan=lifespan)
app.include_router(vessels_router)


@app.get("/health", tags=["ops"])
async def health(request: Request) -> JSONResponse:
    state = request.app.state
    db = {"reachable": False, "last_write": None, "rows_approx": None}
    rd = {"reachable": False}

    try:
        async with asyncio.timeout(HEALTH_TIMEOUT):
            row = await state.pool.fetchrow(HEALTH_DB)
        db = {
            "reachable": True,
            "last_write": row["last_write"].isoformat() if row["last_write"] else None,
            "rows_approx": row["rows_approx"],
        }
    except Exception as exc:  # noqa: BLE001 - health must answer, not raise
        log.warning("health: db check failed: %s", exc)

    try:
        async with asyncio.timeout(HEALTH_TIMEOUT):
            rd["reachable"] = bool(await state.redis.ping())
    except Exception as exc:  # noqa: BLE001
        log.warning("health: redis check failed: %s", exc)

    bc: Broadcaster = state.broadcaster
    ok = db["reachable"] and rd["reachable"]
    body = {
        "status": "ok" if ok else "degraded",
        "db": db,
        "redis": rd,
        "stream": {"clients": len(bc.clients), "received": bc.received, "dropped": bc.dropped},
    }
    return JSONResponse(body, status_code=200 if ok else 503)


@app.websocket("/ws/positions")
async def ws_positions(ws: WebSocket) -> None:
    await ws.accept()
    bc: Broadcaster = ws.app.state.broadcaster
    queue = bc.subscribe()
    log.info("ws client connected (%d total)", len(bc.clients))

    async def sender() -> None:
        while True:
            await ws.send_text(await queue.get())

    async def receiver() -> None:
        # Client frames are ignored; reading them is how a close shows up
        # before the next send would fail.
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                return

    tasks = {asyncio.create_task(sender()), asyncio.create_task(receiver())}
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            exc = task.exception()
            if exc and not isinstance(exc, WebSocketDisconnect):
                log.warning("ws client dropped: %r", exc)
    finally:
        bc.unsubscribe(queue)
        log.info("ws client disconnected (%d total)", len(bc.clients))
