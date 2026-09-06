"""Sawari v2 ingest worker.

One job: make a counter go up, forever, without supervision.

  python -m ingest.worker

Reliability features that are the actual point of this file:
  * exponential-backoff reconnect, 1s -> 60s cap, reset on a healthy stream
  * batched inserts (flush on 500 rows OR 2 seconds), never row-at-a-time
  * ON CONFLICT DO NOTHING so a replayed feed can't crash the loop
  * drop counters by reason, logged every heartbeat
  * graceful SIGTERM/SIGINT: flush the buffer, close the pool, exit 0
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from collections import Counter

import asyncpg
import redis.asyncio as aioredis
import websockets

from .parser import parse_position, parse_static

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("ingest")

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.environ["AISSTREAM_API_KEY"]
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Arabian Gulf + Strait of Hormuz + Gulf of Oman, and the Salalah approaches.
# Format is [[lat_min, lon_min], [lat_max, lon_max]]. Recorded in DESIGN.md.
BOUNDING_BOXES = [
    [[22.0, 47.0], [30.5, 60.5]],   # Gulf, Hormuz, Sohar/Muscat
    [[15.5, 52.0], [19.5, 56.5]],   # Salalah / Duqm approaches
]

BATCH_MAX_ROWS = 500
BATCH_MAX_SECONDS = 2.0
HEARTBEAT_SECONDS = 60

INSERT_POSITION = """
INSERT INTO positions (time, mmsi, geom, sog, cog, heading, nav_status)
VALUES ($1, $2, ST_SetSRID(ST_MakePoint($3, $4), 4326), $5, $6, $7, $8)
ON CONFLICT (mmsi, time) DO NOTHING
"""

UPSERT_VESSEL_SEEN = """
INSERT INTO vessels (mmsi, last_seen) VALUES ($1, $2)
ON CONFLICT (mmsi) DO UPDATE SET last_seen = GREATEST(vessels.last_seen, $2)
"""

UPSERT_VESSEL_STATIC = """
INSERT INTO vessels (mmsi, name, call_sign, imo, ship_type,
                     length_m, width_m, draught_m, destination)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (mmsi) DO UPDATE SET
    name        = COALESCE(EXCLUDED.name, vessels.name),
    call_sign   = COALESCE(EXCLUDED.call_sign, vessels.call_sign),
    imo         = COALESCE(EXCLUDED.imo, vessels.imo),
    ship_type   = COALESCE(EXCLUDED.ship_type, vessels.ship_type),
    length_m    = COALESCE(EXCLUDED.length_m, vessels.length_m),
    width_m     = COALESCE(EXCLUDED.width_m, vessels.width_m),
    draught_m   = COALESCE(EXCLUDED.draught_m, vessels.draught_m),
    destination = COALESCE(EXCLUDED.destination, vessels.destination),
    last_seen   = now()
"""


class Stats:
    def __init__(self) -> None:
        self.received = 0
        self.written = 0
        self.drops: Counter[str] = Counter()
        self.reconnects = 0
        self.started = time.monotonic()

    def line(self) -> str:
        mins = (time.monotonic() - self.started) / 60 or 1e-9
        drops = ", ".join(f"{k}={v}" for k, v in sorted(self.drops.items())) or "none"
        return (
            f"heartbeat received={self.received} written={self.written} "
            f"rate={self.written / mins:.0f}/min reconnects={self.reconnects} "
            f"drops[{drops}]"
        )


class Ingestor:
    def __init__(self) -> None:
        self.stats = Stats()
        self.buffer: list[tuple] = []
        self.seen_mmsi: set[int] = set()
        self.pool: asyncpg.Pool | None = None
        self.redis: aioredis.Redis | None = None
        self.stopping = asyncio.Event()
        self._last_flush = time.monotonic()

    # ---------------------------------------------------------------- setup
    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        log.info("connected to postgres and redis")

    async def close(self) -> None:
        await self.flush()
        if self.pool:
            await self.pool.close()
        if self.redis:
            await self.redis.aclose()
        log.info("shutdown complete | %s", self.stats.line())

    # ------------------------------------------------------------ main loop
    async def run(self) -> None:
        backoff = 1.0
        asyncio.create_task(self.heartbeat())
        asyncio.create_task(self.flush_timer())

        while not self.stopping.is_set():
            try:
                async with websockets.connect(
                    AISSTREAM_URL, ping_interval=20, ping_timeout=20, max_queue=2048
                ) as ws:
                    await ws.send(json.dumps({
                        "APIKey": API_KEY,
                        "BoundingBoxes": BOUNDING_BOXES,
                        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
                    }))
                    log.info("subscribed to aisstream")
                    healthy_since = time.monotonic()

                    async for raw in ws:
                        await self.handle(raw)
                        if backoff > 1.0 and time.monotonic() - healthy_since > 30:
                            backoff = 1.0      # stream is stable again
                        if self.stopping.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the loop must never die
                self.stats.reconnects += 1
                log.warning("stream dropped (%s); reconnecting in %.0fs", exc, backoff)
                try:
                    await asyncio.wait_for(self.stopping.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------- handling
    async def handle(self, raw: str | bytes) -> None:
        self.stats.received += 1
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self.stats.drops["malformed_json"] += 1
            return

        kind = msg.get("MessageType")

        if kind == "ShipStaticData":
            static = parse_static(msg)
            if static and self.pool:
                await self.pool.execute(
                    UPSERT_VESSEL_STATIC, static.mmsi, static.name, static.call_sign,
                    static.imo, static.ship_type, static.length_m, static.width_m,
                    static.draught_m, static.destination,
                )
                self.seen_mmsi.add(static.mmsi)
            return

        if kind != "PositionReport":
            return

        pos, reason = parse_position(msg)
        if reason:
            self.stats.drops[reason] += 1
            return

        self.buffer.append((
            pos.time, pos.mmsi, pos.lon, pos.lat,
            pos.sog, pos.cog, pos.heading, pos.nav_status,
        ))

        if self.redis:
            await self.redis.publish("positions", json.dumps({
                "mmsi": pos.mmsi, "lat": pos.lat, "lon": pos.lon,
                "sog": pos.sog, "cog": pos.cog,
                "time": pos.time.isoformat(),
            }))
            await self.redis.hset("latest_positions", str(pos.mmsi), json.dumps({
                "lat": pos.lat, "lon": pos.lon, "sog": pos.sog,
                "time": pos.time.isoformat(),
            }))

        if len(self.buffer) >= BATCH_MAX_ROWS:
            await self.flush()

    async def flush(self) -> None:
        if not self.buffer or not self.pool:
            return
        batch, self.buffer = self.buffer, []
        self._last_flush = time.monotonic()
        new_mmsi = {row[1] for row in batch} - self.seen_mmsi
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # FK safety: vessels row must exist before port_calls later on.
                    if new_mmsi:
                        await conn.executemany(
                            UPSERT_VESSEL_SEEN,
                            [(m, batch[0][0]) for m in new_mmsi],
                        )
                    await conn.executemany(INSERT_POSITION, batch)
            self.stats.written += len(batch)
            self.seen_mmsi |= new_mmsi
        except Exception as exc:  # noqa: BLE001
            self.stats.drops["db_error"] += len(batch)
            log.error("batch insert failed (%d rows): %s", len(batch), exc)

    # -------------------------------------------------------- housekeeping
    async def flush_timer(self) -> None:
        while not self.stopping.is_set():
            await asyncio.sleep(0.5)
            if self.buffer and time.monotonic() - self._last_flush >= BATCH_MAX_SECONDS:
                await self.flush()

    async def heartbeat(self) -> None:
        while not self.stopping.is_set():
            await asyncio.sleep(HEARTBEAT_SECONDS)
            log.info(self.stats.line())


async def main() -> None:
    ingestor = Ingestor()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, ingestor.stopping.set)
        except NotImplementedError:
            pass  # Windows without ProactorEventLoop support for signals

    await ingestor.start()
    try:
        await ingestor.run()
    finally:
        await ingestor.close()


if __name__ == "__main__":
    asyncio.run(main())
