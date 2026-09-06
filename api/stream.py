"""Redis 'positions' channel -> connected WebSocket clients.

One subscriber per process. Each client gets its own bounded queue, so a slow
consumer drops its own messages instead of stalling the fan-out for everyone.
"""

from __future__ import annotations

import asyncio
import logging

import redis.asyncio as aioredis

log = logging.getLogger("api.stream")

CHANNEL = "positions"
CLIENT_QUEUE_SIZE = 256


class Broadcaster:
    def __init__(self, redis_url: str) -> None:
        self.redis_url = redis_url
        self.clients: set[asyncio.Queue[str]] = set()
        self.received = 0
        self.dropped = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="redis-subscriber")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=CLIENT_QUEUE_SIZE)
        self.clients.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self.clients.discard(queue)

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            try:
                redis = aioredis.from_url(self.redis_url, decode_responses=True)
                try:
                    async with redis.pubsub() as pubsub:
                        await pubsub.subscribe(CHANNEL)
                        log.info("subscribed to redis channel %r", CHANNEL)
                        backoff = 1.0
                        async for message in pubsub.listen():
                            if message.get("type") != "message":
                                continue
                            self._fanout(message["data"])
                finally:
                    await redis.aclose()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the bridge must outlive redis blips
                log.warning("redis subscriber dropped (%s); retrying in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    def _fanout(self, data: str) -> None:
        self.received += 1
        for queue in self.clients:
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                self.dropped += 1
