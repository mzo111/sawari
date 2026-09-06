import asyncio
import json
import os

import websockets

URL = "wss://stream.aisstream.io/v0/stream"
KEY = os.environ["AISSTREAM_API_KEY"]

CASES = {
    "worldwide":  [[[-90, -180], [90, 180]]],
    "gulf_latlon": [[[22.0, 47.0], [30.5, 60.5]]],
    "gulf_lonlat": [[[47.0, 22.0], [60.5, 30.5]]],
}

async def probe(name, boxes):
    try:
        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps({"APIKey": KEY, "BoundingBoxes": boxes}))
            n = 0
            deadline = asyncio.get_event_loop().time() + 25
            while asyncio.get_event_loop().time() < deadline:
                try:
                    m = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                if b"SubscriptionConfirmation" in m or "SubscriptionConfirmation" in str(m):
                    continue
                n += 1
                if n == 1:
                    print(f"  first: {str(m)[:250]}")
            print(f"{name}: {n} messages in 25s")
    except Exception as e:  # noqa: BLE001 - diagnostic script, report and move on
        print(f"{name}: ERROR {e}")

async def main():
    for name, boxes in CASES.items():
        await probe(name, boxes)

asyncio.run(main())
