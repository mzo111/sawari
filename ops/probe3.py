import asyncio
import json
import os
import time
from collections import Counter

import websockets

URL = "wss://stream.aisstream.io/v0/stream"
KEY = os.environ["AISSTREAM_API_KEY"]

REGIONS = {
    "ARABIAN_GULF":  (22.0, 30.5, 47.0, 60.5),
    "GULF_OF_OMAN":  (20.0, 26.0, 56.0, 62.0),
    "RED_SEA":       (12.0, 30.0, 32.0, 44.0),
    "MEDITERRANEAN": (30.0, 46.0, -6.0, 36.0),
    "N_EUROPE":      (48.0, 66.0, -12.0, 32.0),
    "US_COASTS":     (24.0, 50.0, -130.0, -66.0),
    "E_ASIA":        (20.0, 46.0, 100.0, 146.0),
    "SE_ASIA":       (-10.0, 20.0, 95.0, 130.0),
}
ITEMS = list(REGIONS.items())

def region_of(lat, lon):
    for name, (la, lb, oa, ob) in ITEMS:
        if la <= lat <= lb and oa <= lon <= ob:
            return name
    return "OTHER"

async def main():
    counts, total = Counter(), 0
    async with websockets.connect(
        URL, ping_interval=20, ping_timeout=60, max_queue=4096
    ) as ws:
        await ws.send(json.dumps({
            "APIKey": KEY,
            "BoundingBoxes": [[[-90, -180], [90, 180]]],
            "FilterMessageTypes": ["PositionReport"],
        }))
        end = time.time() + 300
        async for raw in ws:
            try:
                meta = json.loads(raw)["MetaData"]
                total += 1
                counts[region_of(meta["latitude"], meta["longitude"])] += 1
            except (KeyError, ValueError):
                pass
            if total % 500 == 0 and time.time() > end:
                break
    print(f"\ntotal: {total} position reports in 5 min\n")
    for name, n in counts.most_common():
        print(f"  {name:<14} {n:>7}  ({100*n/total:.2f}%)")

asyncio.run(main())
