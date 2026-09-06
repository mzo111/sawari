import asyncio
import json
import os

import websockets

URL = "wss://stream.aisstream.io/v0/stream"
BOXES = [[[22.0, 47.0], [30.5, 60.5]]]

async def main():
    key = os.environ["AISSTREAM_API_KEY"]
    print(f"key length: {len(key)}")
    async with websockets.connect(URL) as ws:
        await ws.send(json.dumps({"APIKey": key, "BoundingBoxes": BOXES}))
        for i in range(3):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=20)
            except asyncio.TimeoutError:
                print(f"[{i}] timeout — no message in 20s")
                continue
            print(f"[{i}] {msg[:600]}")

asyncio.run(main())
