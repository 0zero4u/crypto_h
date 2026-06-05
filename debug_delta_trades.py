#!/usr/bin/env python3
"""Debug: check Delta trade messages."""
import asyncio
import json
import websockets

async def main():
    uri = "wss://public-socket.india.delta.exchange"
    print(f"Connecting to {uri}...")

    async with websockets.connect(uri) as ws:
        # Subscribe to trades for multiple active symbols
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "trades", "symbols": ["BTCUSD", "ETHUSD", "XRPUSD"]},
                ]
            }
        }
        print(f">>> {json.dumps(sub)}")
        await ws.send(json.dumps(sub))

        # Collect messages for 10 seconds
        print("\nListening for 10s...\n")
        end_time = asyncio.get_event_loop().time() + 10
        msg_types = {}

        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                data = json.loads(msg)
                msg_type = data.get("type", "unknown")
                msg_types[msg_type] = msg_types.get(msg_type, 0) + 1

                if msg_type == "trades":
                    print(f"TRADE: {json.dumps(data)[:200]}")
                elif msg_type not in ("subscriptions",):
                    print(f"OTHER ({msg_type}): {json.dumps(data)[:150]}")
            except asyncio.TimeoutError:
                continue

        print(f"\nMessage types received: {msg_types}")

asyncio.run(main())
