#!/usr/bin/env python3
"""Debug Delta L1 - capture raw messages and mid-price."""
import asyncio
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.normalizer import normalize_delta_l1


async def main():
    import websockets
    
    uri = "wss://public-socket.india.delta.exchange"
    print(f"Connecting to {uri}...")
    
    last_mid = None
    msg_count = 0
    spike_count = 0
    
    async with websockets.connect(uri) as ws:
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "ob_l1", "symbols": ["BTCUSD"]},
                ]
            }
        }
        await ws.send(json.dumps(sub))
        print("Subscribed to ob_l1\n")
        
        end_time = asyncio.get_event_loop().time() + 30  # 30 seconds
        
        while asyncio.get_event_loop().time() < end_time:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                msg = json.loads(raw)
                
                if msg.get("type") != "ob_l1":
                    continue
                
                msg_count += 1
                
                norm = normalize_delta_l1(msg)
                if not norm:
                    continue
                
                mid = norm["mid_price"]
                bid = norm["bid"]
                ask = norm["ask"]
                
                if last_mid is not None:
                    change_pct = abs(mid - last_mid) / last_mid * 100
                    
                    if change_pct > 0.05:  # More than 0.05% change
                        spike_count += 1
                        print(f"\n[SPIKE #{spike_count}] {datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}")
                        print(f"  Mid: {last_mid:.2f} -> {mid:.2f} ({change_pct:.4f}%)")
                        print(f"  Bid: {bid:.2f} | Ask: {ask:.2f}")
                        print(f"  Spread: {ask - bid:.2f}")
                
                last_mid = mid
                
                if msg_count % 50 == 0:
                    print(f"[{msg_count} msgs] Mid={mid:.2f} "
                          f"Bid={bid:.2f} Ask={ask:.2f} "
                          f"Spread={ask-bid:.2f}")
                
            except asyncio.TimeoutError:
                continue
    
    print(f"\n\nTotal messages: {msg_count}")
    print(f"Spikes detected: {spike_count}")


if __name__ == "__main__":
    asyncio.run(main())
