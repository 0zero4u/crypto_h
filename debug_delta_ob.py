#!/usr/bin/env python3
"""Debug Delta orderbook - capture raw messages and book state."""
import asyncio
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.orderbook import L2OrderBook
from cryptofeed.normalizer import normalize_delta_ob


async def main():
    import websockets
    
    uri = "wss://public-socket.india.delta.exchange"
    print(f"Connecting to {uri}...")
    
    book = L2OrderBook("BTCUSD", depth=20)
    last_seq = 0
    last_dwmp = None
    msg_count = 0
    spike_count = 0
    
    async with websockets.connect(uri) as ws:
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "ob_l2", "symbols": ["BTCUSD"]},
                ]
            }
        }
        await ws.send(json.dumps(sub))
        print("Subscribed to ob_l2\n")
        
        end_time = asyncio.get_event_loop().time() + 30  # 30 seconds
        
        while asyncio.get_event_loop().time() < end_time:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                msg = json.loads(raw)
                
                if msg.get("type") != "ob_l2":
                    continue
                
                msg_count += 1
                action = msg.get("action")
                seq = msg.get("seq", 0)
                
                # Normalize
                norm = normalize_delta_ob(msg)
                if not norm:
                    continue
                
                # Apply to orderbook
                if action == "snapshot":
                    book.apply_snapshot(norm["bids"], norm["asks"], seq, norm["ts_ms"])
                    last_seq = seq
                    print(f"[SNAPSHOT] seq={seq} bids={len(norm['bids'])} asks={len(norm['asks'])}")
                    print(f"  Best bid: {norm['bids'][0] if norm['bids'] else 'N/A'}")
                    print(f"  Best ask: {norm['asks'][0] if norm['asks'] else 'N/A'}")
                elif action == "update":
                    # Check sequence
                    expected_seq = last_seq + 1
                    if seq != expected_seq:
                        print(f"[SEQ GAP] expected={expected_seq} got={seq}")
                    
                    book.apply_diff(norm["bids"], norm["asks"], seq, norm["ts_ms"])
                    last_seq = seq
                
                # Compute DWMP
                dwmp = book.dwmp(n_levels=15)
                if dwmp is None:
                    continue
                
                # Check for spike
                if last_dwmp is not None:
                    change_pct = abs(dwmp - last_dwmp) / last_dwmp * 100
                    
                    if change_pct > 0.5:  # More than 0.5% change
                        spike_count += 1
                        print(f"\n[SPIKE #{spike_count}] {datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}")
                        print(f"  DWMP: {last_dwmp:.2f} -> {dwmp:.2f} ({change_pct:.2f}%)")
                        print(f"  Action: {action}, Seq: {seq}")
                        print(f"  Best bid: {book.best_bid}")
                        print(f"  Best ask: {book.best_ask}")
                        
                        # Show orderbook top 5
                        print(f"\n  Top 5 bids:")
                        for p, q in list(book._bids.items())[:5]:
                            print(f"    {p}: {q}")
                        print(f"  Top 5 asks:")
                        for p, q in list(book._asks.items())[:5]:
                            print(f"    {p}: {q}")
                        
                        # Show the update that caused the spike
                        if action == "update":
                            print(f"\n  Update bids: {norm['bids'][:3]}")
                            print(f"  Update asks: {norm['asks'][:3]}")
                
                last_dwmp = dwmp
                
                # Print periodically
                if msg_count % 50 == 0:
                    print(f"[{msg_count} msgs] DWMP={dwmp:.2f} "
                          f"bid={book.best_bid.price if book.best_bid else 'N/A'} "
                          f"ask={book.best_ask.price if book.best_ask else 'N/A'}")
                
            except asyncio.TimeoutError:
                continue
    
    print(f"\n\nTotal messages: {msg_count}")
    print(f"Spikes detected: {spike_count}")


if __name__ == "__main__":
    asyncio.run(main())
