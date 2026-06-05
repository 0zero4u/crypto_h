#!/usr/bin/env python3
"""Debug Delta divergence values."""
import asyncio
import logging
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.feed import DeltaFeed
from cryptofeed.orderbook import L2OrderBook
from cryptofeed.strategy import TradeCollector, GlobalFairValue, DivergenceTracker

# Track Delta's divergence
divergence_samples = []
trade_collector = TradeCollector(window_seconds=60)
gfv = GlobalFairValue(trade_collector)
div_tracker = DivergenceTracker(window_minutes=3)

# Also track other exchanges
other_dwmps = {}


def on_book(book: L2OrderBook, exchange: str):
    dwmp = book.dwmp(n_levels=15)
    if dwmp is None:
        return
    
    other_dwmps[exchange] = dwmp
    
    # Update GFV with all exchanges
    gfv.update_dwmp(exchange, dwmp)
    result = gfv.compute()
    
    if result:
        gfv_val, weights = result
        
        # Compute Delta's divergence
        delta_dwmp = other_dwmps.get("delta")
        if delta_dwmp:
            div_pct = (delta_dwmp - gfv_val) / gfv_val * 100 if gfv_val != 0 else 0
            ts_ms = int(time.time() * 1000)
            div_tracker.record("delta", div_pct, ts_ms)
            
            div_stats = div_tracker.get_stats("delta")
            
            if len(divergence_samples) < 10 or len(divergence_samples) % 100 == 0:
                print(f"[{exchange}] DWMP={dwmp:.2f} GFV={gfv_val:.2f}")
                print(f"  Delta DWMP={delta_dwmp:.2f} Div={div_pct:.4f}%")
                if div_stats:
                    mean, std = div_stats
                    z = (div_pct - mean) / std if std > 0 else 0
                    print(f"  Rolling: mean={mean:.4f}% std={std:.4f}% z={z:.2f}")
                    print(f"  Signal? |Z|>2={abs(z)>2} |D|>0.02%={abs(div_pct)>0.02}")
            
            divergence_samples.append({
                "time": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
                "exchange": exchange,
                "dwmp": dwmp,
                "gfv": gfv_val,
                "delta_dwmp": delta_dwmp,
                "div_pct": div_pct,
                "mean": div_stats[0] if div_stats else None,
                "std": div_stats[1] if div_stats else None,
                "z": (div_pct - div_stats[0]) / div_stats[1] if div_stats and div_stats[1] > 0 else 0,
            })


def on_delta_book(book: L2OrderBook):
    on_book(book, "delta")


def on_other_book(book: L2OrderBook):
    # Detect which exchange from the book object
    # This is a simplification - in real code we'd need proper detection
    pass


def on_trade(trade_data: dict):
    from cryptofeed.strategy import Trade
    trade = Trade(
        symbol=trade_data["symbol"],
        exchange=trade_data["exchange"],
        price=trade_data["price"],
        qty=trade_data["qty"],
        ts_ms=trade_data["ts_ms"],
        side=trade_data["side"],
        volume=trade_data.get("volume", 0.0),
    )
    trade_collector.add_trade(trade)


async def main():
    print("=" * 60)
    print("DELTA DIVERGENCE DEBUG")
    print("=" * 60)
    
    # Only run Delta feed to see its raw divergence
    delta_feed = DeltaFeed(
        symbols=["BTCUSD"],
        depth=20,
        on_book_update=on_delta_book,
        on_trade=on_trade,
    )
    
    await delta_feed.start()
    
    print("\nRunning for 60s...\n")
    
    for i in range(60):
        await asyncio.sleep(1)
        
        if i == 29:
            print(f"\n--- 30s ---")
            print(f"Samples: {len(divergence_samples)}")
            if divergence_samples:
                last = divergence_samples[-1]
                print(f"Last: div={last['div_pct']:.4f}% z={last['z']:.2f}")
    
    await delta_feed.stop()
    
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Total samples: {len(divergence_samples)}")
    
    if divergence_samples:
        import numpy as np
        divs = [s['div_pct'] for s in divergence_samples]
        zs = [s['z'] for s in divergence_samples if s['z'] is not None]
        
        print(f"\nDivergence stats:")
        print(f"  Mean: {np.mean(divs):.4f}%")
        print(f"  Std: {np.std(divs):.4f}%")
        print(f"  Min: {np.min(divs):.4f}%")
        print(f"  Max: {np.max(divs):.4f}%")
        
        print(f"\nZ-score stats:")
        print(f"  Mean: {np.mean(zs):.4f}")
        print(f"  Std: {np.std(zs):.4f}")
        print(f"  Min: {np.min(zs):.4f}")
        print(f"  Max: {np.max(zs):.4f}")
        
        # Count how many would trigger signals
        signal_count = sum(1 for s in divergence_samples 
                         if s['z'] is not None and abs(s['z']) > 2 and abs(s['div_pct']) > 0.02)
        print(f"\nPotential signals (|Z|>2 AND |D|>0.02%): {signal_count}")


if __name__ == "__main__":
    asyncio.run(main())
