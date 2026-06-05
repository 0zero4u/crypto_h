#!/usr/bin/env python3
"""Debug Delta Exchange data flow - check if data is reaching the strategy."""
import asyncio
import time
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.feed import DeltaFeed
from cryptofeed.orderbook import L2OrderBook
from cryptofeed.strategy import TradeCollector, GlobalFairValue, DivergenceTracker

# Track everything
stats = {
    "ob_l2": 0,
    "trades": 0,
    "dwmp_values": [],
    "gfv_values": [],
    "divergence_values": [],
}

trade_collector = TradeCollector(window_seconds=60)
gfv = GlobalFairValue(trade_collector)
div_tracker = DivergenceTracker(window_minutes=3)


def on_book(book: L2OrderBook):
    stats["ob_l2"] += 1
    
    dwmp = book.dwmp(n_levels=15)
    if dwmp:
        stats["dwmp_values"].append(dwmp)
        
        # Update GFV
        gfv.update_dwmp("delta", dwmp)
        result = gfv.compute()
        
        if result:
            gfv_val, weights = result
            stats["gfv_values"].append(gfv_val)
            
            # Compute divergence
            div_pct = (dwmp - gfv_val) / gfv_val * 100 if gfv_val != 0 else 0
            stats["divergence_values"].append(div_pct)
            
            # Record for rolling stats
            ts_ms = int(time.time() * 1000)
            div_tracker.record("delta", div_pct, ts_ms)
            
            # Check if we have enough data for stats
            div_stats = div_tracker.get_stats("delta")
            
            if stats["ob_l2"] <= 5 or stats["ob_l2"] % 100 == 0:
                print(f"[OB #{stats['ob_l2']}] DWMP={dwmp:.2f} GFV={gfv_val:.2f} "
                      f"Div={div_pct:.4f}% Weights={weights}")
                if div_stats:
                    mean, std = div_stats
                    print(f"  Rolling stats: mean={mean:.4f}% std={std:.4f}% "
                          f"samples={len(div_tracker._divergences.get('delta', []))}")


def on_trade(trade_data: dict):
    stats["trades"] += 1
    
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
    
    if stats["trades"] <= 3 or stats["trades"] % 50 == 0:
        vol = trade_collector.get_volume("delta")
        print(f"[TRADE #{stats['trades']}] {trade_data['side'].upper()} "
              f"{trade_data['qty']} @ {trade_data['price']} | "
              f"Rolling vol: ${vol:,.0f}")


async def main():
    print("=" * 60)
    print("DELTA EXCHANGE DEBUG - Data Flow Analysis")
    print("=" * 60)
    
    feed = DeltaFeed(
        symbols=["BTCUSD"],
        depth=20,
        on_book_update=on_book,
        on_trade=on_trade,
    )
    
    await feed.start()
    
    duration = 120  # 2 minutes
    print(f"\nRunning for {duration}s...\n")
    
    for i in range(duration):
        await asyncio.sleep(1)
        
        if i == 29:
            print(f"\n{'='*60}")
            print(f"30s CHECKPOINT")
            print(f"{'='*60}")
            print(f"OB updates: {stats['ob_l2']}")
            print(f"Trades: {stats['trades']}")
            print(f"DWMP samples: {len(stats['dwmp_values'])}")
            print(f"GFV samples: {len(stats['gfv_values'])}")
            print(f"Divergence samples: {len(stats['divergence_values'])}")
            
            div_stats = div_tracker.get_stats("delta")
            if div_stats:
                mean, std = div_stats
                print(f"\nRolling stats (delta):")
                print(f"  Mean divergence: {mean:.4f}%")
                print(f"  Std divergence: {std:.4f}%")
                print(f"  Samples: {len(div_tracker._divergences.get('delta', []))}")
                
                if std > 0:
                    # Check what z-score would be for latest divergence
                    if stats["divergence_values"]:
                        latest = stats["divergence_values"][-1]
                        z = (latest - mean) / std
                        print(f"\n  Latest divergence: {latest:.4f}%")
                        print(f"  Z-score: {z:.2f}")
                        print(f"  |Z| > 2? {abs(z) > 2}")
                        print(f"  |D| > 0.02%? {abs(latest) > 0.02}")
            else:
                print(f"\nNO ROLLING STATS YET (need 10 samples)")
                print(f"Divergence samples collected: {len(stats['divergence_values'])}")
            
            print(f"{'='*60}\n")
    
    await feed.stop()
    
    # Final summary
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"OB updates: {stats['ob_l2']}")
    print(f"Trades: {stats['trades']}")
    print(f"DWMP samples: {len(stats['dwmp_values'])}")
    print(f"GFV samples: {len(stats['gfv_values'])}")
    print(f"Divergence samples: {len(stats['divergence_values'])}")
    
    if stats["divergence_values"]:
        import numpy as np
        arr = np.array(stats["divergence_values"])
        print(f"\nDivergence stats:")
        print(f"  Mean: {arr.mean():.4f}%")
        print(f"  Std: {arr.std():.4f}%")
        print(f"  Min: {arr.min():.4f}%")
        print(f"  Max: {arr.max():.4f}%")
    
    div_stats = div_tracker.get_stats("delta")
    if div_stats:
        mean, std = div_stats
        print(f"\nRolling stats (3-min window):")
        print(f"  Mean: {mean:.4f}%")
        print(f"  Std: {std:.4f}%")
        
        if stats["divergence_values"]:
            latest = stats["divergence_values"][-1]
            z = (latest - mean) / std if std > 0 else 0
            print(f"\n  Latest divergence: {latest:.4f}%")
            print(f"  Z-score: {z:.2f}")
            print(f"  Signal? |Z|>2 AND |D|>0.02%: {abs(z) > 2 and abs(latest) > 0.02}")


if __name__ == "__main__":
    asyncio.run(main())
