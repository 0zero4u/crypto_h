#!/usr/bin/env python3
"""Debug 4-exchange - track Delta's divergence vs others."""
import asyncio
import logging
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.strategy import Signal, Trade, TradeCollector, GlobalFairValue, DivergenceTracker
from cryptofeed.orchestrator import DivergenceOrchestrator

# Track all exchanges
exchange_dwmps = {}
exchange_divs = {}
div_tracker = DivergenceTracker(window_minutes=3)
gfv_history = []


def on_signal(signal: Signal):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    
    # Print ALL Delta signals and extreme signals
    if signal.exchange == "delta" or abs(signal.z_score) > 5:
        print(f"[{now}] {signal.direction.upper()} {signal.exchange} | "
              f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% | "
              f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}")


async def main():
    print("=" * 60)
    print("4-EXCHANGE DEBUG - Track Delta Divergence")
    print("=" * 60)
    
    orch = DivergenceOrchestrator(
        symbols=["BTCUSDT"],
        depth=20,
        n_levels=20,
        z_threshold=2.0,
        min_divergence_pct=0.02,
        on_signal=on_signal,
        use_gateio=True,
        use_delta=True,
        delta_symbols=["BTCUSD"],
    )
    
    # Monkey-patch to track Delta's divergence
    original_handle_book = orch._handle_book
    
    def tracked_handle_book(book):
        original_handle_book(book)
        
        # Track which exchange this book belongs to
        exchange = orch._detect_exchange(book)
        if exchange:
            dwmp = book.dwmp(n_levels=20)
            if dwmp:
                exchange_dwmps[exchange] = dwmp
                
                # Log Delta's state periodically
                if exchange == "delta" and len(gfv_history) % 50 == 0:
                    result = orch.gfv.compute()
                    if result:
                        gfv_val, weights = result
                        div_pct = (dwmp - gfv_val) / gfv_val * 100 if gfv_val != 0 else 0
                        stats = orch.divergence_tracker.get_stats("delta")
                        print(f"[DEBUG] Delta: DWMP={dwmp:.2f} GFV={gfv_val:.2f} "
                              f"Div={div_pct:.4f}% Weights={weights}")
                        if stats:
                            mean, std = stats
                            z = (div_pct - mean) / std if std > 0 else 0
                            print(f"  Rolling: mean={mean:.4f}% std={std:.4f}% z={z:.2f}")
    
    orch._handle_book = tracked_handle_book
    
    await orch.start()
    
    print("\nRunning for 2 minutes...\n")
    
    for i in range(120):
        await asyncio.sleep(1)
        
        if i == 29:
            print(f"\n--- 30s ---")
            print(f"Exchanges connected: {list(exchange_dwmps.keys())}")
            print(f"DWMPs: {exchange_dwmps}")
        
        elif i == 59:
            print(f"\n--- 60s ---")
            print(f"DWMPs: {exchange_dwmps}")
            
            # Check Delta's divergence stats
            stats = orch.divergence_tracker.get_stats("delta")
            if stats:
                mean, std = stats
                print(f"Delta rolling stats: mean={mean:.4f}% std={std:.4f}%")
            else:
                print(f"Delta rolling stats: None (need 10 samples)")
    
    await orch.stop()
    
    print(f"\n{'='*60}")
    print("FINAL ANALYSIS")
    print(f"{'='*60}")
    print(f"Exchanges: {list(exchange_dwmps.keys())}")
    print(f"DWMPs: {exchange_dwmps}")
    
    # Check if Delta's price is different from others
    delta_dwmp = exchange_dwmps.get("delta")
    other_dwmps = {k: v for k, v in exchange_dwmps.items() if k != "delta"}
    
    if delta_dwmp and other_dwmps:
        avg_other = sum(other_dwmps.values()) / len(other_dwmps)
        diff_pct = (delta_dwmp - avg_other) / avg_other * 100
        print(f"\nDelta vs others:")
        print(f"  Delta DWMP: {delta_dwmp:.2f}")
        print(f"  Avg other DWMP: {avg_other:.2f}")
        print(f"  Difference: {diff_pct:.4f}%")
    
    # Check Delta's divergence stats
    stats = orch.divergence_tracker.get_stats("delta")
    if stats:
        mean, std = stats
        print(f"\nDelta divergence stats:")
        print(f"  Mean: {mean:.4f}%")
        print(f"  Std: {std:.4f}%")
        print(f"  Samples: {len(orch.divergence_tracker._divergences.get('delta', []))}")
    else:
        print(f"\nNo Delta divergence stats (insufficient samples)")


if __name__ == "__main__":
    asyncio.run(main())
