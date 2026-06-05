#!/usr/bin/env python3
"""3-minute 4-exchange test with Delta logging."""
import asyncio
import logging
import sys
from datetime import datetime, timezone
from collections import Counter

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.strategy import Signal
from cryptofeed.orchestrator import DivergenceOrchestrator

signals = []


def on_signal(signal: Signal):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    entry = {
        "time": now,
        "exchange": signal.exchange,
        "direction": signal.direction,
        "z_score": round(signal.z_score, 2),
        "divergence_pct": round(signal.divergence_pct, 4),
        "dwmp": round(signal.dwmp, 2),
        "gfv": round(signal.gfv, 2),
    }
    signals.append(entry)
    
    # Print Delta signals and extreme signals
    if signal.exchange == "delta" or abs(signal.z_score) > 10:
        print(f"[{now}] {signal.direction.upper()} {signal.exchange} | "
              f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% | "
              f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}")


async def main():
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
    
    await orch.start()
    
    print("Running for 3 minutes...")
    
    for i in range(180):
        await asyncio.sleep(1)
        if i == 59:
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"\n--- 60s --- Total: {len(signals)} | Delta: {by_ex.get('delta', 0)} ---")
        elif i == 119:
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"\n--- 120s --- Total: {len(signals)} | Delta: {by_ex.get('delta', 0)} ---")
    
    await orch.stop()
    
    print(f"\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}")
    print(f"Total signals: {len(signals)}")
    
    by_ex = Counter(s["exchange"] for s in signals)
    print(f"\nBy exchange:")
    for ex, count in sorted(by_ex.items()):
        print(f"  {ex}: {count}")
    
    delta_signals = [s for s in signals if s["exchange"] == "delta"]
    if delta_signals:
        print(f"\nDelta signals:")
        for s in delta_signals:
            print(f"  {s['time']} {s['direction'].upper()} Z={s['z_score']} "
                  f"D={s['divergence_pct']}% DWMP={s['dwmp']} GFV={s['gfv']}")


if __name__ == "__main__":
    asyncio.run(main())
