#!/usr/bin/env python3
"""4-exchange divergence test with Delta - 5 minute run."""
import asyncio
import time
import json
import sys
from datetime import datetime, timezone
from collections import Counter

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.strategy import Signal
from cryptofeed.orchestrator import DivergenceOrchestrator

RUN_DURATION = 300
signals = []
start_time = None


def on_signal(signal: Signal):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    elapsed = time.time() - start_time if start_time else 0
    
    entry = {
        "time": now,
        "elapsed_s": round(elapsed, 1),
        "exchange": signal.exchange,
        "direction": signal.direction,
        "z_score": round(signal.z_score, 2),
        "divergence_pct": round(signal.divergence_pct, 4),
        "net_divergence_pct": round(signal.net_divergence_pct, 4),
        "dwmp": round(signal.dwmp, 2),
        "gfv": round(signal.gfv, 2),
    }
    signals.append(entry)
    
    # Only print Delta signals to see if they appear
    if signal.exchange == "delta" or len(signals) <= 10 or len(signals) % 100 == 0:
        print(f"[{now}] #{len(signals)} {signal.direction.upper()} {signal.exchange} | "
              f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% | "
              f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}")


async def main():
    global start_time
    
    print("=" * 60)
    print("4-EXCHANGE DIVERGENCE TEST (5 min)")
    print("Binance + Bybit + Gate.io (BTCUSDT) + Delta (BTCUSD)")
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
    
    await orch.start()
    start_time = time.time()
    
    print("\nWarming up 30s for baseline...\n")
    
    for i in range(RUN_DURATION):
        await asyncio.sleep(1)
        
        if i == 29:
            state = orch.get_state()
            print(f"\n--- 30s WARMUP ---")
            print(f"Exchanges: {list(state['dwmps'].keys())}")
            print(f"GFV: {state.get('gfv', 'N/A')}")
            
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"Signals by exchange: {dict(by_ex)}")
            print(f"Delta signals: {by_ex.get('delta', 0)}\n")
        
        elif i == 59:
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"\n--- 60s --- Total: {len(signals)} | Delta: {by_ex.get('delta', 0)} ---\n")
        
        elif i == 149:
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"\n--- 150s --- Total: {len(signals)} | Delta: {by_ex.get('delta', 0)} ---\n")
        
        elif i == 239:
            by_ex = Counter(s["exchange"] for s in signals)
            print(f"\n--- 240s --- Total: {len(signals)} | Delta: {by_ex.get('delta', 0)} ---\n")
    
    await orch.stop()
    
    # Final results
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Duration: {RUN_DURATION}s")
    print(f"Total signals: {len(signals)}")
    
    by_ex = Counter(s["exchange"] for s in signals)
    print(f"\nSignals by exchange:")
    for ex, count in sorted(by_ex.items()):
        print(f"  {ex}: {count}")
    
    delta_signals = [s for s in signals if s["exchange"] == "delta"]
    print(f"\nDelta Exchange signals: {len(delta_signals)}")
    
    if delta_signals:
        print(f"\nDelta signal details:")
        for s in delta_signals[:10]:
            print(f"  {s['time']} {s['direction'].upper()} Z={s['z_score']} "
                  f"D={s['divergence_pct']}% Net={s['net_divergence_pct']}%")
        if len(delta_signals) > 10:
            print(f"  ... and {len(delta_signals) - 10} more")
    
    profitable = [s for s in signals if s["net_divergence_pct"] > 0]
    print(f"\nProfitable signals: {len(profitable)} / {len(signals)}")
    
    if signals:
        best = max(signals, key=lambda s: s["net_divergence_pct"])
        print(f"Best: {best['net_divergence_pct']:.4f}% ({best['direction']} {best['exchange']})")
    
    # Save
    with open("/home/arshhtripathi/crypto_h/delta_4ex_results.json", "w") as f:
        json.dump({"signals": signals}, f, indent=2)
    print(f"\nSaved to: delta_4ex_results.json")


if __name__ == "__main__":
    asyncio.run(main())
