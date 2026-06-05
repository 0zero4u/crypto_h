#!/usr/bin/env python3
"""Live run: Delta Exchange + Binance + Bybit divergence detection."""
import asyncio
import time
import json
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.strategy import Signal
from cryptofeed.orchestrator import DivergenceOrchestrator

RUN_DURATION = 120
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
    
    print(f"[{now}] SIGNAL #{len(signals)}: {signal.direction.upper()} {signal.exchange} | "
          f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% | "
          f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}")


async def main():
    global start_time
    
    print("=" * 60)
    print("DIVERGENCE DETECTION: Delta + Binance + Bybit")
    print(f"Duration: {RUN_DURATION}s")
    print("=" * 60)
    
    orch = DivergenceOrchestrator(
        symbols=["BTCUSDT"],         # Binance/Bybit/Gate.io symbol
        depth=20,
        n_levels=15,
        z_threshold=2.0,
        min_divergence_pct=0.02,
        on_signal=on_signal,
        use_gateio=True,
        use_delta=True,
        delta_symbols=["BTCUSD"],    # Delta Exchange symbol
    )
    
    await orch.start()
    start_time = time.time()
    
    print("\nWarming up (30s for baseline)...\n")
    
    # Status updates
    for i in range(RUN_DURATION):
        await asyncio.sleep(1)
        if i == 29:
            state = orch.get_state()
            print(f"\n--- 30s WARMUP COMPLETE ---")
            print(f"Exchanges connected: {list(state['dwmps'].keys())}")
            print(f"GFV: {state['gfv']}")
            print(f"Signals so far: {len(signals)}\n")
        elif i == 59:
            print(f"\n--- 60s --- Signals: {len(signals)} ---\n")
        elif i == 89:
            print(f"\n--- 90s --- Signals: {len(signals)} ---\n")
    
    await orch.stop()
    
    # Results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Duration: {RUN_DURATION}s")
    print(f"Total signals: {len(signals)}")
    
    if signals:
        by_exchange = {}
        for s in signals:
            ex = s["exchange"]
            by_exchange[ex] = by_exchange.get(ex, 0) + 1
        
        print("\nSignals by exchange:")
        for ex, count in sorted(by_exchange.items()):
            print(f"  {ex}: {count}")
        
        profitable = [s for s in signals if s["net_divergence_pct"] > 0]
        print(f"\nProfitable (net > 0): {len(profitable)} / {len(signals)}")
        
        if signals:
            best = max(signals, key=lambda s: s["net_divergence_pct"])
            print(f"\nBest signal: {best['net_divergence_pct']:.4f}% "
                  f"({best['direction']} {best['exchange']})")
    
    # Save results
    with open("/home/arshhtripathi/crypto_h/delta_run_results.json", "w") as f:
        json.dump({"signals": signals}, f, indent=2)
    print(f"\nResults saved to: delta_run_results.json")


if __name__ == "__main__":
    asyncio.run(main())
