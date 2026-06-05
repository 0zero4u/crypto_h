"""
Live runner: connects to Binance, Bybit, Gate.io and tracks divergence signals.
Logs signals with hypothetical P&L (buy cheap exchange, sell expensive).
"""
import asyncio
import time
import logging
import json
from datetime import datetime, timezone
from collections import deque

import sys
sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.strategy import Signal
from cryptofeed.orchestrator import DivergenceOrchestrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Track signals and hypothetical trades
signals_log = []
trades_log = []
RUN_DURATION_SECONDS = 300  # 5 minutes


def on_signal(signal: Signal):
    """Handle a divergence signal."""
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    
    entry = {
        "time": now,
        "exchange": signal.exchange,
        "direction": signal.direction,
        "z_score": round(signal.z_score, 2),
        "divergence_pct": round(signal.divergence_pct, 4),
        "net_divergence_pct": round(signal.net_divergence_pct, 4),
        "dwmp": round(signal.dwmp, 2),
        "gfv": round(signal.gfv, 2),
    }
    signals_log.append(entry)
    
    # Log the signal
    logger.info(
        f"SIGNAL #{len(signals_log)}: {signal.direction.upper()} {signal.exchange} | "
        f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% "
        f"Net={signal.net_divergence_pct:.4f}% | "
        f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}"
    )
    
    # Hypothetical trade: if exchange is cheap (long signal), buy there and sell at GFV
    # if exchange is rich (short signal), sell there and buy at GFV
    if signal.direction == "long":
        # Buy at exchange DWMP, sell at GFV
        profit_pct = (signal.gfv - signal.dwmp) / signal.dwmp * 100
        side = f"BUY {signal.exchange} @ {signal.dwmp:.2f}, SELL GFV @ {signal.gfv:.2f}"
    else:
        # Sell at exchange DWMP, buy at GFV
        profit_pct = (signal.dwmp - signal.gfv) / signal.gfv * 100
        side = f"SELL {signal.exchange} @ {signal.dwmp:.2f}, BUY GFV @ {signal.gfv:.2f}"
    
    net_profit = profit_pct - 0.06  # 0.06% round-trip fee
    
    trade_entry = {
        "time": now,
        "direction": signal.direction,
        "exchange": signal.exchange,
        "profit_pct": round(profit_pct, 4),
        "net_profit_pct": round(net_profit, 4),
        "side": side,
    }
    trades_log.append(trade_entry)
    
    logger.info(
        f"  TRADE: {side} | Gross={profit_pct:.4f}% Net={net_profit:.4f}%"
    )


async def print_status(orch: DivergenceOrchestrator):
    """Print periodic status updates."""
    while True:
        await asyncio.sleep(10)
        state = orch.get_state()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        
        dwmps = state.get("dwmps", {})
        gfv = state.get("gfv")
        volumes = state.get("volumes", {})
        
        if dwmps and gfv:
            logger.info(f"[STATUS] {now} | GFV={gfv:.2f}")
            logger.info(f"  {'Exchange':<10} {'DWMP':>12} {'GFV':>12} {'Diff':>10} {'Div%':>10} {'Volume':>15}")
            logger.info(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*10} {'-'*10} {'-'*15}")
            for ex, dwmp in sorted(dwmps.items()):
                vol = volumes.get(ex, 0)
                diff = dwmp - gfv
                div = (diff / gfv * 100) if gfv else 0
                logger.info(f"  {ex:<10} {dwmp:>12.2f} {gfv:>12.2f} {diff:>+10.2f} {div:>+10.4f}% {vol:>15,.0f}")
        
        logger.info(f"  Signals: {len(signals_log)} | Trades: {len(trades_log)}")


async def main():
    logger.info("=" * 60)
    logger.info("LIVE DIVERGENCE DETECTION RUN")
    logger.info(f"Duration: {RUN_DURATION_SECONDS}s | Exchanges: Binance, Bybit, Gate.io, Delta")
    logger.info("=" * 60)
    
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
    
    # Start orchestrator
    await orch.start()
    
    # Start status printer
    status_task = asyncio.create_task(print_status(orch))
    
    # Let it run
    logger.info("Connected. Waiting for data to stabilize (30s warmup)...")
    await asyncio.sleep(RUN_DURATION_SECONDS)
    
    # Stop
    status_task.cancel()
    await orch.stop()
    
    # Print summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"Run duration: {RUN_DURATION_SECONDS}s")
    print(f"Total signals: {len(signals_log)}")
    print(f"Total hypothetical trades: {len(trades_log)}")
    
    if trades_log:
        profitable = [t for t in trades_log if t["net_profit_pct"] > 0]
        losing = [t for t in trades_log if t["net_profit_pct"] <= 0]
        
        total_gross = sum(t["profit_pct"] for t in trades_log)
        total_net = sum(t["net_profit_pct"] for t in trades_log)
        
        print(f"\nProfitable trades: {len(profitable)}")
        print(f"Losing trades: {len(losing)}")
        print(f"Win rate: {len(profitable)/len(trades_log)*100:.1f}%")
        print(f"Total gross profit: {total_gross:.4f}%")
        print(f"Total net profit (after 0.06% fee): {total_net:.4f}%")
        print(f"Avg profit per trade: {total_net/len(trades_log):.4f}%")
        
        if profitable:
            best = max(trades_log, key=lambda t: t["net_profit_pct"])
            print(f"\nBest trade: {best['net_profit_pct']:.4f}% ({best['direction']} {best['exchange']})")
        
        print("\n--- All Signals ---")
        for i, s in enumerate(signals_log):
            t = trades_log[i]
            print(f"  #{i+1} {s['time']} | {s['direction'].upper()} {s['exchange']} | "
                  f"Z={s['z_score']} D={s['divergence_pct']}% | Net={t['net_profit_pct']}%")
    else:
        print("\nNo signals generated during this run.")
        print("This could mean:")
        print("  - Markets are calm (no significant divergences)")
        print("  - Need longer run time to establish baseline (3 min warmup)")
        print("  - Try lowering z_threshold or min_divergence_pct")
    
    # Save raw data
    output = {
        "run_time": datetime.now(timezone.utc).isoformat(),
        "duration_s": RUN_DURATION_SECONDS,
        "signals": signals_log,
        "trades": trades_log,
    }
    with open("/home/arshhtripathi/crypto_h/run_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nRaw data saved to: /home/arshhtripathi/crypto_h/run_results.json")


if __name__ == "__main__":
    asyncio.run(main())
