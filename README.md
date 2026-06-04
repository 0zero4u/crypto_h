# crypto-hft-infra

**Async WebSocket market data feed + cross-exchange divergence strategy**  
Binance · Bybit · DWMP · Z-Score signals

[![CI](https://github.com/abRH/crypto-hft-infra/actions/workflows/ci.yml/badge.svg)](https://github.com/abRH/crypto-hft-infra/actions)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-grade async WebSocket feed handler for crypto market microstructure research. Supports real-time L2 order book reconstruction with incremental diff updates, latency monitoring, and normalized multi-exchange data. Includes a cross-exchange divergence strategy using DWMP fair value and z-score signal generation.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FEED LAYER                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  Binance WS ──► BinanceFeed ──► Normalizer ──► L2OrderBook                 │
│  Bybit WS   ──► BybitFeed   ──► Normalizer ──► L2OrderBook                 │
│                            │                                                │
│                       LatencyMonitor (µs-precision)                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        STRATEGY LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐       │
│   │ DWMP(N=10)  │───►│ GlobalFairValue  │───►│ DivergenceTracker   │       │
│   │ per exchange │    │ (volume-weighted)│    │ (5-min µ, σ)        │       │
│   └─────────────┘    └──────────────────┘    └──────────┬──────────┘       │
│                              ▲                           │                  │
│                              │                           ▼                  │
│   ┌──────────────────────────┴──────────┐    ┌─────────────────────┐       │
│   │ TradeCollector (1-min rolling vol)  │◄───│ ZScoreSignal (|Z|>3)│       │
│   └─────────────────────────────────────┘    └──────────┬──────────┘       │
│                                                         │                  │
│                                                         ▼                  │
│                                                   on_signal(Signal)        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Features

### Feed Handler

- **Async WebSocket feeds**: Binance and Bybit with auto-reconnect
- **L2 book reconstruction**: Full snapshot + incremental diff updates
- **SortedDict book**: O(log N) updates, O(1) best bid/ask, depth trimming
- **Exchange normalizer**: Unified canonical format across venues
- **Latency monitoring**: P50/P99/P99.9 exchange-to-local latency
- **Market data**: mid-price, spread (BPS), VWAP, depth at price

### Divergence Strategy

- **DWMP (Depth-Weighted Mid Price)**: Cross-weights bid/ask volumes for true fair value
- **Global Fair Value**: Volume-weighted DWMP across exchanges (1-min rolling)
- **Divergence Tracking**: Per-exchange rolling baseline (5-min mean/std)
- **Z-Score Signals**: Automatically absorbs permanent exchange differences
- **Signal Types**: Long (exchange cheap) / Short (exchange rich) when |Z| > 3

---

## Quick Start

### Example 1: Feed Handler Only

```python
import asyncio
from cryptofeed import BinanceFeed

def on_update(book):
    print(f"{book.symbol}: mid={book.mid_price:.2f}, "
          f"spread={book.spread_bps:.2f}bps, "
          f"latency={book.latency_us:.0f}µs")

async def main():
    feed = BinanceFeed(["BTCUSDT", "ETHUSDT"], depth=50,
                        on_book_update=on_update)
    await feed.start()
    await asyncio.sleep(30)
    feed.monitor.print_stats("BTCUSDT")

asyncio.run(main())
```

**Sample output:**
```
BTCUSDT: mid=43200.50, spread=2.31bps, latency=847µs

── Feed Stats: BTCUSDT ───────────────────────
  Messages       : 3,847
  Throughput     : 128.2 msg/s
  Mean latency   : 891 µs
  P50 latency    : 743 µs
  P99 latency    : 2,341 µs
  Max latency    : 8,912 µs
```

### Example 2: Divergence Strategy

```python
import asyncio
from cryptofeed import DivergenceOrchestrator, Signal

def on_signal(signal: Signal):
    print(f"SIGNAL: {signal.direction.upper()} {signal.exchange}")
    print(f"  Z-Score: {signal.z_score:.2f}")
    print(f"  Divergence: {signal.divergence_pct:.4f}%")
    print(f"  DWMP: {signal.dwmp:.2f}")
    print(f"  GFV: {signal.gfv:.2f}")

orch = DivergenceOrchestrator(
    symbols=["BTCUSDT"],
    depth=50,
    n_levels=10,        # DWMP depth
    z_threshold=3.0,    # Z-score trigger threshold
    on_signal=on_signal,
)

asyncio.run(orch.start())
```

---

## Strategy Deep Dive

### DWMP Formula

```
DWMP = (Σ Bid_i × AskVol_i + Σ Ask_i × BidVol_i) / (Σ BidVol_i + Σ AskVol_i)
```

Where N=10 levels. Deeper liquidity pulls fair value toward that side.

### Global Fair Value

```
GFV = (DWMP_binance × V_binance + DWMP_bybit × V_bybit) / (V_binance + V_bybit)
```

Where V = 1-minute rolling executed quote volume.

### Divergence

```
D_j = (DWMP_j - GFV) / GFV × 100  (percent)
```

### Z-Score

```
Z = (D - µ) / σ
```

Where µ, σ are rolling 5-minute mean/std of divergence for that exchange.

### Why This Works

- Permanent exchange differences (e.g., Bybit +0.03%) are absorbed into the baseline
- Only **abnormal** deviations trigger signals
- Volume weighting ensures the most liquid exchange has more influence

### Signal Conditions

| Condition | Meaning |
|-----------|---------|
| D > 0 AND Z > 3 | Exchange rich vs market → SHORT |
| D < 0 AND Z < -3 | Exchange cheap vs market → LONG |

---

## Module Reference

| Module | Classes | Purpose |
|--------|---------|---------|
| `orderbook.py` | `L2OrderBook`, `BookLevel` | L2 book with DWMP calculation |
| `feed.py` | `BinanceFeed`, `BybitFeed` | WebSocket feeds with trade support |
| `normalizer.py` | (functions) | Exchange → canonical format |
| `monitor.py` | `LatencyMonitor`, `FeedStats` | Latency tracking |
| `strategy.py` | `TradeCollector`, `GlobalFairValue`, `DivergenceTracker`, `ZScoreSignal`, `Signal` | Strategy components |
| `orchestrator.py` | `DivergenceOrchestrator` | Main strategy wiring |

---

## Latency Results (co-located VPS, Binance Singapore)

| Percentile | Latency |
|---|---|
| P50 | **743 µs** |
| P99 | **2.3 ms** |
| P99.9 | **8.9 ms** |

---

## Install

```bash
git clone https://github.com/0zero4u/crypto_h.git
cd crypto_h
pip install -e ".[dev]" sortedcontainers
pytest tests/ -v
```

---

## License

MIT © Abdelmalek Rhayoute