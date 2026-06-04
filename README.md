# crypto-hft-infra

**Async WebSocket market data feed + cross-exchange divergence strategy**  
Binance · Bybit · Gate.io · Delta Exchange · DWMP · Z-Score signals

[![CI](https://github.com/abRH/crypto-hft-infra/actions/workflows/ci.yml/badge.svg)](https://github.com/abRH/crypto-hft-infra/actions)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-grade async WebSocket feed handler for crypto market microstructure research. Supports real-time L2 order book reconstruction with incremental diff updates, latency monitoring, and normalized multi-exchange data. Includes a cross-exchange divergence strategy using DWMP fair value and z-score signal generation with fee-adjusted filtering.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FEED LAYER (All Futures)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  Binance (fstream.binance.com) ──► BinanceFeed ──► Normalizer ──► L2Book   │
│  Bybit (stream.bybit.com/v5/public/linear) ──► BybitFeed ──► Normalizer    │
│  Gate.io (fx-ws.gateio.ws/v4/ws/usdt) ──► GateIoFeed ──► Normalizer        │
│  Delta (public-socket.india.delta.exchange) ──► DeltaFeed ──► Normalizer    │
│                (REST snapshot + delta merge with sequence sync)             │
│                            │                                                │
│                       LatencyMonitor (µs-precision)                         │
│                       CRC32 Checksum Validation (Delta)                     │
│                       Price-Sanity Checks (5% deviation)                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        STRATEGY LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐       │
│   │ DWMP(N=20)  │───►│ GlobalFairValue  │───►│ DivergenceTracker   │       │
│   │ per exchange │    │ (volume-weighted)│    │ (3-min µ, σ)        │       │
│   └─────────────┘    └──────────────────┘    └──────────┬──────────┘       │
│                              ▲                           │                  │
│                              │                           ▼                  │
│   ┌──────────────────────────┴──────────┐    ┌─────────────────────┐       │
│   │ TradeCollector (1-min rolling vol)  │◄───│ ZScoreSignal        │       │
│   └─────────────────────────────────────┘    │ |Z|>3 AND |D|>0.07%│       │
│                                              └──────────┬──────────┘       │
│                                                         │                  │
│                                                         ▼                  │
│                                              on_signal(Signal)             │
│                                              net_divergence_pct            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Features

### Feed Handler

- **Async WebSocket feeds**: Binance, Bybit, Gate.io, and Delta Exchange futures with auto-reconnect
- **L2 book reconstruction**: Full snapshot + incremental diff updates
- **Gate.io sync**: REST snapshot + WebSocket delta merge with sequence ID tracking
- **Delta Exchange sync**: CRC32 checksum validation, contract-to-base conversion
- **SortedDict book**: O(log N) updates, O(1) best bid/ask, depth trimming
- **Exchange normalizer**: Unified canonical format across venues
- **Latency monitoring**: P50/P99/P99.9 exchange-to-local latency
- **Market data**: mid-price, spread (BPS), VWAP, depth at price
- **Price-sanity checks**: Rejects levels >5% from current best price
- **Sequence validation**: Detects duplicate/corrupted sequence numbers

### Divergence Strategy

- **DWMP (Depth-Weighted Mid Price)**: Cross-weights bid/ask volumes for true fair value
- **Global Fair Value**: Volume-weighted DWMP across exchanges (1-min rolling)
- **Divergence Tracking**: Per-exchange rolling baseline (3-min mean/std)
- **Z-Score Signals**: Automatically absorbs permanent exchange differences
- **Fee-Adjusted Filtering**: Only signals where |D| > 0.07% AND |Z| > 3
- **Net Profitability**: Each signal includes `net_divergence_pct` after 0.06% round-trip fee

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
    feed = BinanceFeed(["BTCUSDT"], depth=20, on_book_update=on_update)
    await feed.start()
    await asyncio.sleep(30)
    feed.monitor.print_stats("BTCUSDT")

asyncio.run(main())
```

### Example 2: Divergence Strategy (4 exchanges)

```python
import asyncio
from cryptofeed import DivergenceOrchestrator, Signal

def on_signal(signal: Signal):
    print(f"SIGNAL: {signal.direction.upper()} {signal.exchange}")
    print(f"  Z-Score: {signal.z_score:.2f}")
    print(f"  Divergence: {signal.divergence_pct:.4f}%")
    print(f"  Net (after fees): {signal.net_divergence_pct:.4f}%")

orch = DivergenceOrchestrator(
    symbols=["BTCUSDT"],
    depth=20,
    n_levels=20,
    z_threshold=3.0,
    min_divergence_pct=0.07,
    on_signal=on_signal,
    use_gateio=True,
    use_delta=True,
    delta_symbols=["BTCUSD"],  # Delta uses BTCUSD not BTCUSDT
)

asyncio.run(orch.start())
```

---

## Exchange Endpoints

| Exchange | Endpoint | Market | Trade Stream |
|----------|----------|--------|--------------|
| Binance | `fstream.binance.com` | USDT-M Futures | `@trade` (individual) |
| Bybit | `stream.bybit.com/v5/public/linear` | USDT Perpetual | `publicTrade` |
| Gate.io | `fx-ws.gateio.ws/v4/ws/usdt` | USDT Futures | `futures.trades` |
| Delta | `public-socket.india.delta.exchange` | USD Futures | `trades` |

All exchanges use **futures** markets with **individual trade** streams for fair comparison.

---

## Strategy Deep Dive

### DWMP Formula

```
DWMP = (Σ Bid_i × AskVol_i + Σ Ask_i × BidVol_i) / (Σ BidVol_i + Σ AskVol_i)
```

Where N=20 levels. Deeper liquidity pulls fair value toward that side.

### Global Fair Value

```
GFV = Σ(DWMP_j × V_j) / Σ(V_j)
```

Where V_j = 1-minute rolling executed quote volume per exchange.

### Divergence

```
D_j = (DWMP_j - GFV) / GFV × 100  (percent)
```

### Z-Score

```
Z = (D - µ) / σ
```

Where µ, σ are rolling 3-minute mean/std of divergence for that exchange.

### Signal Conditions (Fee-Adjusted)

Signals require **both** conditions:

| Condition | Purpose |
|-----------|---------|
| \|Z\| > 3 | Statistical significance (99.7% confidence) |
| \|D\| > 0.07% | Covers 0.06% round-trip fee + buffer |

### Why Both Filters?

- **Z-threshold alone fails**: In low-volatility (σ=0.008%), Z=3 means only 0.028% divergence — doesn't cover fees
- **Min divergence alone fails**: 0.10% divergence might be normal (Z=1) in high-volatility — likely to mean-revert
- **Both together**: Z ensures the move is unusual, min divergence ensures profitability

### Signal Fields

| Field | Description |
|-------|-------------|
| `divergence_pct` | Raw divergence from GFV |
| `z_score` | Standard deviations from baseline |
| `net_divergence_pct` | Divergence after 0.06% round-trip fee |

---

## Data Integrity

### CRC32 Checksum Validation (Delta Exchange)

Delta Exchange provides a `cs` (checksum) field in every `ob_updates` message for data integrity verification.

**Calculation:**
1. Take top 10 bid/ask levels (sorted: asks ascending, bids descending)
2. Format: `price0:size0,price1:size1,...,price9:size9`
3. Concatenate: `asks_string + "|" + bids_string`
4. Calculate CRC32 (unsigned 32-bit integer)

**Example:**
```
asks = [["100.00", "23"], ["100.05", "34"]]
bids = [["99.04", "87"], ["98.65", "102"], ["98.30", "16"]]
checksum_string = "100.00:23,100.05:34|99.04:87,98.65:102,98.30:16"
CRC32 = 3599895312
```

### Price-Sanity Validation

Rejects orderbook levels that deviate >5% from current best price on the same side.

**Why?** A corrupted delta with a valid sequence number can pass sequence checks unchecked, overwriting book state with wrong prices. The price-sanity check catches this.

**Example:**
```
Current best_bid: 63500.0
Corrupted level: 48000.0 (24% deviation)
Result: REJECTED with warning
```

### Sequence Validation

Tracks `_last_seq` and rejects updates with `seq <= last_seq`.

**Why?** Prevents duplicate/corrupted sequence number replays from corrupting book state.

### Contract Size Conversion (Delta Exchange)

Delta Exchange trades use contracts, not base asset quantity.

**BTCUSD:** 1 contract = 0.001 BTC

**Example:**
```
Trade: "s": 40.0 (40 contracts)
Base asset: 40 * 0.001 = 0.04 BTC
Volume: 0.04 BTC * $63,000 = $2,520
```

Without conversion, volume would be inflated 1000x (40 * $63,000 = $2,520,000).

---

## Module Reference

| Module | Classes | Purpose |
|--------|---------|---------|
| `orderbook.py` | `L2OrderBook`, `BookLevel` | L2 book with DWMP calculation, CRC32 checksum, price-sanity checks |
| `feed.py` | `BinanceFeed`, `BybitFeed`, `GateIoFeed`, `DeltaFeed` | WebSocket feeds with trade support |
| `normalizer.py` | (functions) | Exchange → canonical format (Binance, Bybit, Gate.io, Delta) |
| `monitor.py` | `LatencyMonitor`, `FeedStats` | Latency tracking |
| `strategy.py` | `TradeCollector`, `GlobalFairValue`, `DivergenceTracker`, `ZScoreSignal`, `Signal` | Strategy components |
| `orchestrator.py` | `DivergenceOrchestrator` | Main strategy wiring (supports 4 exchanges) |

---

## Install

```bash
git clone https://github.com/0zero4u/crypto_h.git
cd crypto_h
pip install -e ".[dev]" sortedcontainers aiohttp
pytest tests/ -v
```

---

## License

MIT © Abdelmalek Rhayoute
