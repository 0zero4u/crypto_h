# crypto-hft-infra

**Async WebSocket market data feed with L2 order book reconstruction**  
Binance · Bybit · Real-time latency monitoring · Sub-millisecond updates

[![CI](https://github.com/abRH/crypto-hft-infra/actions/workflows/ci.yml/badge.svg)](https://github.com/abRH/crypto-hft-infra/actions)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-grade async WebSocket feed handler for crypto market microstructure research. Supports real-time L2 order book reconstruction with incremental diff updates, latency monitoring, and normalized multi-exchange data.

---

## Architecture

```
Binance WS ──► BinanceFeed.process_message()
Bybit WS   ──► BybitFeed.process_message()
                        │
                ┌───────▼────────┐
                │   Normalizer   │  Exchange-specific → canonical format
                └───────┬────────┘
                        │
                ┌───────▼────────┐
                │  L2OrderBook   │  Incremental diff / snapshot
                │  (SortedDict)  │  O(log N) update, O(1) best bid/ask
                └───────┬────────┘
                        │
                ┌───────▼────────┐
                │LatencyMonitor  │  µs-precision timestamp recording
                └────────────────┘
```

---

## Features

- **Async WebSocket feeds**: Binance and Bybit with auto-reconnect
- **L2 book reconstruction**: Full snapshot + incremental diff updates
- **SortedDict book**: O(log N) updates, O(1) best bid/ask, depth trimming
- **Exchange normalizer**: Unified canonical format across venues
- **Latency monitoring**: P50/P99/P99.9 exchange-to-local latency
- **Market data**: mid-price, spread (BPS), VWAP, depth at price

---

## Quick Start

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
pip install -e ".[dev]" sortedcontainers
pytest tests/ -v
```

---

## License

MIT © Abdelmalek Rhayoute
