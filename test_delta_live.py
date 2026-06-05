#!/usr/bin/env python3
"""Quick test: verify Delta Exchange WebSocket feed works."""
import asyncio
import json
import sys
import time

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.feed import DeltaFeed
from cryptofeed.orderbook import L2OrderBook

msg_count = {"ob": 0, "trade": 0}
last_ob = None
last_trade = None


def on_book(book: L2OrderBook):
    global last_ob
    msg_count["ob"] += 1
    last_ob = {
        "symbol": book.symbol,
        "best_bid": book.best_bid.price if book.best_bid else None,
        "best_ask": book.best_ask.price if book.best_ask else None,
        "mid": book.mid_price,
        "spread_bps": book.spread_bps,
        "bids": len(book._bids),
        "asks": len(book._asks),
    }
    if msg_count["ob"] <= 3 or msg_count["ob"] % 100 == 0:
        print(f"[OB #{msg_count['ob']}] {last_ob['symbol']} "
              f"bid={last_ob['best_bid']} ask={last_ob['best_ask']} "
              f"mid={last_ob['mid']:.2f} spread={last_ob['spread_bps']:.2f}bps "
              f"depth={last_ob['bids']}x{last_ob['asks']}")


def on_trade(trade_data: dict):
    global last_trade
    msg_count["trade"] += 1
    last_trade = trade_data
    if msg_count["trade"] <= 3 or msg_count["trade"] % 50 == 0:
        print(f"[TRADE #{msg_count['trade']}] {trade_data['symbol']} "
              f"{trade_data['side'].upper()} {trade_data['qty']} @ {trade_data['price']}")


async def main():
    print("=" * 60)
    print("DELTA EXCHANGE FEED TEST")
    print("Endpoint: wss://public-socket.india.delta.exchange")
    print("Symbol: BTCUSD")
    print("=" * 60)

    feed = DeltaFeed(
        symbols=["BTCUSD"],
        depth=20,
        on_book_update=on_book,
        on_trade=on_trade,
    )

    await feed.start()

    # Run for 30 seconds
    duration = 30
    print(f"\nRunning for {duration}s...\n")

    for i in range(duration):
        await asyncio.sleep(1)
        if i == 5:
            print(f"\n--- 5s summary: OB={msg_count['ob']} Trades={msg_count['trade']} ---\n")

    await feed.stop()

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Orderbook updates: {msg_count['ob']}")
    print(f"Trade updates: {msg_count['trade']}")

    if last_ob:
        print(f"\nLast orderbook:")
        print(f"  Symbol: {last_ob['symbol']}")
        print(f"  Best bid: {last_ob['best_bid']}")
        print(f"  Best ask: {last_ob['best_ask']}")
        print(f"  Mid price: {last_ob['mid']:.2f}")
        print(f"  Spread: {last_ob['spread_bps']:.2f} bps")
        print(f"  Depth: {last_ob['bids']} bids x {last_ob['asks']} asks")

    if last_trade:
        print(f"\nLast trade:")
        print(f"  {last_trade['side'].upper()} {last_trade['qty']} @ {last_trade['price']}")

    if msg_count["ob"] > 0 and msg_count["trade"] > 0:
        print("\n✅ Delta Exchange feed is WORKING")
    elif msg_count["ob"] > 0:
        print("\n⚠️  Orderbook working, no trades received")
    elif msg_count["trade"] > 0:
        print("\n⚠️  Trades working, no orderbook received")
    else:
        print("\n❌ No data received - check connection")


if __name__ == "__main__":
    asyncio.run(main())
