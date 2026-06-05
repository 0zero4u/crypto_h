#!/usr/bin/env python3
"""Quick check: Delta vs other exchange prices."""
import asyncio
import sys
import time

sys.path.insert(0, "/home/arshhtripathi/crypto_h/src")

from cryptofeed.feed import BinanceFeed, DeltaFeed
from cryptofeed.orderbook import L2OrderBook


prices = {}


def on_book(book: L2OrderBook, exchange: str):
    dwmp = book.dwmp(n_levels=20)
    if dwmp:
        prices[exchange] = dwmp


async def main():
    print("Checking Delta vs Binance prices for 30s...\n")
    
    binance = BinanceFeed(
        symbols=["BTCUSDT"],
        depth=20,
        on_book_update=lambda b: on_book(b, "binance"),
    )
    
    delta = DeltaFeed(
        symbols=["BTCUSD"],
        depth=20,
        on_book_update=lambda b: on_book(b, "delta"),
    )
    
    await binance.start()
    await delta.start()
    
    for i in range(30):
        await asyncio.sleep(1)
        
        if prices.get("binance") and prices.get("delta"):
            diff = prices["delta"] - prices["binance"]
            diff_pct = diff / prices["binance"] * 100
            
            if i % 5 == 0:
                print(f"[{i}s] Binance={prices['binance']:.2f} Delta={prices['delta']:.2f} "
                      f"Diff={diff:.2f} ({diff_pct:.4f}%)")
    
    await binance.stop()
    await delta.stop()
    
    print(f"\nFinal:")
    print(f"  Binance: {prices.get('binance', 'N/A')}")
    print(f"  Delta: {prices.get('delta', 'N/A')}")
    if prices.get("binance") and prices.get("delta"):
        diff = prices["delta"] - prices["binance"]
        diff_pct = diff / prices["binance"] * 100
        print(f"  Difference: {diff:.2f} ({diff_pct:.4f}%)")


if __name__ == "__main__":
    asyncio.run(main())
