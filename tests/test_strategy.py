"""Tests for strategy modules."""
import pytest
import time
from cryptofeed.orderbook import L2OrderBook
from cryptofeed.strategy import (
    Trade, TradeCollector, GlobalFairValue,
    DivergenceTracker, ZScoreSignal, Signal,
)


def make_book(exchange: str = "binance", symbol: str = "BTCUSDT",
              bid_base: float = 43200.0, ask_base: float = 43201.0) -> L2OrderBook:
    """Create a test order book with realistic levels."""
    book = L2OrderBook(symbol, depth=50)
    bids = [(bid_base - i, 1.0 + i * 0.1) for i in range(20)]
    asks = [(ask_base + i, 0.5 + i * 0.05) for i in range(20)]
    book.apply_snapshot(bids, asks, update_id=1000, exchange_ts_ms=1700000000000)
    return book


def make_trade(exchange: str = "binance", price: float = 43200.5,
               qty: float = 0.1, ts_offset_ms: int = 0) -> Trade:
    """Create a test trade."""
    return Trade(
        symbol="BTCUSDT",
        exchange=exchange,
        price=price,
        qty=qty,
        ts_ms=int(time.time() * 1000) + ts_offset_ms,
        side="buy",
    )


class TestDWMP:
    """Tests for L2OrderBook.dwmp() method."""

    def test_dwmp_basic(self):
        book = make_book()
        dwmp = book.dwmp(n_levels=10)
        assert dwmp is not None
        assert dwmp > 0
        # DWMP should be close to mid price (within reasonable bounds)
        mid = book.mid_price
        assert mid is not None
        # DWMP should be within 3% of mid - cross-weighting prevents wild swings
        assert abs(dwmp - mid) / mid < 0.03

    def test_dwmp_near_mid_when_symmetric(self):
        book = L2OrderBook("BTCUSDT", depth=50)
        # Symmetric: same volume at each level
        bids = [(43200.0 - i, 1.0) for i in range(10)]
        asks = [(43201.0 + i, 1.0) for i in range(10)]
        book.apply_snapshot(bids, asks, update_id=1, exchange_ts_ms=1700000000000)

        dwmp = book.dwmp(n_levels=10)
        mid = book.mid_price
        # With symmetric volumes, DWMP should be very close to mid
        assert abs(dwmp - mid) < 0.5

    def test_dwmp_shifts_toward_deep_liquidity(self):
        """DWMP should shift toward the side with more depth."""
        book = L2OrderBook("BTCUSDT", depth=50)
        # Bids: normal volume
        bids = [(43200.0 - i, 1.0) for i in range(10)]
        # Asks: huge volume at level 3+
        asks = [(43201.0 + i, 0.1 if i < 3 else 10.0) for i in range(10)]
        book.apply_snapshot(bids, asks, update_id=1, exchange_ts_ms=1700000000000)

        dwmp = book.dwmp(n_levels=10)
        mid = book.mid_price
        # Huge sell walls should pull DWMP down (toward bids)
        assert dwmp < mid

    def test_dwmp_fallback_to_mid(self):
        """Empty book should return mid_price."""
        book = L2OrderBook("BTCUSDT", depth=50)
        assert book.dwmp() is None  # mid_price is None for empty book

    def test_dwmp_different_levels(self):
        book = make_book()
        dwmp_5 = book.dwmp(n_levels=5)
        dwmp_10 = book.dwmp(n_levels=10)
        dwmp_20 = book.dwmp(n_levels=20)
        # All should be valid
        assert all(x is not None for x in [dwmp_5, dwmp_10, dwmp_20])


class TestTradeCollector:
    """Tests for TradeCollector rolling volume."""

    def test_add_trade(self):
        tc = TradeCollector(window_seconds=60)
        trade = make_trade()
        tc.add_trade(trade)
        vol = tc.get_volume("binance")
        assert vol > 0

    def test_volume_calculation(self):
        tc = TradeCollector(window_seconds=60)
        tc.add_trade(make_trade(price=43200.0, qty=1.0))
        tc.add_trade(make_trade(price=43300.0, qty=2.0))
        vol = tc.get_volume("binance")
        # 43200 * 1 + 43300 * 2 = 129800
        assert abs(vol - 129800.0) < 0.01

    def test_volume_per_exchange(self):
        tc = TradeCollector(window_seconds=60)
        tc.add_trade(make_trade(exchange="binance", price=43200.0, qty=1.0))
        tc.add_trade(make_trade(exchange="bybit", price=43210.0, qty=0.5))

        assert tc.get_volume("binance") > 0
        assert tc.get_volume("bybit") > 0
        assert tc.get_volume("okx") == 0  # No OKX trades

    def test_volume_pruning(self):
        tc = TradeCollector(window_seconds=1)  # 1-second window
        tc.add_trade(make_trade(ts_offset_ms=-2000))  # 2 seconds ago
        vol = tc.get_volume("binance")
        assert vol == 0  # Should be pruned


class TestGlobalFairValue:
    """Tests for GlobalFairValue computation."""

    def test_gfv_single_exchange(self):
        tc = TradeCollector(window_seconds=60)
        tc.add_trade(make_trade(exchange="binance", price=43200.0, qty=1.0))

        gfv = GlobalFairValue(tc)
        gfv.update_dwmp("binance", 43200.5)

        result = gfv.compute()
        assert result is not None
        gfv_val, weights = result
        assert abs(gfv_val - 43200.5) < 0.01
        assert weights["binance"] == 1.0

    def test_gfv_volume_weighted(self):
        tc = TradeCollector(window_seconds=60)
        # Binance: 2x volume
        tc.add_trade(make_trade(exchange="binance", price=43200.0, qty=2.0))
        # Bybit: 1x volume
        tc.add_trade(make_trade(exchange="bybit", price=43200.0, qty=1.0))

        gfv = GlobalFairValue(tc)
        gfv.update_dwmp("binance", 43200.0)
        gfv.update_dwmp("bybit", 43300.0)  # Bybit higher

        result = gfv.compute()
        assert result is not None
        gfv_val, weights = result
        # GFV should be closer to Binance (more volume)
        # Expected: (43200*2 + 43300*1) / 3 = 43233.33
        assert abs(gfv_val - 43233.33) < 1.0
        assert weights["binance"] > weights["bybit"]

    def test_gfv_no_data(self):
        tc = TradeCollector(window_seconds=60)
        gfv = GlobalFairValue(tc)
        assert gfv.compute() is None


class TestDivergenceTracker:
    """Tests for DivergenceTracker rolling stats."""

    def test_record_and_stats(self):
        dt = DivergenceTracker(window_minutes=5)
        # Record enough samples
        for i in range(20):
            dt.record("binance", 0.01 * i, int(time.time() * 1000))

        stats = dt.get_stats("binance")
        assert stats is not None
        mean, std = stats
        assert mean > 0
        assert std > 0

    def test_insufficient_data(self):
        dt = DivergenceTracker(window_minutes=5)
        for i in range(5):  # Less than 10 required
            dt.record("binance", 0.01, int(time.time() * 1000))
        assert dt.get_stats("binance") is None

    def test_per_exchange_stats(self):
        dt = DivergenceTracker(window_minutes=5)
        ts = int(time.time() * 1000)
        for i in range(20):
            dt.record("binance", 0.05, ts)  # Binance always +0.05%
            dt.record("bybit", -0.02, ts)   # Bybit always -0.02%

        b_stats = dt.get_stats("binance")
        y_stats = dt.get_stats("bybit")
        assert b_stats is not None
        assert y_stats is not None
        # Different baselines
        assert abs(b_stats[0] - 0.05) < 0.01
        assert abs(y_stats[0] - (-0.02)) < 0.01


class TestZScoreSignal:
    """Tests for ZScoreSignal evaluation."""

    def test_no_signal_normal(self):
        zs = ZScoreSignal(threshold=3.0)
        # z = (0.05 - 0.05) / 0.01 = 0 → no signal
        signal = zs.evaluate("binance", 43200.0, 43200.0,
                             0.05, mean=0.05, std=0.01, ts_ms=0)
        assert signal is None

    def test_short_signal(self):
        zs = ZScoreSignal(threshold=3.0)
        # z = (0.30 - 0.05) / 0.05 = 5.0 → SHORT signal
        signal = zs.evaluate("binance", 43300.0, 43200.0,
                             0.30, mean=0.05, std=0.05, ts_ms=0)
        assert signal is not None
        assert signal.direction == "short"
        assert signal.z_score > 3.0

    def test_long_signal(self):
        zs = ZScoreSignal(threshold=3.0)
        # z = (-0.30 - (-0.05)) / 0.05 = -5.0 → LONG signal
        signal = zs.evaluate("bybit", 43100.0, 43200.0,
                             -0.30, mean=-0.05, std=0.05, ts_ms=0)
        assert signal is not None
        assert signal.direction == "long"
        assert signal.z_score < -3.0

    def test_no_signal_below_threshold(self):
        zs = ZScoreSignal(threshold=3.0)
        # z = (0.15 - 0.05) / 0.05 = 2.0 < 3.0 → no signal
        signal = zs.evaluate("binance", 43250.0, 43200.0,
                             0.15, mean=0.05, std=0.05, ts_ms=0)
        assert signal is None

    def test_zero_std(self):
        zs = ZScoreSignal(threshold=3.0)
        signal = zs.evaluate("binance", 43200.0, 43200.0,
                             0.05, mean=0.05, std=0.0, ts_ms=0)
        assert signal is None  # Avoid division by zero