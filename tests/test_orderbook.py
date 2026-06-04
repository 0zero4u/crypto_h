"""Tests for L2 order book."""
import pytest
from cryptofeed import L2OrderBook, BookLevel


def make_book():
    book = L2OrderBook("BTCUSDT", depth=50)
    bids = [(43200.0 - i, 1.0 + i*0.1) for i in range(20)]
    asks = [(43201.0 + i, 0.5 + i*0.05) for i in range(20)]
    book.apply_snapshot(bids, asks, update_id=1000, exchange_ts_ms=1700000000000)
    return book


class TestL2OrderBook:
    def test_snapshot(self):
        book = make_book()
        assert book.best_bid is not None
        assert book.best_ask is not None
        assert book.best_bid.price == 43200.0
        assert book.best_ask.price == 43201.0

    def test_spread_positive(self):
        book = make_book()
        assert book.spread > 0
        assert book.spread == 1.0

    def test_spread_bps(self):
        book = make_book()
        assert book.spread_bps is not None
        assert book.spread_bps > 0

    def test_mid_price(self):
        book = make_book()
        expected_mid = (43200.0 + 43201.0) / 2
        assert abs(book.mid_price - expected_mid) < 1e-9

    def test_diff_update_new_level(self):
        book = make_book()
        book.apply_diff([(43195.0, 2.0)], [], update_id=1001)
        # New bid level added
        assert book.best_bid.price == 43200.0  # Still old best

    def test_diff_update_best_bid(self):
        book = make_book()
        book.apply_diff([(43205.0, 5.0)], [], update_id=1001)
        assert book.best_bid.price == 43205.0  # New best bid

    def test_diff_remove_level(self):
        book = make_book()
        # Remove best bid (qty = 0)
        book.apply_diff([(43200.0, 0.0)], [], update_id=1001)
        assert book.best_bid.price != 43200.0

    def test_vwap_ask(self):
        book = make_book()
        vwap = book.vwap(side="ask", levels=5)
        assert vwap is not None
        assert vwap >= book.best_ask.price

    def test_depth_at_price(self):
        book = make_book()
        depth = book.depth_at_price(43200.0, side="bid")
        assert depth >= book.best_bid.qty

    def test_get_levels_depth(self):
        book = make_book()
        levels = book.get_levels(5)
        assert len(levels["bids"]) == 5
        assert len(levels["asks"]) == 5

    def test_update_count(self):
        book = make_book()
        assert book._update_count == 0
        book.apply_diff([(43199.0, 1.0)], [], update_id=1001)
        assert book._update_count == 1


class TestNormalizer:
    def test_binance_depth_parse(self):
        from cryptofeed import normalize_binance_depth
        msg = {
            "e": "depthUpdate",
            "E": 1700000000000,
            "s": "BTCUSDT",
            "U": 100,
            "u": 101,
            "b": [["43200.00", "0.100"]],
            "a": [["43201.00", "0.050"]],
        }
        norm = normalize_binance_depth(msg)
        assert norm is not None
        assert norm["symbol"] == "BTCUSDT"
        assert norm["type"] == "diff"
        assert len(norm["bids"]) == 1
        assert norm["bids"][0] == (43200.0, 0.1)

    def test_binance_depth_wrong_type(self):
        from cryptofeed import normalize_binance_depth
        msg = {"e": "trade", "s": "BTCUSDT"}
        assert normalize_binance_depth(msg) is None

    def test_bybit_depth_parse(self):
        from cryptofeed import normalize_bybit_depth
        msg = {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1700000000000,
            "data": {
                "s": "BTCUSDT",
                "u": 12345,
                "b": [["43200.00", "1.5"]],
                "a": [["43201.00", "0.8"]],
            },
        }
        norm = normalize_bybit_depth(msg)
        assert norm is not None
        assert norm["type"] == "snapshot"
        assert norm["symbol"] == "BTCUSDT"


class TestLatencyMonitor:
    def test_record_and_stats(self):
        import time
        from cryptofeed import LatencyMonitor
        monitor = LatencyMonitor()
        local_ts_ns = time.time_ns()
        exchange_ts_ms = (local_ts_ns // 1_000_000) - 10
        monitor.record("BTCUSDT", exchange_ts_ms, local_ts_ns)
        monitor.record("BTCUSDT", exchange_ts_ms - 5, local_ts_ns)
        stats = monitor.get_stats("BTCUSDT")
        assert stats is not None
        assert stats.msg_count == 2
        assert stats.mean_latency_us > 0
