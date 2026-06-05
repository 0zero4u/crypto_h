"""Tests for Delta Exchange normalizers."""
import pytest
from cryptofeed.normalizer import normalize_delta_ob, normalize_delta_trade, normalize_delta_l1


class TestDeltaOB:
    def test_snapshot(self):
        msg = {
            "a": [["16919.0", "1087"], ["16919.5", "1193"], ["16920.0", "510"]],
            "b": [["16918.0", "602"], ["16917.5", "1792"], ["16917.0", "2039"]],
            "lts": 1671140718980723,
            "sy": "BTCUSD",
            "ts": 1671140718980723,
            "type": "ob_l2",
        }
        norm = normalize_delta_ob(msg)
        assert norm is not None
        assert norm["type"] == "snapshot"
        assert norm["symbol"] == "BTCUSD"
        assert norm["exchange"] == "delta"
        assert norm["update_id"] == 1671140718980723
        assert norm["ts_ms"] == 1671140718980
        assert len(norm["bids"]) == 3
        assert len(norm["asks"]) == 3
        assert norm["bids"][0] == (16918.0, 602.0)
        assert norm["asks"][0] == (16919.0, 1087.0)

    def test_wrong_type(self):
        msg = {"type": "trades", "sy": "BTCUSD"}
        assert normalize_delta_ob(msg) is None

    def test_ob_updates_ignored(self):
        msg = {
            "action": "snapshot",
            "a": [["16919.0", "1087"]],
            "b": [["16918.0", "602"]],
            "sy": "BTCUSD",
            "type": "ob_updates",
        }
        assert normalize_delta_ob(msg) is None


class TestDeltaL1:
    def test_l1_basic(self):
        msg = {
            "ap": "68519.0",
            "as": "285",
            "bp": "68518.0",
            "bs": "2452",
            "lts": 1775037882675402,
            "sy": "BTCUSD",
            "ts": 1775037882748105,
            "type": "ob_l1",
        }
        norm = normalize_delta_l1(msg)
        assert norm is not None
        assert norm["symbol"] == "BTCUSD"
        assert norm["exchange"] == "delta"
        assert norm["bid"] == 68518.0
        assert norm["ask"] == 68519.0
        assert norm["bid_size"] == 2452.0
        assert norm["ask_size"] == 285.0
        assert norm["mid_price"] == 68518.5
        assert norm["ts_ms"] == 1775037882675

    def test_l1_wrong_type(self):
        msg = {"type": "trades", "sy": "BTCUSD"}
        assert normalize_delta_l1(msg) is None

    def test_l1_missing_prices(self):
        msg = {"type": "ob_l1", "sy": "BTCUSD", "bp": "100.0"}
        assert normalize_delta_l1(msg) is None

    def test_l1_zero_prices(self):
        msg = {
            "ap": "0.0",
            "as": "285",
            "bp": "0.0",
            "bs": "2452",
            "sy": "BTCUSD",
            "type": "ob_l1",
        }
        assert normalize_delta_l1(msg) is None


class TestDeltaTrade:
    def test_trade_taker_buy(self):
        msg = {
            "type": "trades",
            "sy": "BTCUSD",
            "p": "63678.0",
            "s": 40.0,
            "r": "t",
            "t": 1780557431637499,
            "ts": 1780557432004295,
        }
        norm = normalize_delta_trade(msg)
        assert norm is not None
        assert norm["symbol"] == "BTCUSD"
        assert norm["exchange"] == "delta"
        assert norm["price"] == 63678.0
        assert norm["qty"] == 0.04
        assert norm["side"] == "buy"
        assert norm["ts_ms"] == 1780557431637

    def test_trade_maker_sell(self):
        msg = {
            "type": "trades",
            "sy": "ETHUSD",
            "p": "1780.4",
            "s": 20.0,
            "r": "m",
            "t": 1780557431823774,
            "ts": 1780557432009542,
        }
        norm = normalize_delta_trade(msg)
        assert norm is not None
        assert norm["side"] == "sell"
        assert norm["qty"] == 0.02

    def test_trade_wrong_type(self):
        msg = {"type": "ob_l2", "sy": "BTCUSD"}
        assert normalize_delta_trade(msg) is None

    def test_trade_missing_fields(self):
        msg = {"type": "trades", "sy": "BTCUSD"}
        assert normalize_delta_trade(msg) is None
