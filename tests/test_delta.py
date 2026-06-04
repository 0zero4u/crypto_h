"""Tests for Delta Exchange normalizers."""
import pytest
from cryptofeed.normalizer import normalize_delta_ob, normalize_delta_trade


class TestDeltaOB:
    def test_snapshot(self):
        msg = {
            "action": "snapshot",
            "a": [["16919.0", "1087"], ["16919.5", "1193"], ["16920.0", "510"]],
            "b": [["16918.0", "602"], ["16917.5", "1792"], ["16917.0", "2039"]],
            "ts": 1671140718980723,
            "seq": 6199,
            "sy": "BTCUSD",
            "type": "ob_updates",
            "cs": 2178756498,
        }
        norm = normalize_delta_ob(msg)
        assert norm is not None
        assert norm["type"] == "snapshot"
        assert norm["symbol"] == "BTCUSD"
        assert norm["exchange"] == "delta"
        assert norm["update_id"] == 6199
        assert norm["ts_ms"] == 1671140718980
        assert norm["cs"] == 2178756498
        assert len(norm["bids"]) == 3
        assert len(norm["asks"]) == 3
        assert norm["bids"][0] == (16918.0, 602.0)
        assert norm["asks"][0] == (16919.0, 1087.0)

    def test_update(self):
        msg = {
            "action": "update",
            "a": [["16919.0", "0"], ["16919.5", "710"]],
            "b": [["16918.5", "304"]],
            "seq": 6200,
            "sy": "BTCUSD",
            "type": "ob_updates",
            "ts": 1671140769059031,
            "cs": 3409694612,
        }
        norm = normalize_delta_ob(msg)
        assert norm is not None
        assert norm["type"] == "diff"
        assert norm["symbol"] == "BTCUSD"
        assert norm["exchange"] == "delta"
        assert norm["update_id"] == 6200
        assert norm["cs"] == 3409694612
        assert len(norm["bids"]) == 1
        assert len(norm["asks"]) == 2
        assert norm["asks"][0] == (16919.0, 0.0)
        assert norm["asks"][1] == (16919.5, 710.0)

    def test_wrong_type(self):
        msg = {"type": "trades", "sy": "BTCUSD"}
        assert normalize_delta_ob(msg) is None

    def test_error_action(self):
        msg = {
            "action": "error",
            "symbol": "BTCUSD",
            "type": "ob_updates",
            "msg": "Snapshot load failed",
        }
        assert normalize_delta_ob(msg) is None


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
        msg = {"type": "ob_updates", "sy": "BTCUSD"}
        assert normalize_delta_trade(msg) is None

    def test_trade_missing_fields(self):
        msg = {"type": "trades", "sy": "BTCUSD"}
        assert normalize_delta_trade(msg) is None
