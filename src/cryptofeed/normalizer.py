"""
Exchange-specific message normalizers → canonical format.
"""
from __future__ import annotations

from typing import List, Tuple, Optional
import orjson


def normalize_binance_depth(msg: dict) -> Optional[dict]:
    """
    Parse Binance diff depth WebSocket message.
    
    Binance format:
    {
      "e": "depthUpdate",
      "E": 1672531200000,  // Event time ms
      "s": "BTCUSDT",
      "U": 123456789,      // First update ID
      "u": 123456790,      // Final update ID
      "b": [["43200.00", "0.100"]],   // Bids to update
      "a": [["43201.00", "0.050"]]    // Asks to update
    }
    """
    if msg.get("e") != "depthUpdate":
        return None

    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(p), float(q)) for p, q in raw]

    return {
        "type":       "diff",
        "symbol":     msg["s"],
        "exchange":   "binance",
        "ts_ms":      msg["E"],
        "update_id":  msg["u"],
        "bids":       parse_levels(msg.get("b", [])),
        "asks":       parse_levels(msg.get("a", [])),
    }


def normalize_binance_snapshot(msg: dict) -> Optional[dict]:
    """
    Parse Binance REST snapshot response.
    
    GET /api/v3/depth?symbol=BTCUSDT&limit=100
    """
    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(p), float(q)) for p, q in raw]

    return {
        "type":      "snapshot",
        "symbol":    msg.get("symbol", "UNKNOWN"),
        "exchange":  "binance",
        "ts_ms":     0,
        "update_id": msg.get("lastUpdateId", 0),
        "bids":      parse_levels(msg.get("bids", [])),
        "asks":      parse_levels(msg.get("asks", [])),
    }


def normalize_bybit_trade(msg: dict) -> Optional[dict]:
    """
    Parse Bybit V5 public trade message.

    Bybit format:
    {
      "topic": "publicTrade.BTCUSDT",
      "type": "snapshot",
      "ts": 1672531200000,
      "data": [{
        "T": 1672531200000,
        "s": "BTCUSDT",
        "S": "Buy",
        "v": "0.100",
        "p": "43200.00",
        ...
      }]
    }
    """
    topic = msg.get("topic", "")
    if not topic.startswith("publicTrade"):
        return None

    data = msg.get("data", [])
    if not data:
        return None

    trade = data[0] if isinstance(data, list) else data

    return {
        "symbol": trade["s"],
        "exchange": "bybit",
        "price": float(trade["p"]),
        "qty": float(trade["v"]),
        "ts_ms": trade["T"],
        "side": trade["S"].lower(),
    }


def normalize_bybit_depth(msg: dict) -> Optional[dict]:
    """
    Parse Bybit V5 orderbook.1 / orderbook.50 message.

    Bybit format:
    {
      "topic": "orderbook.50.BTCUSDT",
      "type": "snapshot" | "delta",
      "ts": 1672531200000,
      "data": {
        "s": "BTCUSDT",
        "u": 1234567,
        "b": [["43200.00", "0.100"]],
        "a": [["43201.00", "0.050"]]
      }
    }
    """
    topic = msg.get("topic", "")
    if not topic.startswith("orderbook"):
        return None

    data = msg.get("data", {})
    msg_type = msg.get("type", "delta")

    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(p), float(q)) for p, q in raw]

    return {
        "type":      "snapshot" if msg_type == "snapshot" else "diff",
        "symbol":    data.get("s", "UNKNOWN"),
        "exchange":  "bybit",
        "ts_ms":     msg.get("ts", 0),
        "update_id": data.get("u", 0),
        "bids":      parse_levels(data.get("b", [])),
        "asks":      parse_levels(data.get("a", [])),
    }


def normalize_binance_trade(msg: dict) -> Optional[dict]:
    """
    Parse Binance trade stream message.

    Binance format:
    {
      "e": "trade",
      "E": 1672531200000,
      "s": "BTCUSDT",
      "p": "43200.00",
      "q": "0.100",
      "T": 1672531200000,
      "m": true
    }
    """
    if msg.get("e") != "trade":
        return None

    return {
        "symbol": msg["s"],
        "exchange": "binance",
        "price": float(msg["p"]),
        "qty": float(msg["q"]),
        "ts_ms": msg["T"],
        "side": "sell" if msg.get("m") else "buy",
    }
