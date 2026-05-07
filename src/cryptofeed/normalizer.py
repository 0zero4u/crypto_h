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
