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
    Parse Binance trade/aggTrade stream message.
    Handles combined-stream wrapper: {"stream": "...", "data": {...}}
    """
    data = msg.get("data", msg)
    event_type = data.get("e")
    if event_type not in ("trade", "aggTrade"):
        return None

    return {
        "symbol": data["s"],
        "exchange": "binance",
        "price": float(data["p"]),
        "qty": float(data["q"]),
        "ts_ms": data["T"],
        "side": "sell" if data.get("m") else "buy",
    }


def normalize_gateio_depth(msg: dict) -> Optional[dict]:
    """
    Parse Gate.io futures order_book_update delta message.

    Gate.io format:
    {
      "channel": "futures.order_book_update",
      "event": "update",
      "result": {
        "t": 1615366381417,
        "s": "BTC_USDT",
        "U": 2517661101,
        "u": 2517661113,
        "b": [{"p": "54672.1", "s": "0"}, ...],
        "a": [{"p": "54743.6", "s": "95"}, ...]
      }
    }
    """
    channel = msg.get("channel", "")
    if channel != "futures.order_book_update":
        return None

    event = msg.get("event")
    if event != "update":
        return None

    result = msg.get("result", {})

    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(item["p"]), float(item["s"])) for item in raw]

    symbol = result.get("s", "UNKNOWN")
    normalized_symbol = symbol.replace("_", "")

    return {
        "type":      "diff",
        "symbol":    normalized_symbol,
        "exchange":  "gateio",
        "ts_ms":     result.get("t", 0),
        "update_id": result.get("u", 0),
        "first_id":  result.get("U", 0),
        "bids":      parse_levels(result.get("b", [])),
        "asks":      parse_levels(result.get("a", [])),
    }


def normalize_gateio_snapshot(data: dict, symbol: str) -> Optional[dict]:
    """
    Parse Gate.io REST order book snapshot.

    GET /api/v4/futures/usdt/order_book?contract=BTC_USDT&limit=20&with_id=true

    Response:
    {
      "id": 81045888518,
      "asks": [{"p": "94364.1", "s": "41549"}, ...],
      "bids": [{"p": "94364", "s": "10000"}, ...]
    }
    """
    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(item["p"]), float(item["s"])) for item in raw]

    normalized_symbol = symbol.replace("_", "")

    return {
        "type":      "snapshot",
        "symbol":    normalized_symbol,
        "exchange":  "gateio",
        "ts_ms":     data.get("current", 0),
        "update_id": data.get("id", 0),
        "bids":      parse_levels(data.get("bids", [])),
        "asks":      parse_levels(data.get("asks", [])),
    }


def normalize_gateio_trade(msg: dict) -> Optional[dict]:
    """
    Parse Gate.io futures.trades message.

    {
      "channel": "futures.trades",
      "event": "update",
      "result": [{
        "size": "-108",
        "create_time_ms": 1545136464123,
        "price": "96.4",
        "contract": "BTC_USDT"
      }]
    }
    """
    channel = msg.get("channel", "")
    if channel != "futures.trades":
        return None

    event = msg.get("event")
    if event != "update":
        return None

    results = msg.get("result", [])
    if not results:
        return None

    trade = results[0] if isinstance(results, list) else results

    size = float(trade.get("size", 0))
    if size == 0:
        return None

    symbol = trade.get("contract", "UNKNOWN")

    return {
        "symbol":   symbol.replace("_", ""),
        "exchange": "gateio",
        "price":    float(trade["price"]),
        "qty":      abs(size),
        "ts_ms":    trade.get("create_time_ms", 0),
        "side":     "buy" if size > 0 else "sell",
        "volume":   abs(size),
    }


def normalize_delta_ob(msg: dict) -> Optional[dict]:
    """
    Parse Delta Exchange ob_l2 WebSocket message.

    ob_l2 provides top 15 levels of orderbook data at ~500ms intervals.
    Always a full snapshot (no incremental updates).

    Delta format:
    {
      "a": [["68525.0", "3313"], ["68525.5", "3009"], ...],  // asks (top 15)
      "b": [["68524.0", "2452"], ["68523.5", "3000"], ...],  // bids (top 15)
      "lts": 1775038313132415,  // last orderbook updated timestamp (microseconds)
      "sy": "BTCUSD",           // symbol
      "ts": 1775038313632092,   // publish timestamp (microseconds)
      "type": "ob_l2"
    }
    """
    msg_type = msg.get("type")
    if msg_type != "ob_l2":
        return None

    def parse_levels(raw: list) -> List[Tuple[float, float]]:
        return [(float(p), float(s)) for p, s in raw]

    lts_us = msg.get("lts", 0)
    ts_us = msg.get("ts", 0)
    ts_ms = (lts_us or ts_us) // 1000 if (lts_us or ts_us) > 0 else 0

    raw_bids = msg.get("b", [])
    raw_asks = msg.get("a", [])

    update_id = lts_us or ts_us

    return {
        "type":      "snapshot",
        "symbol":    msg.get("sy", "UNKNOWN"),
        "exchange":  "delta",
        "ts_ms":     ts_ms,
        "update_id": update_id,
        "bids":      parse_levels(raw_bids),
        "asks":      parse_levels(raw_asks),
        "bids_raw":  raw_bids,
        "asks_raw":  raw_asks,
    }


def normalize_delta_trade(msg: dict) -> Optional[dict]:
    """
    Parse Delta Exchange trades WebSocket message.

    Delta format:
    {
      "type": "trades",
      "sy": "BTCUSD",
      "p": "63678.0",
      "s": 40.0,
      "r": "m",                    // "m"=maker, "t"=taker
      "t": 1780557431637499,       // trade timestamp (microseconds)
      "ts": 1780557432004295       // server timestamp (microseconds)
    }
    """
    msg_type = msg.get("type")
    if msg_type != "trades":
        return None

    price = msg.get("p")
    size = msg.get("s")
    if price is None or size is None:
        return None

    size = abs(float(size))
    if size == 0:
        return None

    # "r" field: "t" = taker was buyer, "m" = taker was seller
    role = msg.get("r", "")
    side = "buy" if role == "t" else "sell"

    # Convert microseconds to milliseconds (use trade timestamp "t")
    ts_us = msg.get("t", 0)
    ts_ms = ts_us // 1000 if ts_us > 0 else 0

    # Delta BTCUSD: 1 contract = 0.001 BTC
    # Convert contracts to base asset quantity
    CONTRACT_SIZE = 0.001
    base_qty = size * CONTRACT_SIZE

    return {
        "symbol":   msg.get("sy", "UNKNOWN"),
        "exchange": "delta",
        "price":    float(price),
        "qty":      base_qty,
        "ts_ms":    ts_ms,
        "side":     side,
    }
