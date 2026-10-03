"""Read-only cross-sectional OKX perpetual market-data collector.

This routine deliberately has no account, credential, executor, or order API
dependency.  It selects liquid non-major USDT perpetuals from OKX's public
market data, keeps ETH as a benchmark, and records normalized order-book and
trade-flow features with delayed forward-return labels.

The output is newline-delimited JSON compressed with gzip.  That format is
dependency-free, appendable after a restart, and can be converted to Parquet
when the training environment is prepared.  One row is emitted only after its
1/5/15/30 second labels are known.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import math
import os
import statistics
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Deque

import aiohttp
from pydantic import BaseModel, Field, field_validator
from telegram.ext import ContextTypes

from condor.reports import LiveReport

CATEGORY = "Trading Research"
CONTINUOUS = True

OKX_REST = "https://www.okx.com"
OKX_PUBLIC_WS = "wss://ws.okx.com/ws/v5/public"
SCHEMA_VERSION = "okx-alt-cross-section-v1"
ETH_INST_ID = "ETH-USDT-SWAP"
DEFAULT_EXCLUDES = (
    "BTC,SOL,BNB,XRP,DOGE,ADA,TRX,TON,LINK,AVAX,BCH,LTC,DOT,SUI,"
    "USDT,USDC,DAI,FDUSD,TUSD,USDE,PYUSD"
)
LABEL_HORIZONS = (1, 5, 15, 30)


def _number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


class Config(BaseModel):
    """Public-data-only configuration; there are intentionally no order fields."""

    universe_size: int = Field(default=20, ge=5, le=50)
    excluded_bases: str = Field(default=DEFAULT_EXCLUDES)
    min_turnover_usdt: float = Field(default=5_000_000, ge=500_000)
    max_ticker_spread_bps: float = Field(default=12, ge=1, le=100)
    min_listing_days: int = Field(default=30, ge=7, le=3650)
    sample_interval_ms: int = Field(default=1000, ge=500, le=10_000)
    universe_refresh_minutes: int = Field(default=60, ge=15, le=1440)
    rolling_window_samples: int = Field(default=1800, ge=300, le=20_000)
    maker_fee_bps_per_side: float = Field(default=2.0, ge=-2, le=20)
    taker_fee_bps_per_side: float = Field(default=5.0, ge=0, le=30)
    data_dir: str = Field(default="data/research/alt_cross_section")

    @field_validator("excluded_bases")
    @classmethod
    def normalize_excludes(cls, value: str) -> str:
        return ",".join(
            sorted({part.strip().upper() for part in value.split(",") if part.strip()})
        )


class InstrumentMeta:
    def __init__(
        self,
        inst_id: str,
        base: str,
        ct_val: float,
        ct_mult: float,
        tick_size: float,
        lot_size: float,
        listing_time: float,
    ) -> None:
        self.inst_id = inst_id
        self.base = base
        self.ct_val = ct_val
        self.ct_mult = ct_mult
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.listing_time = listing_time

    @property
    def base_per_contract(self) -> float:
        value = self.ct_val * self.ct_mult
        return value if value > 0 else 1.0


class MarketState:
    def __init__(
        self,
        meta: InstrumentMeta,
        turnover_usdt_24h: float,
        change_pct_24h: float,
        ticker_spread_bps: float,
    ) -> None:
        self.meta = meta
        self.turnover_usdt_24h = turnover_usdt_24h
        self.change_pct_24h = change_pct_24h
        self.ticker_spread_bps = ticker_spread_bps
        self.bids: list[tuple[float, float]] = []
        self.asks: list[tuple[float, float]] = []
        self.trades: Deque[dict[str, float | str]] = deque(maxlen=20_000)
        self.frames: Deque[tuple[float, float]] = deque(maxlen=120)
        self.pending: Deque[dict[str, Any]] = deque()
        self.history: dict[str, Deque[float]] = {}
        self.last_book_at = 0.0
        self.last_trade_at = 0.0

    def update_book(self, data: dict[str, Any]) -> None:
        self.bids = _levels(data.get("bids"), reverse=True)
        self.asks = _levels(data.get("asks"), reverse=False)
        if self.bids and self.asks:
            self.last_book_at = _number(data.get("ts")) / 1000 or time.time()

    def add_trades(self, rows: list[dict[str, Any]]) -> None:
        for raw in rows:
            price = _number(raw.get("px"))
            contracts = _number(raw.get("sz"))
            side = str(raw.get("side") or "").lower()
            timestamp = _number(raw.get("ts")) / 1000 or time.time()
            if price <= 0 or contracts <= 0 or side not in {"buy", "sell"}:
                continue
            self.trades.append(
                {
                    "timestamp": timestamp,
                    "price": price,
                    "contracts": contracts,
                    "quote_notional": (
                        contracts * self.meta.base_per_contract * price
                    ),
                    "side": side,
                }
            )
            self.last_trade_at = timestamp


def _levels(raw: Any, *, reverse: bool) -> list[tuple[float, float]]:
    rows: list[tuple[float, float]] = []
    for item in raw or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        price, size = _number(item[0]), _number(item[1])
        if price > 0 and size > 0:
            rows.append((price, size))
    rows.sort(key=lambda item: item[0], reverse=reverse)
    return rows


def _base(inst_id: str) -> str:
    suffix = "-USDT-SWAP"
    return inst_id[: -len(suffix)] if inst_id.endswith(suffix) else ""


def instrument_map(rows: list[dict[str, Any]]) -> dict[str, InstrumentMeta]:
    result: dict[str, InstrumentMeta] = {}
    for row in rows:
        inst_id = str(row.get("instId") or "")
        base = _base(inst_id)
        if (
            not base
            or str(row.get("state") or "").lower() != "live"
            or str(row.get("ctType") or "").lower() != "linear"
        ):
            continue
        result[inst_id] = InstrumentMeta(
            inst_id=inst_id,
            base=base,
            ct_val=max(_number(row.get("ctVal")), 1e-12),
            ct_mult=max(_number(row.get("ctMult")), 1e-12),
            tick_size=_number(row.get("tickSz")),
            lot_size=_number(row.get("lotSz")),
            listing_time=_number(row.get("listTime")) / 1000,
        )
    return result


def select_universe(
    tickers: list[dict[str, Any]],
    instruments: dict[str, InstrumentMeta],
    *,
    now: float,
    excluded: set[str],
    size: int,
    min_turnover: float,
    max_spread_bps: float,
    min_listing_days: int,
) -> list[dict[str, Any]]:
    """Rank a stable, liquid alt universe and append ETH as the benchmark."""
    eligible: list[dict[str, Any]] = []
    benchmark: dict[str, Any] | None = None
    for row in tickers:
        inst_id = str(row.get("instId") or "")
        meta = instruments.get(inst_id)
        if meta is None:
            continue
        last = _number(row.get("last"))
        bid, ask = _number(row.get("bidPx")), _number(row.get("askPx"))
        # OKX reports ``volCcy24h`` in base currency for linear swaps; unlike
        # ``vol24h`` it is already contract-size adjusted.
        volume_base = _number(row.get("volCcy24h"))
        turnover = last * volume_base
        spread_bps = (ask / bid - 1) * 10_000 if ask > bid > 0 else math.inf
        listing_days = (now - meta.listing_time) / 86400 if meta.listing_time else 0
        open_24h = _number(row.get("open24h"))
        candidate = {
            "inst_id": inst_id,
            "base": meta.base,
            "turnover_usdt_24h": turnover,
            "ticker_spread_bps": spread_bps,
            "change_pct_24h": (last / open_24h - 1) * 100 if open_24h > 0 else 0,
            "listing_days": listing_days,
        }
        if inst_id == ETH_INST_ID:
            benchmark = candidate
            continue
        if meta.base in excluded:
            continue
        if (
            turnover < min_turnover
            or spread_bps > max_spread_bps
            or listing_days < min_listing_days
        ):
            continue
        eligible.append(candidate)
    eligible.sort(key=lambda item: item["turnover_usdt_24h"], reverse=True)
    selected = eligible[:size]
    if benchmark is not None:
        selected.append(benchmark)
    return selected


def percentile_rank(history: Deque[float], value: float) -> float:
    if not history:
        return 0.5
    below = sum(item < value for item in history)
    equal = sum(item == value for item in history)
    return (below + 0.5 * equal) / len(history)


def _trade_metrics(state: MarketState, now: float) -> dict[str, float]:
    rows = [row for row in state.trades if now - float(row["timestamp"]) <= 60]
    if not rows:
        return {
            "trade_notional_5s": 0.0,
            "trade_notional_60s": 0.0,
            "volume_multiple": 0.0,
            "buy_ratio": 0.5,
            "trade_vwap": 0.0,
            "trade_return_5s_bps": 0.0,
        }
    recent = [row for row in rows if now - float(row["timestamp"]) <= 5]
    prior = [row for row in rows if 5 < now - float(row["timestamp"]) <= 60]
    total = sum(float(row["quote_notional"]) for row in rows)
    recent_total = sum(float(row["quote_notional"]) for row in recent)
    prior_total = sum(float(row["quote_notional"]) for row in prior)
    buy = sum(
        float(row["quote_notional"]) for row in rows if row["side"] == "buy"
    )
    weighted = sum(
        float(row["price"]) * float(row["quote_notional"]) for row in rows
    )
    recent_rate = recent_total / 5
    prior_rate = prior_total / 55 if prior else 0.0
    first_price = float(recent[0]["price"]) if recent else float(rows[-1]["price"])
    last_price = float(recent[-1]["price"]) if recent else first_price
    return {
        "trade_notional_5s": recent_total,
        "trade_notional_60s": total,
        "volume_multiple": recent_rate / prior_rate if prior_rate > 0 else 0.0,
        "buy_ratio": buy / total if total > 0 else 0.5,
        "trade_vwap": weighted / total if total > 0 else 0.0,
        "trade_return_5s_bps": (
            (last_price / first_price - 1) * 10_000 if first_price > 0 else 0.0
        ),
    }


def compute_features(state: MarketState, *, now: float) -> dict[str, Any] | None:
    if not state.bids or not state.asks:
        return None
    best_bid, bid_top = state.bids[0]
    best_ask, ask_top = state.asks[0]
    if best_ask <= best_bid or best_bid <= 0:
        return None
    mid = (best_bid + best_ask) / 2
    base_per_contract = state.meta.base_per_contract
    bid_depth = sum(price * size * base_per_contract for price, size in state.bids)
    ask_depth = sum(price * size * base_per_contract for price, size in state.asks)
    total_depth = bid_depth + ask_depth
    obi = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0
    microprice = (
        (best_ask * bid_top + best_bid * ask_top) / (bid_top + ask_top)
        if bid_top + ask_top > 0
        else mid
    )
    ask_gaps = [
        (state.asks[i + 1][0] / state.asks[i][0] - 1) * 10_000
        for i in range(len(state.asks) - 1)
    ]
    bid_gaps = [
        (state.bids[i][0] / state.bids[i + 1][0] - 1) * 10_000
        for i in range(len(state.bids) - 1)
        if state.bids[i + 1][0] > 0
    ]
    trades = _trade_metrics(state, now)
    row: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "timestamp": now,
        "inst_id": state.meta.inst_id,
        "base": state.meta.base,
        "is_benchmark": state.meta.inst_id == ETH_INST_ID,
        "mid": mid,
        "spread_bps": (best_ask / best_bid - 1) * 10_000,
        "microprice_bps": (microprice / mid - 1) * 10_000,
        "obi_top5": obi,
        "bid_depth_top5_usdt": bid_depth,
        "ask_depth_top5_usdt": ask_depth,
        "depth_total_top5_usdt": total_depth,
        "bid_depth_share": bid_depth / total_depth if total_depth > 0 else 0.5,
        "up_liquidity_gap_bps": max(ask_gaps, default=0.0),
        "down_liquidity_gap_bps": max(bid_gaps, default=0.0),
        "turnover_usdt_24h": state.turnover_usdt_24h,
        "change_pct_24h": state.change_pct_24h,
        "ticker_spread_bps": state.ticker_spread_bps,
        "book_age_ms": max(0.0, (now - state.last_book_at) * 1000),
        "trade_age_ms": (
            max(0.0, (now - state.last_trade_at) * 1000)
            if state.last_trade_at > 0
            else -1.0
        ),
        "tick_size_bps": state.meta.tick_size / mid * 10_000,
        "contract_base_value": base_per_contract,
        **trades,
    }
    return row


ROLLING_KEYS = (
    "spread_bps",
    "depth_total_top5_usdt",
    "obi_top5",
    "volume_multiple",
    "trade_notional_5s",
    "microprice_bps",
    "up_liquidity_gap_bps",
    "down_liquidity_gap_bps",
)


def add_percentiles(
    rows: list[dict[str, Any]], states: dict[str, MarketState], window: int
) -> None:
    for row in rows:
        state = states[row["inst_id"]]
        for key in ROLLING_KEYS:
            value = float(row[key])
            history = state.history.setdefault(key, deque(maxlen=window))
            row[f"{key}_rolling_pct"] = percentile_rank(history, value)
            history.append(value)
    for key in ROLLING_KEYS:
        ordered = sorted(float(row[key]) for row in rows)
        for row in rows:
            value = float(row[key])
            below = sum(item < value for item in ordered)
            equal = sum(item == value for item in ordered)
            row[f"{key}_cross_section_pct"] = (
                (below + 0.5 * equal) / len(ordered) if ordered else 0.5
            )


def add_benchmark_context(rows: list[dict[str, Any]]) -> None:
    eth = next((row for row in rows if row["is_benchmark"]), None)
    for row in rows:
        row["eth_trade_return_5s_bps"] = (
            float(eth["trade_return_5s_bps"]) if eth else 0.0
        )
        row["eth_obi_top5"] = float(eth["obi_top5"]) if eth else 0.0
        row["eth_volume_multiple"] = float(eth["volume_multiple"]) if eth else 0.0


def labelled_row(
    pending: dict[str, Any], frames: Deque[tuple[float, float]], config: Config
) -> dict[str, Any] | None:
    base_time, base_mid = float(pending["timestamp"]), float(pending["mid"])
    if not frames or frames[-1][0] < base_time + max(LABEL_HORIZONS):
        return None
    output = dict(pending)
    for horizon in LABEL_HORIZONS:
        candidates = [item for item in frames if item[0] >= base_time + horizon]
        if not candidates:
            return None
        future_mid = min(candidates, key=lambda item: item[0])[1]
        raw_return = (future_mid / base_mid - 1) * 10_000
        output[f"future_return_{horizon}s_bps"] = raw_return
        output[f"future_long_net_maker_{horizon}s_bps"] = (
            raw_return - 2 * config.maker_fee_bps_per_side
        )
        output[f"future_short_net_maker_{horizon}s_bps"] = (
            -raw_return - 2 * config.maker_fee_bps_per_side
        )
    output["estimated_round_trip_maker_cost_bps"] = (
        2 * config.maker_fee_bps_per_side
    )
    output["estimated_round_trip_taker_cost_bps"] = (
        2 * config.taker_fee_bps_per_side
    )
    return output


class GzipJsonlWriter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.rows_written = 0

    def write(self, rows: list[dict[str, Any]]) -> None:
        by_day: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            day = datetime.fromtimestamp(float(row["timestamp"]), UTC).strftime(
                "%Y-%m-%d"
            )
            by_day.setdefault(day, []).append(row)
        for day, day_rows in by_day.items():
            path = self.root / f"features-{day}.jsonl.gz"
            with gzip.open(path, "at", encoding="utf-8") as handle:
                for row in day_rows:
                    handle.write(json.dumps(row, separators=(",", ":")) + "\n")
                    self.rows_written += 1

    def manifest(self, *, universe: list[dict[str, Any]], started_at: float) -> None:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "read_only": True,
            "source": "OKX public REST/WebSocket",
            "started_at": started_at,
            "updated_at": time.time(),
            "label_horizons_seconds": list(LABEL_HORIZONS),
            "universe": universe,
            "rows_written_this_process": self.rows_written,
        }
        path = self.root / "manifest.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temporary.replace(path)


async def _public_json(
    session: aiohttp.ClientSession, path: str, params: dict[str, str]
) -> list[dict[str, Any]]:
    async with session.get(f"{OKX_REST}{path}", params=params, timeout=20) as response:
        response.raise_for_status()
        payload = await response.json()
    if str(payload.get("code", "0")) != "0":
        raise RuntimeError(f"OKX public API error: {payload.get('msg')}")
    return list(payload.get("data") or [])


async def discover_universe(
    session: aiohttp.ClientSession, config: Config
) -> tuple[list[dict[str, Any]], dict[str, InstrumentMeta]]:
    tickers, instruments_raw = await asyncio.gather(
        _public_json(session, "/api/v5/market/tickers", {"instType": "SWAP"}),
        _public_json(session, "/api/v5/public/instruments", {"instType": "SWAP"}),
    )
    instruments = instrument_map(instruments_raw)
    selected = select_universe(
        tickers,
        instruments,
        now=time.time(),
        excluded=set(config.excluded_bases.split(",")),
        size=config.universe_size,
        min_turnover=config.min_turnover_usdt,
        max_spread_bps=config.max_ticker_spread_bps,
        min_listing_days=config.min_listing_days,
    )
    if len(selected) < config.universe_size + 1:
        raise RuntimeError(
            f"only {len(selected) - int(any(x['inst_id'] == ETH_INST_ID for x in selected))} "
            "alt swaps passed the liquidity gates"
        )
    return selected, instruments


async def _stream(states: dict[str, MarketState]) -> None:
    args = [
        {"channel": channel, "instId": inst_id}
        for inst_id in states
        for channel in ("books5", "trades")
    ]
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=25)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(OKX_PUBLIC_WS, heartbeat=15) as ws:
                    await ws.send_json({"op": "subscribe", "args": args})
                    while True:
                        try:
                            message = await ws.receive(timeout=20)
                        except TimeoutError:
                            await ws.send_str("ping")
                            continue
                        if message.type == aiohttp.WSMsgType.TEXT:
                            if message.data == "pong":
                                continue
                            payload = json.loads(message.data)
                            arg = payload.get("arg") or {}
                            state = states.get(str(arg.get("instId") or ""))
                            if state is None:
                                continue
                            channel = str(arg.get("channel") or "")
                            data = list(payload.get("data") or [])
                            if channel == "books5" and data:
                                state.update_book(data[-1])
                            elif channel == "trades" and data:
                                state.add_trades(data)
                        elif message.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            break
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)


async def _collect_generation(
    *,
    config: Config,
    selected: list[dict[str, Any]],
    instruments: dict[str, InstrumentMeta],
    writer: GzipJsonlWriter,
    report: LiveReport,
    started_at: float,
) -> None:
    states = {
        item["inst_id"]: MarketState(
            meta=instruments[item["inst_id"]],
            turnover_usdt_24h=float(item["turnover_usdt_24h"]),
            change_pct_24h=float(item["change_pct_24h"]),
            ticker_spread_bps=float(item["ticker_spread_bps"]),
        )
        for item in selected
    }
    stream = asyncio.create_task(_stream(states))
    deadline = time.monotonic() + config.universe_refresh_minutes * 60
    last_report_at = 0.0
    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(config.sample_interval_ms / 1000)
            now = time.time()
            rows = [
                row
                for state in states.values()
                if (row := compute_features(state, now=now)) is not None
                and row["book_age_ms"] <= 5000
            ]
            if not rows:
                continue
            add_percentiles(rows, states, config.rolling_window_samples)
            add_benchmark_context(rows)
            output: list[dict[str, Any]] = []
            for row in rows:
                state = states[row["inst_id"]]
                state.frames.append((now, float(row["mid"])))
                state.pending.append(row)
                while state.pending:
                    ready = labelled_row(state.pending[0], state.frames, config)
                    if ready is None:
                        break
                    output.append(ready)
                    state.pending.popleft()
            if output:
                writer.write(output)
            if now - last_report_at >= 10:
                stale = sum(now - state.last_book_at > 5 for state in states.values())
                report.clear()
                report.builder.h1("OKX Alt Cross-Section Training Collector")
                report.builder.kpi("Mode", "READ ONLY · NO ORDER API")
                report.builder.kpi("Schema", SCHEMA_VERSION)
                report.builder.kpi("Alt universe", str(len(states) - 1))
                report.builder.kpi("Benchmark", "ETH-USDT-SWAP")
                report.builder.kpi("Rows written", str(writer.rows_written))
                report.builder.kpi("Stale books", str(stale))
                report.builder.kpi(
                    "Sample interval", f"{config.sample_interval_ms} ms"
                )
                report.builder.kpi("Storage", str(writer.root.resolve()))
                report.builder.table(
                    [
                        {
                            "Pair": item["inst_id"],
                            "24h turnover": f"{item['turnover_usdt_24h'] / 1e6:.1f}M",
                            "Spread": f"{item['ticker_spread_bps']:.2f} bps",
                            "24h": f"{item['change_pct_24h']:+.2f}%",
                        }
                        for item in selected
                    ]
                )
                await report.update()
                writer.manifest(universe=selected, started_at=started_at)
                last_report_at = now
    finally:
        stream.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stream


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Continuously collect public features; this function cannot place orders."""
    del context
    started_at = time.time()
    root = Path(os.path.expanduser(config.data_dir))
    if not root.is_absolute():
        root = Path(__file__).resolve().parents[3] / root
    writer = GzipJsonlWriter(root)
    report = LiveReport(
        "OKX Alt Cross-Section Training Collector",
        source_name="okx_alt_training_collector",
        tags=["okx", "public-data", "altcoins", "training", "read-only"],
    )
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            selected, instruments = await discover_universe(session, config)
            writer.manifest(universe=selected, started_at=started_at)
            await _collect_generation(
                config=config,
                selected=selected,
                instruments=instruments,
                writer=writer,
                report=report,
                started_at=started_at,
            )
