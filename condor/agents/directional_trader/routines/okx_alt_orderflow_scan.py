"""Rank non-major OKX perps and confirm scalps with order-flow structure.

The routine is analysis-only.  It never creates an executor or submits an order.
It combines OKX's public 24h ticker/trade data with the configured Hummingbot
server's candles and order book, then returns percentage-based sizing guidance.
"""

from __future__ import annotations

import asyncio
import math
from statistics import median
from typing import Any

import aiohttp
from pydantic import BaseModel, Field, field_validator
from telegram.ext import ContextTypes

from condor.reports import ReportBuilder
from condor.reports.footprint import estimate_volume_profile
from config_manager import get_client
from routines.base import RoutineResult

CATEGORY = "Trading Research"

OKX_BASE_URL = "https://www.okx.com"
DEFAULT_EXCLUDES = (
    "BTC,ETH,SOL,BNB,XRP,DOGE,ADA,TRX,TON,LINK,AVAX,BCH,LTC,DOT,SUI,"
    "USDT,USDC,DAI,FDUSD,TUSD,USDE,PYUSD"
)


def public_candle_connector(connector_name: str) -> str:
    """Map the OKX demo trading domain to its identical public candle feed.

    Hummingbot has no separate CandlesFactory entry for OKX simulated trading;
    OKX demo and production nevertheless share the same public market candles.
    Account, order-book and execution calls keep using the demo connector.
    """
    if connector_name == "okx_perpetual_demo":
        return "okx_perpetual"
    return connector_name


class Config(BaseModel):
    """OKX alt-perp scalp scan using ranking, book VWAP, OBI and volume profile."""

    connector_name: str = Field(
        default="okx_perpetual", description="Hummingbot perpetual connector"
    )
    top_per_side: int = Field(
        default=3, ge=1, le=8, description="Gainers and losers to inspect"
    )
    excluded_bases: str = Field(
        default=DEFAULT_EXCLUDES, description="Comma-separated bases never traded"
    )
    min_turnover_usdt: float = Field(
        default=10_000_000, ge=0, description="Minimum estimated 24h USDT turnover"
    )
    equity_preview_usdt: float = Field(
        default=100,
        gt=0,
        description="Preview equity used only when live portfolio value is unavailable",
    )
    equity_cap_usdt: float = Field(
        default=100,
        gt=0,
        description="Hard cap on sizing equity, even when the connector holds more",
    )
    total_margin_pct: float = Field(
        default=30.0, gt=0, le=100, description="Total margin cap as percent of equity"
    )
    per_position_margin_pct: float = Field(
        default=15.0,
        gt=0,
        le=100,
        description="Per-position margin cap as percent of equity",
    )
    risk_per_trade_pct: float = Field(
        default=0.6, gt=0, le=5, description="Target account risk per trade, percent"
    )
    estimated_cost_pct: float = Field(
        default=0.12,
        ge=0,
        le=2,
        description="Round-trip fee plus slippage estimate, percent of notional",
    )
    depth_band_pct: float = Field(
        default=0.25,
        gt=0,
        le=2,
        description="Order-book depth band around mid, percent",
    )
    min_obi: float = Field(
        default=0.20,
        ge=0,
        lt=1,
        description="Minimum absolute normalized order-book imbalance",
    )
    min_taker_ratio: float = Field(
        default=0.58, ge=0.5, le=1, description="Minimum aggressive-side trade ratio"
    )
    max_spread_pct: float = Field(
        default=0.12, gt=0, le=2, description="Maximum bid/ask spread, percent"
    )
    max_vwap_slippage_pct: float = Field(
        default=0.12, gt=0, le=2, description="Maximum book VWAP slippage, percent"
    )
    poc_no_trade_band_pct: float = Field(
        default=0.10,
        ge=0,
        le=2,
        description="No-trade band around profile POC, percent",
    )
    profile_minutes: int = Field(
        default=120,
        ge=30,
        le=600,
        description="One-minute candles in the volume profile",
    )
    profile_buckets: int = Field(
        default=36, ge=12, le=100, description="Volume-profile price buckets"
    )
    book_samples: int = Field(
        default=3, ge=2, le=8, description="Snapshots required for persistent imbalance"
    )
    sample_interval_ms: int = Field(
        default=350, ge=100, le=2000, description="Delay between order-book snapshots"
    )

    @field_validator("excluded_bases")
    @classmethod
    def normalize_excludes(cls, value: str) -> str:
        return ",".join(
            sorted({part.strip().upper() for part in value.split(",") if part.strip()})
        )


def _number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _extract_rows(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("data") or payload.get("candles") or []
    return []


def _normalize_candles(payload: Any) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for raw in _extract_rows(payload):
        if isinstance(raw, dict):
            row = {
                key: _number(raw.get(key))
                for key in ("timestamp", "open", "high", "low", "close", "volume")
            }
        elif isinstance(raw, (list, tuple)) and len(raw) >= 6:
            row = {
                "timestamp": _number(raw[0]),
                "open": _number(raw[1]),
                "high": _number(raw[2]),
                "low": _number(raw[3]),
                "close": _number(raw[4]),
                "volume": _number(raw[5]),
            }
        else:
            continue
        if row["close"] > 0 and row["high"] >= row["low"] > 0 and row["volume"] >= 0:
            rows.append(row)
    rows.sort(key=lambda item: item["timestamp"])
    return rows


def rank_tickers(
    rows: list[dict[str, Any]], excluded: set[str], min_turnover: float, top_n: int
) -> list[dict[str, Any]]:
    """Return liquid USDT swaps from both tails of the 24h return ranking."""
    eligible: list[dict[str, Any]] = []
    for row in rows:
        inst_id = str(row.get("instId") or "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        base = inst_id[: -len("-USDT-SWAP")].upper()
        if not base or base in excluded:
            continue
        last = _number(row.get("last"))
        open_24h = _number(row.get("open24h"))
        base_volume = _number(row.get("volCcy24h"))
        if last <= 0 or open_24h <= 0:
            continue
        turnover = last * base_volume
        if turnover < min_turnover:
            continue
        eligible.append(
            {
                "inst_id": inst_id,
                "pair": f"{base}-USDT",
                "base": base,
                "last": last,
                "change_pct": (last / open_24h - 1) * 100,
                "turnover_usdt": turnover,
            }
        )
    ordered = sorted(eligible, key=lambda item: item["change_pct"])
    losers = ordered[:top_n]
    gainers = list(reversed(ordered[-top_n:]))
    for item in losers:
        item["ranking_side"] = "LOSER"
    for item in gainers:
        item["ranking_side"] = "GAINER"
    return gainers + losers


def _levels(raw: Any) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    for item in raw or []:
        if isinstance(item, dict):
            price = _number(item.get("price"))
            amount = _number(item.get("amount", item.get("quantity")))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, amount = _number(item[0]), _number(item[1])
        else:
            continue
        if price > 0 and amount > 0:
            parsed.append((price, amount))
    return parsed


def _vwap_for_quote(
    levels: list[tuple[float, float]], quote_target: float
) -> float | None:
    remaining = quote_target
    base_total = 0.0
    quote_total = 0.0
    for price, amount in levels:
        available_quote = price * amount
        take_quote = min(remaining, available_quote)
        if take_quote <= 0:
            continue
        base_total += take_quote / price
        quote_total += take_quote
        remaining -= take_quote
        if remaining <= 1e-9:
            break
    if remaining > max(quote_target * 0.01, 0.01) or base_total <= 0:
        return None
    return quote_total / base_total


def book_metrics(
    book: dict[str, Any], depth_band_pct: float, quote_target: float
) -> dict[str, float] | None:
    bids = _levels(book.get("bids") if isinstance(book, dict) else None)
    asks = _levels(book.get("asks") if isinstance(book, dict) else None)
    if not bids or not asks:
        return None
    bids.sort(key=lambda level: level[0], reverse=True)
    asks.sort(key=lambda level: level[0])
    best_bid, best_ask = bids[0][0], asks[0][0]
    if best_ask <= best_bid or best_bid <= 0:
        return None
    mid = (best_bid + best_ask) / 2
    band = depth_band_pct / 100
    bid_depth = sum(
        price * amount for price, amount in bids if price >= mid * (1 - band)
    )
    ask_depth = sum(
        price * amount for price, amount in asks if price <= mid * (1 + band)
    )
    depth_total = bid_depth + ask_depth
    buy_vwap = _vwap_for_quote(asks, quote_target)
    sell_vwap = _vwap_for_quote(bids, quote_target)
    return {
        "mid": mid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_pct": (best_ask - best_bid) / mid * 100,
        "obi": (bid_depth - ask_depth) / depth_total if depth_total > 0 else 0.0,
        "bid_depth_quote": bid_depth,
        "ask_depth_quote": ask_depth,
        "buy_vwap_slippage_pct": ((buy_vwap / mid - 1) * 100) if buy_vwap else math.inf,
        "sell_vwap_slippage_pct": (
            ((1 - sell_vwap / mid) * 100) if sell_vwap else math.inf
        ),
    }


def candle_vwap(
    candles: list[dict[str, float]], length: int = 60
) -> tuple[float, float]:
    window = candles[-length:]
    if not window:
        return 0.0, 0.0

    def value(rows: list[dict[str, float]]) -> float:
        denominator = sum(row["volume"] for row in rows)
        if denominator <= 0:
            return 0.0
        return (
            sum(
                ((row["high"] + row["low"] + row["close"]) / 3) * row["volume"]
                for row in rows
            )
            / denominator
        )

    current = value(window)
    prior = (
        value(candles[-(length * 2) : -length])
        if len(candles) >= length * 2
        else value(window[: max(1, len(window) // 2)])
    )
    slope_pct = ((current / prior - 1) * 100) if prior > 0 else 0.0
    return current, slope_pct


def volume_profile_levels(
    candles: list[dict[str, float]], buckets: int, value_area_fraction: float = 0.70
) -> tuple[float, float, float]:
    prices, buys, sells = estimate_volume_profile(candles, buckets=buckets)
    totals = [buy + sell for buy, sell in zip(buys, sells)]
    if not prices or not totals or sum(totals) <= 0:
        return 0.0, 0.0, 0.0
    poc_index = max(range(len(totals)), key=totals.__getitem__)
    selected = {poc_index}
    accumulated = totals[poc_index]
    target = sum(totals) * value_area_fraction
    lower = upper = poc_index
    while accumulated < target and (lower > 0 or upper < len(totals) - 1):
        down_volume = totals[lower - 1] if lower > 0 else -1.0
        up_volume = totals[upper + 1] if upper < len(totals) - 1 else -1.0
        if up_volume >= down_volume and upper < len(totals) - 1:
            upper += 1
            selected.add(upper)
            accumulated += totals[upper]
        elif lower > 0:
            lower -= 1
            selected.add(lower)
            accumulated += totals[lower]
        else:
            break
    return prices[poc_index], prices[min(selected)], prices[max(selected)]


def trade_volume_profile_levels(
    trades: list[dict[str, Any]], buckets: int, value_area_fraction: float = 0.70
) -> tuple[float, float, float]:
    """Build POC/VAL/VAH from actual OKX prints rather than candle estimates."""
    parsed = [
        (_number(trade.get("px")), _number(trade.get("sz")))
        for trade in trades
        if _number(trade.get("px")) > 0 and _number(trade.get("sz")) > 0
    ]
    if len(parsed) < 30 or buckets < 2:
        return 0.0, 0.0, 0.0
    low = min(price for price, _ in parsed)
    high = max(price for price, _ in parsed)
    if high <= low:
        return 0.0, 0.0, 0.0
    width = (high - low) / buckets
    totals = [0.0] * buckets
    for price, amount in parsed:
        index = min(buckets - 1, max(0, int((price - low) / width)))
        totals[index] += amount
    prices = [low + (index + 0.5) * width for index in range(buckets)]
    poc_index = max(range(len(totals)), key=totals.__getitem__)
    selected = {poc_index}
    accumulated = totals[poc_index]
    target = sum(totals) * value_area_fraction
    lower = upper = poc_index
    while accumulated < target and (lower > 0 or upper < len(totals) - 1):
        down_volume = totals[lower - 1] if lower > 0 else -1.0
        up_volume = totals[upper + 1] if upper < len(totals) - 1 else -1.0
        if up_volume >= down_volume and upper < len(totals) - 1:
            upper += 1
            selected.add(upper)
            accumulated += totals[upper]
        elif lower > 0:
            lower -= 1
            selected.add(lower)
            accumulated += totals[lower]
        else:
            break
    return prices[poc_index], prices[min(selected)], prices[max(selected)]


def taker_ratios(trades: list[dict[str, Any]]) -> tuple[float, float]:
    buy_quote = sell_quote = 0.0
    for trade in trades:
        value = _number(trade.get("px")) * _number(trade.get("sz"))
        if str(trade.get("side") or "").lower() == "buy":
            buy_quote += value
        elif str(trade.get("side") or "").lower() == "sell":
            sell_quote += value
    total = buy_quote + sell_quote
    if total <= 0:
        return 0.5, 0.5
    return buy_quote / total, sell_quote / total


def stop_pct_for_leverage(leverage: int) -> float:
    return 0.50 if leverage >= 10 else 0.65 if leverage >= 7 else 0.80


def percentage_size(
    equity: float,
    leverage: int,
    stop_pct: float,
    cost_pct: float,
    risk_pct: float,
    margin_cap_pct: float,
) -> dict[str, float]:
    denominator = leverage * ((stop_pct + cost_pct) / 100)
    raw_margin_pct = risk_pct / denominator if denominator > 0 else 0.0
    margin_pct = min(margin_cap_pct, raw_margin_pct)
    margin = equity * margin_pct / 100
    return {
        "margin_pct": margin_pct,
        "margin_usdt": margin,
        "notional_usdt": margin * leverage,
        "estimated_risk_usdt": margin * leverage * (stop_pct + cost_pct) / 100,
    }


async def _okx_json(
    session: aiohttp.ClientSession, path: str, params: dict[str, str]
) -> list[dict[str, Any]]:
    async with session.get(f"{OKX_BASE_URL}{path}", params=params) as response:
        response.raise_for_status()
        payload = await response.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(
            f"OKX public endpoint rejected request: {payload.get('msg')}"
        )
    return [row for row in payload.get("data", []) if isinstance(row, dict)]


def _parse_equity(value: Any, fallback: float) -> float:
    if isinstance(value, (int, float)) and _number(value) > 0:
        return _number(value)
    if isinstance(value, dict):
        for key in ("total_value", "total_usd", "value", "balance"):
            parsed = _number(value.get(key))
            if parsed > 0:
                return parsed
    return fallback


def bounded_equity(value: Any, fallback: float, cap: float) -> tuple[float, str]:
    """Return connector equity without ever sizing above the configured test cap."""
    live = _parse_equity(value, 0)
    if live > 0:
        bounded = min(live, cap)
        source = "live connector"
        if live > cap:
            source += f", capped from {live:.2f}"
        return bounded, source
    return min(fallback, cap), "preview"


async def run(
    config: Config, context: ContextTypes.DEFAULT_TYPE
) -> RoutineResult | str:
    client = await get_client(context._chat_id, context=context)
    if not client:
        return "No Hummingbot server available"

    excluded = {part for part in config.excluded_bases.split(",") if part}
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        ticker_rows = await _okx_json(
            session, "/api/v5/market/tickers", {"instType": "SWAP"}
        )
        candidates = rank_tickers(
            ticker_rows, excluded, config.min_turnover_usdt, config.top_per_side
        )
        if not candidates:
            return "No liquid non-major OKX USDT swaps passed the ranking filters"

        try:
            live_equity = await client.portfolio.get_total_value(
                connector_name=config.connector_name
            )
        except Exception:
            live_equity = None
        equity, equity_source = bounded_equity(
            live_equity,
            config.equity_preview_usdt,
            config.equity_cap_usdt,
        )
        probe_notional = equity * config.per_position_margin_pct / 100 * 5
        semaphore = asyncio.Semaphore(8)

        async def guarded(call):
            async with semaphore:
                return await call

        async def inspect(candidate: dict[str, Any]) -> dict[str, Any]:
            pair = candidate["pair"]
            inst_id = candidate["inst_id"]
            candle_call = guarded(
                client.market_data.get_candles(
                    public_candle_connector(config.connector_name),
                    pair,
                    interval="1m",
                    max_records=max(config.profile_minutes, 120),
                )
            )
            trades_call = _okx_json(
                session, "/api/v5/market/trades", {"instId": inst_id, "limit": "500"}
            )
            try:
                candle_payload, trades = await asyncio.gather(candle_call, trades_call)
            except Exception as exc:
                return {**candidate, "decision": "ERROR", "reason": str(exc)[:120]}

            candles = _normalize_candles(candle_payload)[-config.profile_minutes :]
            if len(candles) < 30:
                return {
                    **candidate,
                    "decision": "WATCH",
                    "reason": "insufficient 1m candles",
                }

            samples: list[dict[str, float]] = []
            for index in range(config.book_samples):
                try:
                    raw_book = await guarded(
                        client.market_data.get_order_book(
                            config.connector_name, pair, depth=50
                        )
                    )
                    metrics = book_metrics(
                        raw_book or {}, config.depth_band_pct, probe_notional
                    )
                    if metrics:
                        samples.append(metrics)
                except Exception:
                    pass
                if index + 1 < config.book_samples:
                    await asyncio.sleep(config.sample_interval_ms / 1000)
            if len(samples) < config.book_samples:
                return {
                    **candidate,
                    "decision": "WATCH",
                    "reason": "order book not persistent",
                }

            obis = [sample["obi"] for sample in samples]
            last_book = samples[-1]
            current = last_book["mid"]
            vwap, vwap_slope = candle_vwap(candles)
            poc, val, vah = trade_volume_profile_levels(trades, config.profile_buckets)
            vp_source = "trades"
            if poc <= 0:
                poc, val, vah = volume_profile_levels(candles, config.profile_buckets)
                vp_source = "1m estimate"
            buy_ratio, sell_ratio = taker_ratios(trades)
            poc_distance_pct = abs(current / poc - 1) * 100 if poc > 0 else 0.0
            spread_ok = last_book["spread_pct"] <= config.max_spread_pct
            ranking_side = candidate["ranking_side"]

            if ranking_side == "GAINER":
                direction = "LONG"
                flow_ok = (
                    min(obis) >= config.min_obi and buy_ratio >= config.min_taker_ratio
                )
                structure_ok = (
                    current > vwap > 0 and vwap_slope > 0 and current > vah > 0
                )
                slip = last_book["buy_vwap_slippage_pct"]
            else:
                direction = "SHORT"
                flow_ok = (
                    max(obis) <= -config.min_obi
                    and sell_ratio >= config.min_taker_ratio
                )
                structure_ok = current < vwap and vwap_slope < 0 and 0 < current < val
                slip = last_book["sell_vwap_slippage_pct"]

            profile_ok = poc > 0 and poc_distance_pct > config.poc_no_trade_band_pct
            slip_ok = math.isfinite(slip) and slip <= config.max_vwap_slippage_pct
            decision = (
                direction
                if all((flow_ok, structure_ok, profile_ok, spread_ok, slip_ok))
                else "WATCH"
            )
            failed = []
            for ok, label in (
                (flow_ok, "flow"),
                (structure_ok, "VWAP/VP"),
                (profile_ok, "POC band"),
                (spread_ok, "spread"),
                (slip_ok, "book VWAP"),
            ):
                if not ok:
                    failed.append(label)

            depth_multiple = min(
                last_book["bid_depth_quote"], last_book["ask_depth_quote"]
            ) / max(probe_notional, 1)
            if last_book["spread_pct"] <= 0.05 and slip <= 0.05 and depth_multiple >= 5:
                leverage = 10
            elif (
                last_book["spread_pct"] <= 0.08 and slip <= 0.08 and depth_multiple >= 3
            ):
                leverage = 7
            else:
                leverage = 5
            stop_pct = stop_pct_for_leverage(leverage)
            sizing = percentage_size(
                equity,
                leverage,
                stop_pct,
                config.estimated_cost_pct,
                config.risk_per_trade_pct,
                config.per_position_margin_pct,
            )
            return {
                **candidate,
                "decision": decision,
                "reason": (
                    "all gates passed" if decision != "WATCH" else ", ".join(failed)
                ),
                "price": current,
                "spread_pct": last_book["spread_pct"],
                "obi": median(obis),
                "buy_ratio": buy_ratio,
                "sell_ratio": sell_ratio,
                "vwap": vwap,
                "vwap_slope_pct": vwap_slope,
                "poc": poc,
                "val": val,
                "vah": vah,
                "vp_source": vp_source,
                "book_vwap_slippage_pct": slip,
                "leverage": leverage,
                "stop_loss_pct": stop_pct,
                "take_profit_pct": stop_pct * 1.5,
                "time_limit_seconds": 300,
                **sizing,
            }

        results = await asyncio.gather(
            *(inspect(candidate) for candidate in candidates)
        )

    table = []
    for row in results:
        table.append(
            {
                "Pair": row["pair"],
                "Rank": row.get("ranking_side", ""),
                "24h": f"{row.get('change_pct', 0):+.2f}%",
                "Signal": row.get("decision", "WATCH"),
                "OBI": f"{row.get('obi', 0):+.2f}",
                "Taker": f"{max(row.get('buy_ratio', 0), row.get('sell_ratio', 0)) * 100:.1f}%",
                "Spread": f"{row.get('spread_pct', 0):.3f}%",
                "VWAP Slip": f"{row.get('book_vwap_slippage_pct', 0):.3f}%",
                "VP": row.get("vp_source", "—"),
                "Lev": f"{row.get('leverage', 0)}x" if row.get("leverage") else "—",
                "Margin": (
                    f"{row.get('margin_pct', 0):.1f}%" if row.get("margin_pct") else "—"
                ),
                "Why": row.get("reason", ""),
            }
        )

    actionable = [row for row in results if row.get("decision") in ("LONG", "SHORT")]
    builder = ReportBuilder("OKX Alt Order-Flow Scalp Scan")
    builder.source("routine", "okx_alt_orderflow_scan")
    builder.tags(["okx", "perpetual", "order-flow", "scalping", "analysis-only"])
    builder.section(
        "01 / SCAN", "Non-major OKX USDT swaps; analysis only, no orders submitted."
    )
    builder.kpi("Candidates", str(len(results)))
    builder.kpi("Actionable", str(len(actionable)))
    builder.kpi("Sizing equity", f"{equity:.2f} USDT ({equity_source})")
    builder.kpi(
        "Margin caps",
        f"{config.per_position_margin_pct:.0f}% / {config.total_margin_pct:.0f}%",
    )
    builder.table(
        table,
        [
            "Pair",
            "Rank",
            "24h",
            "Signal",
            "OBI",
            "Taker",
            "Spread",
            "VWAP Slip",
            "VP",
            "Lev",
            "Margin",
            "Why",
        ],
    )
    builder.markdown(
        "Signals require persistent book imbalance, aggressive-trade confirmation, "
        "VWAP direction, a VAH/VAL break, distance from POC, and acceptable spread/book VWAP. "
        "Volume Profile uses actual OKX trade prints when at least 30 are available, "
        "with a clearly labelled one-minute OHLCV fallback."
    )
    builder.manual_order()
    await builder.save()

    summary = (
        f"Scanned {len(results)} liquid non-major OKX swaps; "
        f"{len(actionable)} passed every order-flow gate. Analysis only — no order was sent."
    )
    return RoutineResult(text=summary, table_data=table, table_columns=list(table[0]))
