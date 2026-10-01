"""Deterministic OKX demo forward test for short-duration alt-perp scalps.

The routine is intentionally demo-only.  It polls public market structure,
creates tightly bounded PositionExecutors on ``okx_perpetual_demo`` and reports
net, fee-inclusive executor performance.  It never falls back to a live
connector.
"""

from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from pydantic import BaseModel, Field, field_validator, model_validator
from telegram.ext import ContextTypes

from agents.directional_trader.routines.okx_alt_orderflow_scan import (
    DEFAULT_EXCLUDES,
    OKX_BASE_URL,
    _normalize_candles,
    _number,
    _okx_json,
    book_metrics,
    candle_vwap,
    rank_tickers,
    taker_ratios,
    trade_volume_profile_levels,
)
from condor.reports import LiveReport
from config_manager import get_client
from mcp_servers.hummingbot_api.tools import executor_create

CATEGORY = "Trading Research"
CONTINUOUS = True


class Config(BaseModel):
    """High-frequency forward test on the OKX simulated perpetual venue."""

    connector_name: str = Field(default="okx_perpetual_demo")
    account_name: str = Field(default="master_account")
    controller_id: str = Field(
        default="okx-alt-hft-demo-v11", min_length=8, max_length=64
    )
    run_until_depleted: bool = Field(default=True)
    minimum_equity_usdt: float = Field(default=1.0, ge=0.1, le=10)
    dynamic_pair_selection: bool = Field(default=True)
    interval_sec: int = Field(default=10, ge=5, le=60)
    max_runtime_minutes: int = Field(default=120, ge=10, le=1440)
    target_closed_trades: int = Field(default=30, ge=10, le=200)
    max_entries: int = Field(default=50, ge=10, le=500)
    top_per_side: int = Field(default=4, ge=1, le=8)
    min_turnover_usdt: float = Field(default=3_000_000, ge=0)
    excluded_bases: str = Field(default=DEFAULT_EXCLUDES)
    leverage: int = Field(default=10, ge=10, le=10)
    margin_per_trade_pct: float = Field(default=30, gt=0, le=30)
    min_margin_per_trade_pct: float = Field(default=1, ge=1, le=5)
    max_open_positions: int = Field(default=1, ge=1, le=1)
    stop_loss_pct: float = Field(default=0.30, gt=0, le=1)
    take_profit_pct: float = Field(default=0.45, gt=0, le=2)
    time_limit_seconds: int = Field(default=300, ge=60, le=600)
    entry_timeout_seconds: int = Field(default=30, ge=10, le=120)
    min_obi: float = Field(default=0.08, ge=0, lt=1)
    min_taker_ratio: float = Field(default=0.54, ge=0.5, le=1)
    min_confirmations: int = Field(default=3, ge=2, le=4)
    max_spread_pct: float = Field(default=0.12, gt=0, le=1)
    max_vwap_slippage_pct: float = Field(default=0.08, gt=0, le=1)
    cooldown_seconds: int = Field(default=60, ge=60, le=3600)
    default_fee_rate_pct: float = Field(default=0.05, gt=0, le=0.20)
    max_fee_rate_pct: float = Field(default=0.08, gt=0, le=0.20)
    min_edge_cost_multiple: float = Field(default=2.0, ge=1.25, le=5)
    max_loss_pct: float = Field(default=3, gt=0, le=10)
    max_consecutive_losses: int = Field(default=4, ge=2, le=10)
    profitable_profit_factor: float = Field(default=1.15, ge=1, le=3)

    @field_validator("connector_name")
    @classmethod
    def demo_only(cls, value: str) -> str:
        if value != "okx_perpetual_demo":
            raise ValueError("this routine may only use okx_perpetual_demo")
        return value

    @field_validator("controller_id")
    @classmethod
    def isolated_controller(cls, value: str) -> str:
        if not value.startswith("okx-alt-hft-demo-"):
            raise ValueError("controller_id must use the okx-alt-hft-demo- namespace")
        return value

    @model_validator(mode="after")
    def margin_range_is_ordered(self) -> "Config":
        if self.min_margin_per_trade_pct > self.margin_per_trade_pct:
            raise ValueError("minimum margin percentage exceeds the 30% cap")
        return self


def signal_decision(
    *,
    direction: str,
    obi: float,
    buy_ratio: float,
    sell_ratio: float,
    price: float,
    vwap: float,
    poc: float,
    val: float,
    vah: float,
    spread_pct: float,
    slippage_pct: float,
    min_obi: float,
    min_taker_ratio: float,
    min_confirmations: int,
    max_spread_pct: float,
    max_vwap_slippage_pct: float,
) -> dict[str, Any]:
    """Score an HFT entry while keeping liquidity gates mandatory."""
    is_long = direction == "LONG"
    checks = {
        "book": obi >= min_obi if is_long else obi <= -min_obi,
        "taker": (
            buy_ratio >= min_taker_ratio if is_long else sell_ratio >= min_taker_ratio
        ),
        "vwap": price > vwap > 0 if is_long else 0 < price < vwap,
        "profile": (
            price > vah > 0 or (poc > 0 and price > poc * 1.0005)
            if is_long
            else 0 < price < val or (poc > 0 and price < poc * 0.9995)
        ),
    }
    score = sum(checks.values())
    liquidity_ok = (
        math.isfinite(slippage_pct)
        and spread_pct <= max_spread_pct
        and slippage_pct <= max_vwap_slippage_pct
    )
    eligible = (
        liquidity_ok
        and score >= min_confirmations
        and (checks["book"] or checks["taker"])
    )
    failed = [name for name, passed in checks.items() if not passed]
    if not liquidity_ok:
        failed.append("liquidity")
    return {
        "eligible": eligible,
        "score": score,
        "checks": checks,
        "reason": "passed" if eligible else ", ".join(failed),
    }


def quantize_amount(notional: float, price: float, rules: dict[str, Any]) -> float:
    """Round base amount down to the venue step and enforce its minima."""
    step = _number(rules.get("min_base_amount_increment"))
    minimum = _number(rules.get("min_order_size"))
    min_notional = max(
        _number(rules.get("min_notional_size")),
        _number(rules.get("min_order_value")),
    )
    if notional <= 0 or price <= 0 or step <= 0:
        raise ValueError("invalid sizing inputs or trading rule")
    raw = Decimal(str(notional)) / Decimal(str(price))
    quantum = Decimal(str(step))
    amount = (raw / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum
    result = float(amount)
    if result < minimum or result * price < min_notional or result <= 0:
        raise ValueError("calculated amount is below exchange minimum")
    return result


def cap_amount_to_exchange_max(
    amount: float, max_amount: float, rules: dict[str, Any]
) -> float:
    """Apply a small buffer to the exchange contract cap and round down."""
    step = _number(rules.get("min_base_amount_increment"))
    minimum = _number(rules.get("min_order_size"))
    if amount <= 0 or max_amount <= 0 or step <= 0:
        raise ValueError("invalid exchange maximum or amount step")
    capped = min(amount, max_amount * 0.95)
    quantum = Decimal(str(step))
    rounded = (
        Decimal(str(capped)) / quantum
    ).to_integral_value(rounding=ROUND_DOWN) * quantum
    result = float(rounded)
    if result < minimum or result <= 0:
        raise ValueError("exchange maximum is below the minimum order size")
    return result


def position_size(
    test_equity: float, margin_pct: float, leverage: int
) -> tuple[float, float]:
    """Return margin and notional while enforcing the 30%/10x risk envelope."""
    if test_equity <= 0 or not 0 < margin_pct <= 30 or leverage != 10:
        raise ValueError(
            "position sizing requires positive equity, margin <= 30%, and 10x leverage"
        )
    margin = test_equity * margin_pct / 100
    return margin, margin * leverage


def liquidity_sized_position(
    *,
    book: dict[str, Any],
    direction: str,
    account_equity: float,
    leverage: int,
    min_margin_pct: float,
    max_margin_pct: float,
    max_slippage_pct: float,
) -> dict[str, Any] | None:
    """Choose the largest discrete margin percentage the visible book can absorb."""
    if (
        account_equity <= 0
        or leverage != 10
        or not 0 < min_margin_pct <= max_margin_pct <= 30
    ):
        raise ValueError("invalid liquidity sizing envelope")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")

    steps = sorted(
        {
            max_margin_pct,
            *(
                value
                for value in (30, 20, 15, 10, 5, 3, 2, 1)
                if min_margin_pct <= value <= max_margin_pct
            ),
            min_margin_pct,
        },
        reverse=True,
    )
    for margin_pct in steps:
        margin_usdt = account_equity * margin_pct / 100
        notional_usdt = margin_usdt * leverage
        metrics = book_metrics(book, 0.25, notional_usdt)
        if not metrics:
            continue
        slippage_pct = (
            metrics["buy_vwap_slippage_pct"]
            if direction == "LONG"
            else metrics["sell_vwap_slippage_pct"]
        )
        if math.isfinite(slippage_pct) and slippage_pct <= max_slippage_pct:
            return {
                **metrics,
                "margin_pct": float(margin_pct),
                "margin_usdt": margin_usdt,
                "notional_usdt": notional_usdt,
                "slippage_pct": slippage_pct,
            }
    return None


def conviction_margin_cap(
    *,
    score: int,
    obi: float,
    taker_ratio: float,
    min_margin_pct: float,
    max_margin_pct: float,
) -> float:
    """Scale risk with independent order-flow confirmation, never with losses.

    A barely passing 3/4 signal is deliberately kept small.  Full size needs
    all four confirmations plus strong book and taker flow.  This is
    anti-martingale sizing: losing history can never increase the next order.
    """
    if score >= 4 and abs(obi) >= 0.20 and taker_ratio >= 0.60:
        cap = 30.0
    elif score >= 4 or (score >= 3 and abs(obi) >= 0.12 and taker_ratio >= 0.58):
        cap = 15.0
    else:
        cap = 5.0
    return max(min_margin_pct, min(max_margin_pct, cap))


async def retry_network_call(operation: Any, *, max_delay: float = 60.0) -> Any:
    """Retry transient network failures forever while preserving cancellation."""
    delay = 2.0
    while True:
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError, OSError):
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 2)


def observed_fee_rates(rows: list[dict[str, Any]]) -> dict[str, float]:
    """Estimate each pair's actual one-way fee rate in percentage points.

    ``filled_amount_quote`` is total executed turnover (entry plus exit), so
    fees divided by turnover is the directly observed fee rate per execution.
    """
    totals: dict[str, list[float]] = {}
    seen: set[str] = set()
    for row in rows:
        row_id = str(row.get("executor_id") or row.get("id") or "")
        if row_id and row_id in seen:
            continue
        if row_id:
            seen.add(row_id)
        pair = str(
            row.get("trading_pair") or row.get("config", {}).get("trading_pair") or ""
        )
        volume = _number(row.get("filled_amount_quote"))
        fees = _number(row.get("cum_fees_quote"))
        if not pair or volume <= 0 or fees < 0:
            continue
        bucket = totals.setdefault(pair, [0.0, 0.0])
        bucket[0] += fees
        bucket[1] += volume
    return {
        pair: fees / volume * 100
        for pair, (fees, volume) in totals.items()
        if volume > 0
    }


def fee_edge_decision(
    *,
    take_profit_pct: float,
    fee_rate_pct: float,
    spread_pct: float,
    slippage_pct: float,
    max_fee_rate_pct: float,
    min_edge_cost_multiple: float,
) -> dict[str, Any]:
    """Require the gross target to cover estimated round-trip costs by a margin."""
    cost_pct = 2 * fee_rate_pct + spread_pct + slippage_pct
    eligible = (
        math.isfinite(cost_pct)
        and fee_rate_pct <= max_fee_rate_pct
        and take_profit_pct >= cost_pct * min_edge_cost_multiple
    )
    return {
        "eligible": eligible,
        "fee_rate_pct": fee_rate_pct,
        "estimated_cost_pct": cost_pct,
        "estimated_net_target_pct": take_profit_pct - cost_pct,
        "reason": "passed" if eligible else "fees/cost exceed allowed edge",
    }


def shanghai_trading_day(timestamp: float | None = None) -> str:
    moment = datetime.fromtimestamp(
        timestamp or time.time(), tz=ZoneInfo("Asia/Shanghai")
    )
    return moment.date().isoformat()


def daily_pair_from_rows(rows: list[dict[str, Any]], trading_day: str) -> str | None:
    """Recover today's one-coin lock after a routine restart."""
    ordered = sorted(rows, key=lambda row: _number(row.get("created_at")))
    for row in ordered:
        created_at = _number(row.get("created_at"))
        pair = str(
            row.get("trading_pair") or row.get("config", {}).get("trading_pair") or ""
        )
        if created_at > 0 and pair and shanghai_trading_day(created_at) == trading_day:
            return pair
    return None


def _executor_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "executors", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def performance_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate fee-inclusive forward-test statistics from terminal executors."""
    closed = [
        row
        for row in rows
        if str(row.get("status") or "").upper() in {"TERMINATED", "CLOSED"}
        and _number(row.get("filled_amount_quote")) > 0
    ]
    pnl = [_number(row.get("net_pnl_quote")) for row in closed]
    wins = [value for value in pnl if value > 0]
    losses = [value for value in pnl if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (
        gross_profit / gross_loss if gross_loss > 0 else math.inf if wins else 0.0
    )
    equity = peak = 0.0
    max_drawdown = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    consecutive_losses = 0
    for value in reversed(pnl):
        if value < 0:
            consecutive_losses += 1
        else:
            break
    return {
        "closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "net_pnl": sum(pnl),
        "win_rate": len(wins) / len(closed) if closed else 0.0,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "consecutive_losses": consecutive_losses,
        "fees": sum(_number(row.get("cum_fees_quote")) for row in closed),
        "volume": sum(_number(row.get("filled_amount_quote")) for row in closed),
        "failed": sum(
            1
            for row in rows
            if str(row.get("close_type") or "").upper() == "FAILED"
            and _number(row.get("filled_amount_quote")) <= 0
        ),
    }


def stale_unfilled_executors(
    rows: list[dict[str, Any]], *, now: float, timeout_seconds: int
) -> list[dict[str, Any]]:
    """Return active maker entries that never received a fill before timeout."""
    return [
        row
        for row in rows
        if str(row.get("status") or "").upper() in {"RUNNING", "CREATED"}
        and _number(row.get("filled_amount_quote")) <= 0
        and _number(row.get("created_at")) > 0
        and now - _number(row.get("created_at")) >= timeout_seconds
    ]


def profitability_verdict(
    summary: dict[str, Any],
    target_closed: int,
    min_profit_factor: float,
    max_loss: float,
) -> str:
    if summary["closed"] < target_closed:
        return "INCONCLUSIVE"
    if (
        summary["net_pnl"] > 0
        and summary["profit_factor"] >= min_profit_factor
        and summary["max_drawdown"] <= max_loss
    ):
        return "PROFITABLE"
    return "NOT_PROFITABLE"


async def _inspect_candidate(
    session: aiohttp.ClientSession,
    client: Any,
    candidate: dict[str, Any],
    config: Config,
    account_equity: float,
) -> dict[str, Any]:
    pair = candidate["pair"]
    inst_id = candidate["inst_id"]
    try:
        book, trades, candle_rows = await asyncio.gather(
            client.market_data.get_order_book(config.connector_name, pair, depth=50),
            _okx_json(
                session, "/api/v5/market/trades", {"instId": inst_id, "limit": "300"}
            ),
            _okx_candles(session, inst_id),
        )
        candles = _normalize_candles(candle_rows)
        direction = "LONG" if candidate["ranking_side"] == "GAINER" else "SHORT"
        sizing = liquidity_sized_position(
            book=book or {},
            direction=direction,
            account_equity=account_equity,
            leverage=config.leverage,
            min_margin_pct=config.min_margin_per_trade_pct,
            max_margin_pct=config.margin_per_trade_pct,
            max_slippage_pct=config.max_vwap_slippage_pct,
        )
        if not sizing:
            raise ValueError(
                f"insufficient depth even at {config.min_margin_per_trade_pct:g}% margin"
            )
        metrics = sizing
        if len(candles) < 30:
            raise ValueError("insufficient market data")
        price = metrics["mid"]
        vwap, _ = candle_vwap(candles, 30)
        poc, val, vah = trade_volume_profile_levels(trades, 30)
        buy_ratio, sell_ratio = taker_ratios(trades)
        slippage = metrics["slippage_pct"]
        decision = signal_decision(
            direction=direction,
            obi=metrics["obi"],
            buy_ratio=buy_ratio,
            sell_ratio=sell_ratio,
            price=price,
            vwap=vwap,
            poc=poc,
            val=val,
            vah=vah,
            spread_pct=metrics["spread_pct"],
            slippage_pct=slippage,
            min_obi=config.min_obi,
            min_taker_ratio=config.min_taker_ratio,
            min_confirmations=config.min_confirmations,
            max_spread_pct=config.max_spread_pct,
            max_vwap_slippage_pct=config.max_vwap_slippage_pct,
        )
        taker_ratio = buy_ratio if direction == "LONG" else sell_ratio
        margin_cap = conviction_margin_cap(
            score=decision["score"],
            obi=metrics["obi"],
            taker_ratio=taker_ratio,
            min_margin_pct=config.min_margin_per_trade_pct,
            max_margin_pct=config.margin_per_trade_pct,
        )
        if sizing["margin_pct"] > margin_cap:
            resized = liquidity_sized_position(
                book=book or {},
                direction=direction,
                account_equity=account_equity,
                leverage=config.leverage,
                min_margin_pct=config.min_margin_per_trade_pct,
                max_margin_pct=margin_cap,
                max_slippage_pct=config.max_vwap_slippage_pct,
            )
            if resized:
                metrics = resized
                slippage = metrics["slippage_pct"]
        return {
            **candidate,
            **decision,
            "direction": direction,
            "price": price,
            "maker_price": (
                metrics["best_bid"] if direction == "LONG" else metrics["best_ask"]
            ),
            "obi": metrics["obi"],
            "taker_ratio": taker_ratio,
            "spread_pct": metrics["spread_pct"],
            "slippage_pct": slippage,
            "margin_pct": metrics["margin_pct"],
            "margin_usdt": metrics["margin_usdt"],
            "notional_usdt": metrics["notional_usdt"],
        }
    except Exception as exc:
        return {**candidate, "eligible": False, "score": 0, "reason": str(exc)[:120]}


async def _okx_candles(session: aiohttp.ClientSession, inst_id: str) -> list[list[Any]]:
    """Fetch OKX candles without discarding their positional-array rows."""
    async with session.get(
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params={"instId": inst_id, "bar": "1m", "limit": "80"},
    ) as response:
        response.raise_for_status()
        payload = await response.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(f"OKX candles rejected request: {payload.get('msg')}")
    return [row for row in payload.get("data", []) if isinstance(row, list)]


async def _stop_active(client: Any, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if str(row.get("status") or "").upper() in {"RUNNING", "CREATED"}:
            executor_id = row.get("id") or row.get("executor_id")
            if executor_id:
                try:
                    await client.executors.stop_executor(
                        str(executor_id), keep_position=False
                    )
                except Exception:
                    pass


async def _demo_trading_pairs(client: Any, config: Config) -> set[str]:
    """Fail closed unless the authenticated demo universe can be loaded."""
    if config.connector_name != "okx_perpetual_demo" or client._session is None:
        raise RuntimeError("initialized OKX demo client is required")
    url = f"{client.base_url}/connectors/{config.connector_name}/account-instruments"
    async with client._session.get(
        url,
        params={"account_name": config.account_name, "inst_type": "SWAP"},
    ) as response:
        response.raise_for_status()
        payload = await response.json()
    pairs = {
        str(pair)
        for pair in payload.get("trading_pairs", [])
        if isinstance(pair, str) and pair.endswith("-USDT")
    }
    if not pairs:
        raise RuntimeError("OKX demo returned no account-tradable USDT swaps")
    return pairs


async def _demo_max_size(
    client: Any, config: Config, trading_pair: str
) -> dict[str, float]:
    """Fetch the authenticated OKX demo contract cap after leverage is set."""
    if config.connector_name != "okx_perpetual_demo" or client._session is None:
        raise RuntimeError("initialized OKX demo client is required")
    url = f"{client.base_url}/connectors/{config.connector_name}/max-size"
    async with client._session.get(
        url,
        params={
            "account_name": config.account_name,
            "trading_pair": trading_pair,
            "td_mode": "cross",
        },
    ) as response:
        response.raise_for_status()
        payload = await response.json()
    return {
        "max_buy": _number(payload.get("max_buy")),
        "max_sell": _number(payload.get("max_sell")),
    }


async def _ensure_initialized_client(client: Any) -> Any:
    """Reopen a cached API client whose aiohttp session was rotated or closed."""
    session = getattr(client, "_session", None)
    if session is None or bool(getattr(session, "closed", False)):
        await client.init()
    return client


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Run a demo-only forward test, optionally until its demo equity is depleted."""
    if config.connector_name != "okx_perpetual_demo":
        return "Refused: demo connector is mandatory"
    chat_id = getattr(context, "_chat_id", 0)
    controller_id = config.controller_id
    client = await get_client(chat_id, context=context)
    if not client:
        return "No Hummingbot server available"

    live_equity = await retry_network_call(
        lambda: client.portfolio.get_total_value(
            account_name=config.account_name,
            connector_name=config.connector_name,
        )
    )
    if _number(live_equity) <= 0:
        return "Refused: OKX demo equity is unavailable"
    try:
        supported_pairs = await retry_network_call(
            lambda: _demo_trading_pairs(client, config)
        )
    except Exception as exc:
        return f"Refused: could not verify OKX demo trading universe: {exc}"
    account_equity = _number(live_equity)
    initial_equity = account_equity
    max_margin_usdt, max_notional_usdt = position_size(
        account_equity, config.margin_per_trade_pct, config.leverage
    )
    loss_limit_usdt = account_equity * config.max_loss_pct / 100
    start = time.time()
    last_entry: dict[str, float] = {}
    entry_stop_requested_at: dict[str, float] = {}
    events: list[dict[str, Any]] = []
    trading_day = shanghai_trading_day()
    daily_pair: str | None = None
    report = LiveReport(
        "OKX Alt HFT Demo Profitability Test",
        source_name="okx_alt_hft_demo",
        tags=["okx", "demo", "hft", "forward-test"],
    )
    timeout = aiohttp.ClientTimeout(total=15)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            fee_payload = await retry_network_call(
                lambda: client.executors.search_executors(
                    account_names=[config.account_name],
                    connector_names=[config.connector_name],
                    limit=200,
                )
            )
            fee_history_rows = _executor_rows(fee_payload)
            while True:
                # ConfigManager rotates shared clients periodically. A long-lived
                # routine must reopen its retained client instead of dying on the
                # next request with "Client not initialized".
                await _ensure_initialized_client(client)
                refreshed_equity = await retry_network_call(
                    lambda: client.portfolio.get_total_value(
                        account_name=config.account_name,
                        connector_name=config.connector_name,
                    )
                )
                account_equity = _number(refreshed_equity)
                max_margin_usdt, max_notional_usdt = position_size(
                    max(account_equity, config.minimum_equity_usdt),
                    config.margin_per_trade_pct,
                    config.leverage,
                )
                payload = await retry_network_call(
                    lambda: client.executors.search_executors(
                        account_names=[config.account_name],
                        connector_names=[config.connector_name],
                        controller_ids=[controller_id],
                        limit=max(config.max_entries, 100),
                    )
                )
                rows = _executor_rows(payload)
                current_day = shanghai_trading_day()
                if current_day != trading_day:
                    trading_day = current_day
                    daily_pair = None
                if config.dynamic_pair_selection:
                    daily_pair = None
                else:
                    daily_pair = daily_pair or daily_pair_from_rows(rows, trading_day)
                fee_rates = observed_fee_rates([*fee_history_rows, *rows])
                active = [
                    row
                    for row in rows
                    if str(row.get("status") or "").upper() in {"RUNNING", "CREATED"}
                ]
                stale_entries = stale_unfilled_executors(
                    active,
                    now=time.time(),
                    timeout_seconds=config.entry_timeout_seconds,
                )
                for stale in stale_entries:
                    executor_id = str(
                        stale.get("id") or stale.get("executor_id") or ""
                    )
                    last_request = entry_stop_requested_at.get(executor_id, 0)
                    if executor_id and time.time() - last_request >= 30:
                        await retry_network_call(
                            lambda executor_id=executor_id: client.executors.stop_executor(
                                executor_id, keep_position=False
                            )
                        )
                        entry_stop_requested_at[executor_id] = time.time()
                        events.append(
                            {
                                "Time": time.strftime("%H:%M:%S"),
                                "Pair": str(
                                    stale.get("trading_pair")
                                    or stale.get("config", {}).get("trading_pair")
                                    or ""
                                ),
                                "Side": "—",
                                "Score": "—",
                                "OBI": "—",
                                "Taker": "—",
                                "Margin": "—",
                                "Cost": "—",
                                "Net target": "—",
                                "Event": "ENTRY TIMEOUT / CANCEL",
                            }
                        )
                stats = performance_summary(rows)
                verdict = profitability_verdict(
                    stats,
                    config.target_closed_trades,
                    config.profitable_profit_factor,
                    loss_limit_usdt,
                )
                elapsed = time.time() - start
                halt_reason = ""
                if account_equity <= config.minimum_equity_usdt:
                    halt_reason = "demo account depleted"
                elif not config.run_until_depleted:
                    if stats["net_pnl"] <= -loss_limit_usdt:
                        halt_reason = "loss limit"
                    elif stats["consecutive_losses"] >= config.max_consecutive_losses:
                        halt_reason = "consecutive loss limit"
                    elif stats["closed"] >= config.target_closed_trades:
                        halt_reason = "sample complete"
                    elif len(rows) >= config.max_entries:
                        halt_reason = "entry cap"
                    elif elapsed >= config.max_runtime_minutes * 60:
                        halt_reason = "time limit"

                if halt_reason:
                    await _stop_active(client, active)
                elif len(active) < config.max_open_positions:
                    ticker_rows = await retry_network_call(
                        lambda: _okx_json(
                            session,
                            "/api/v5/market/tickers",
                            {"instType": "SWAP"},
                        )
                    )
                    excluded = set(config.excluded_bases.split(","))
                    candidates = rank_tickers(
                        ticker_rows,
                        excluded,
                        config.min_turnover_usdt,
                        config.top_per_side,
                    )
                    candidates = [
                        candidate
                        for candidate in candidates
                        if candidate["pair"] in supported_pairs
                    ]
                    if daily_pair:
                        candidates = [
                            candidate
                            for candidate in candidates
                            if candidate["pair"] == daily_pair
                        ]
                    active_pairs = {
                        str(
                            row.get("trading_pair")
                            or row.get("config", {}).get("trading_pair")
                            or ""
                        )
                        for row in active
                    }
                    inspected = await asyncio.gather(
                        *(
                            _inspect_candidate(
                                session, client, candidate, config, account_equity
                            )
                            for candidate in candidates
                            if candidate["pair"] not in active_pairs
                            and time.time() - last_entry.get(candidate["pair"], 0)
                            >= config.cooldown_seconds
                        )
                    )
                    for row in inspected:
                        fee_decision = fee_edge_decision(
                            take_profit_pct=config.take_profit_pct,
                            fee_rate_pct=fee_rates.get(
                                row["pair"], config.default_fee_rate_pct
                            ),
                            spread_pct=_number(row.get("spread_pct")),
                            slippage_pct=_number(row.get("slippage_pct")),
                            max_fee_rate_pct=config.max_fee_rate_pct,
                            min_edge_cost_multiple=config.min_edge_cost_multiple,
                        )
                        row["signal_eligible"] = bool(row.get("eligible"))
                        row["eligible"] = (
                            bool(row.get("eligible")) and fee_decision["eligible"]
                        )
                        row.update(
                            {
                                f"cost_{key}": value
                                for key, value in fee_decision.items()
                            }
                        )
                    eligible = sorted(
                        (row for row in inspected if row.get("eligible")),
                        key=lambda row: (row.get("score", 0), abs(row.get("obi", 0))),
                        reverse=True,
                    )
                    if eligible:
                        pick = eligible[0]
                        if not config.dynamic_pair_selection:
                            daily_pair = daily_pair or pick["pair"]
                        rules_payload = await retry_network_call(
                            lambda: client.connectors.get_trading_rules(
                                config.connector_name, [pick["pair"]]
                            )
                        )
                        rules = rules_payload.get(pick["pair"], {})
                        amount = quantize_amount(
                            pick["notional_usdt"], pick["price"], rules
                        )
                        await retry_network_call(
                            lambda: client.trading.set_leverage(
                                account_name=config.account_name,
                                connector_name=config.connector_name,
                                trading_pair=pick["pair"],
                                leverage=config.leverage,
                            )
                        )
                        maximums = await retry_network_call(
                            lambda: _demo_max_size(client, config, pick["pair"])
                        )
                        exchange_max = (
                            maximums["max_buy"]
                            if pick["direction"] == "LONG"
                            else maximums["max_sell"]
                        )
                        amount = cap_amount_to_exchange_max(
                            amount, exchange_max, rules
                        )
                        result = await retry_network_call(
                            lambda: executor_create.create_position_executor(
                                client,
                                connector_name=config.connector_name,
                                trading_pair=pick["pair"],
                                side=1 if pick["direction"] == "LONG" else 2,
                                amount=amount,
                                entry_price=pick["maker_price"],
                                leverage=config.leverage,
                                stop_loss=config.stop_loss_pct / 100,
                                take_profit=config.take_profit_pct / 100,
                                time_limit=config.time_limit_seconds,
                                open_order_type=3,
                                take_profit_order_type=3,
                                stop_loss_order_type=1,
                                time_limit_order_type=1,
                                level_id="okx-alt-hft-demo",
                                account_name=config.account_name,
                                controller_id=controller_id,
                            )
                        )
                        last_entry[pick["pair"]] = time.time()
                        events.append(
                            {
                                "Time": time.strftime("%H:%M:%S"),
                                "Pair": pick["pair"],
                                "Side": pick["direction"],
                                "Score": f"{pick['score']}/4",
                                "OBI": f"{pick['obi']:+.2f}",
                                "Taker": f"{pick['taker_ratio'] * 100:.1f}%",
                                "Margin": (
                                    f"{amount * pick['price'] / config.leverage:.2f} "
                                    f"(signal cap {pick['margin_pct']:.0f}%)"
                                ),
                                "Cost": f"{pick['cost_estimated_cost_pct']:.3f}%",
                                "Net target": f"{pick['cost_estimated_net_target_pct']:.3f}%",
                                "Event": (
                                    "SUBMITTED"
                                    if result.get("executor_id")
                                    else "CREATE"
                                ),
                            }
                        )

                report.clear()
                report.builder.manual_order()
                report.builder.kpi("Verdict", verdict)
                report.builder.kpi(
                    "Closed",
                    (
                        f"{stats['closed']} / until depleted"
                        if config.run_until_depleted
                        else f"{stats['closed']}/{config.target_closed_trades}"
                    ),
                )
                report.builder.kpi("Failed", str(stats["failed"]))
                report.builder.kpi("Open", str(len(active)))
                report.builder.kpi("Net PnL", f"{stats['net_pnl']:+.4f} USDT")
                report.builder.kpi("Win rate", f"{stats['win_rate'] * 100:.1f}%")
                pf = stats["profit_factor"]
                report.builder.kpi(
                    "Profit factor", "∞" if math.isinf(pf) else f"{pf:.2f}"
                )
                report.builder.kpi("Max drawdown", f"{stats['max_drawdown']:.4f} USDT")
                report.builder.kpi("Fees", f"{stats['fees']:.4f} USDT")
                report.builder.kpi("Volume", f"{stats['volume']:.2f} USDT")
                report.builder.kpi("Initial equity", f"{initial_equity:.2f} USDT")
                report.builder.kpi("Current equity", f"{account_equity:.2f} USDT")
                report.builder.kpi(
                    "Margin cap",
                    f"{max_margin_usdt:.2f} USDT ({config.margin_per_trade_pct:.0f}%)",
                )
                report.builder.kpi(
                    "Dynamic sizing",
                    f"{config.min_margin_per_trade_pct:.0f}%–{config.margin_per_trade_pct:.0f}%",
                )
                report.builder.kpi("Notional cap", f"{max_notional_usdt:.2f} USDT")
                report.builder.kpi(
                    "Loss limit",
                    (
                        f"disabled; stop below {config.minimum_equity_usdt:.2f} USDT"
                        if config.run_until_depleted
                        else f"{loss_limit_usdt:.2f} USDT ({config.max_loss_pct:.0f}%)"
                    ),
                )
                report.builder.kpi(
                    "Pair selection",
                    (
                        "dynamic gainers/losers"
                        if config.dynamic_pair_selection
                        else daily_pair or "waiting for signal"
                    ),
                )
                report.builder.kpi(
                    "Entry timeout", f"{config.entry_timeout_seconds}s maker cancel"
                )
                report.builder.table(events[-50:])
                await report.update()

                if halt_reason:
                    return (
                        f"{halt_reason}: {verdict}; {stats['closed']} closed, "
                        f"net {stats['net_pnl']:+.4f} USDT, PF "
                        f"{'inf' if math.isinf(stats['profit_factor']) else f'{stats['profit_factor']:.2f}'}, "
                        f"max drawdown {stats['max_drawdown']:.4f} USDT"
                    )
                await asyncio.sleep(config.interval_sec)
    except asyncio.CancelledError:
        payload = await client.executors.search_executors(
            connector_names=[config.connector_name],
            controller_ids=[controller_id],
            limit=max(config.max_entries, 100),
        )
        await _stop_active(client, _executor_rows(payload))
        return "Stopped; all active demo executors were asked to close"
