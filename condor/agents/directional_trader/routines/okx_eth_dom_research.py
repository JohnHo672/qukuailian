"""Demo-only ETH DOM research and execution routine.

The routine observes the public OKX incremental order book and trade stream,
estimates *probabilistic* large-order intent, and looks for replenishment that
is consistent with iceberg/absorption behaviour.  It never places deceptive
orders.  Execution is hard-locked to ``okx_perpetual_demo`` and aggregate
estimated margin is fixed at one 20% allocation of current demo equity.  The
routine never scales in: any existing ETH position, active entry order, or
closing executor blocks a new entry.

The free public ``bbo-tbt`` feed supplies 10 ms-class top-of-book updates while
``books`` supplies 400-level, 100 ms-class depth.  This is still not
exchange-colocated HFT.  The resulting feature rows are labelled with forward
returns and written to JSONL so the observations can be analysed before any
live deployment.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque

import aiohttp
from pydantic import BaseModel, Field, field_validator
from telegram.ext import ContextTypes

from agents.directional_trader.routines.okx_alt_hft_demo import (
    _demo_max_size,
    _ensure_initialized_client,
    _executor_rows,
    _number,
    _okx_candles,
    _stop_active,
    cap_amount_to_exchange_max,
    observed_fee_rates,
    performance_summary,
    quantize_amount,
    retry_network_call,
)
from agents.directional_trader.routines.okx_alt_orderflow_scan import (
    _normalize_candles,
    candle_vwap,
    trade_volume_profile_levels,
)
from condor.reports import LiveReport
from config_manager import get_client
from mcp_servers.hummingbot_api.tools import executor_create

CATEGORY = "Trading Research"
CONTINUOUS = True

OKX_PUBLIC_WS = "wss://ws.okx.com/ws/v5/public"
PAIR = "ETH-USDT"
INST_ID = "ETH-USDT-SWAP"
CONTRACT_VALUE_BASE = 0.1  # OKX public instrument metadata: 1 contract = 0.1 ETH
ENTRY_ORDER_TYPE = 3  # LIMIT_MAKER: retain price control for scalp entries
TAKE_PROFIT_ORDER_TYPE = 3  # LIMIT_MAKER: collect rather than cross when possible
RISK_EXIT_ORDER_TYPE = 1  # MARKET: required by PositionExecutor for SL/time-limit safety


def executor_submission_status(result: dict[str, Any]) -> tuple[bool, str]:
    """Return a truthful executor submission status for reports and throttling."""
    error = str(result.get("error") or "").strip()
    if error:
        return False, error
    executor_id = str(result.get("executor_id") or "").strip()
    if not executor_id:
        return False, "execution API returned no executor_id"
    return True, executor_id


def maker_close_price(direction: str, frame: "BookFrame") -> float:
    """Rest a close on the passive side instead of crossing the spread."""
    if direction == "LONG":
        return frame.best_ask
    if direction == "SHORT":
        return frame.best_bid
    raise ValueError(f"unsupported position direction: {direction}")


def emergency_exit_reason(reason: str) -> bool:
    """Identify exits where fill certainty must override maker-fee preference."""
    return reason in {"defensive account hard-stop", "defensive account time limit"}


class Config(BaseModel):
    """ETH-only, demo-only DOM research configuration."""

    connector_name: str = Field(default="okx_perpetual_demo")
    account_name: str = Field(default="master_account")
    controller_id: str = Field(default="okx-eth-dom-research-v1")
    trading_pair: str = Field(default=PAIR)
    leverage: int = Field(default=10, ge=10, le=10)
    position_margin_pct: float = Field(default=20, ge=20, le=20)
    max_positions: int = Field(default=1, ge=1, le=1)
    min_seconds_between_entries: float = Field(default=10, ge=5, le=300)
    decision_interval_ms: int = Field(default=250, ge=100, le=2000)
    feature_sample_ms: int = Field(default=1000, ge=250, le=5000)
    book_stale_ms: int = Field(default=750, ge=250, le=5000)
    wall_multiple: float = Field(default=4.0, ge=2, le=20)
    min_wall_age_ms: int = Field(default=750, ge=200, le=10000)
    min_obi: float = Field(default=0.12, ge=0.02, le=0.8)
    min_depth_imbalance: float = Field(default=0.08, ge=0.01, le=0.8)
    max_visible_depth_share_pct: float = Field(default=15, ge=1, le=25)
    min_taker_ratio: float = Field(default=0.56, ge=0.5, le=0.9)
    min_intent_score: float = Field(default=0.58, ge=0.4, le=0.95)
    max_spoof_risk: float = Field(default=0.55, ge=0.1, le=0.95)
    min_iceberg_score: float = Field(default=0.45, ge=0.1, le=0.95)
    min_stop_pct: float = Field(default=0.12, ge=0.08, le=0.35)
    max_stop_pct: float = Field(default=0.35, ge=0.12, le=0.50)
    min_take_profit_pct: float = Field(default=0.18, ge=0.12, le=0.60)
    max_take_profit_pct: float = Field(default=0.60, ge=0.18, le=1.0)
    minimum_reward_risk: float = Field(default=1.20, ge=1.1, le=2.0)
    time_limit_seconds: int = Field(default=30, ge=15, le=300)
    default_fee_rate_pct: float = Field(default=0.05, gt=0, le=0.2)
    break_even_buffer_pct: float = Field(default=0.01, ge=0.01, le=0.20)
    trailing_delta_stop_ratio: float = Field(default=0.50, ge=0.20, le=1.0)
    min_trailing_delta_pct: float = Field(default=0.06, ge=0.03, le=0.25)
    max_trailing_delta_pct: float = Field(default=0.25, ge=0.06, le=0.50)
    retail_stop_lookback_seconds: float = Field(default=30, ge=10, le=120)
    retail_stop_recent_seconds: float = Field(default=3, ge=1, le=10)
    retail_stop_excursion_bps: float = Field(default=0.75, ge=0.25, le=5)
    entry_timeout_seconds: float = Field(default=5, ge=2, le=30)
    unfilled_shutdown_grace_seconds: float = Field(default=10, ge=5, le=120)
    minimum_hold_seconds: float = Field(default=0.5, ge=0.5, le=10)
    soft_exit_confirmations: int = Field(default=1, ge=1, le=6)
    exit_cooldown_seconds: float = Field(default=5, ge=2, le=120)
    dust_position_notional_usdt: float = Field(default=5, ge=1, le=20)
    data_dir: str = Field(default="data/research/eth_dom")

    @field_validator("connector_name")
    @classmethod
    def demo_only(cls, value: str) -> str:
        if value != "okx_perpetual_demo":
            raise ValueError("ETH DOM research is hard-locked to OKX demo")
        return value

    @field_validator("trading_pair")
    @classmethod
    def eth_only(cls, value: str) -> str:
        if value != PAIR:
            raise ValueError("ETH DOM research may only trade ETH-USDT")
        return value

    @field_validator("controller_id")
    @classmethod
    def isolated_controller(cls, value: str) -> str:
        if not value.startswith("okx-eth-dom-research-"):
            raise ValueError("controller_id must use the ETH DOM research namespace")
        return value


class WallState:
    def __init__(
        self,
        first_seen: float,
        last_seen: float,
        max_size: float,
        current_size: float,
        add_events: int = 0,
        cancel_events: int = 0,
        replenished_size: float = 0.0,
        executed_near: float = 0.0,
    ) -> None:
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.max_size = max_size
        self.current_size = current_size
        self.add_events = add_events
        self.cancel_events = cancel_events
        self.replenished_size = replenished_size
        self.executed_near = executed_near


class BookFrame:
    def __init__(
        self,
        timestamp: float,
        mid: float,
        best_bid: float,
        best_ask: float,
        obi: float,
        microprice: float,
    ) -> None:
        self.timestamp = timestamp
        self.mid = mid
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.obi = obi
        self.microprice = microprice


class DomState:
    def __init__(
        self,
        bids: dict[float, float] | None = None,
        asks: dict[float, float] | None = None,
        trades: Deque[dict[str, float | str]] | None = None,
        frames: Deque[BookFrame] | None = None,
        walls: dict[tuple[str, float], WallState] | None = None,
        last_seq_id: int | None = None,
        last_book_time: float = 0.0,
        bbo_bid: float = 0.0,
        bbo_ask: float = 0.0,
        bbo_bid_size: float = 0.0,
        bbo_ask_size: float = 0.0,
        last_bbo_time: float = 0.0,
        reconnects: int = 0,
    ) -> None:
        self.bids = bids if bids is not None else {}
        self.asks = asks if asks is not None else {}
        self.trades = trades if trades is not None else deque(maxlen=10000)
        self.frames = frames if frames is not None else deque(maxlen=1200)
        self.walls = walls if walls is not None else {}
        self.last_seq_id = last_seq_id
        self.last_book_time = last_book_time
        self.bbo_bid = bbo_bid
        self.bbo_ask = bbo_ask
        self.bbo_bid_size = bbo_bid_size
        self.bbo_ask_size = bbo_ask_size
        self.last_bbo_time = last_bbo_time
        self.reconnects = reconnects

    def reset_book(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_seq_id = None

    def add_trade(self, row: dict[str, Any]) -> None:
        price = _number(row.get("px"))
        size = _number(row.get("sz"))
        side = str(row.get("side") or "").lower()
        timestamp = _number(row.get("ts")) / 1000 or time.time()
        if price > 0 and size > 0 and side in {"buy", "sell"}:
            self.trades.append(
                {"price": price, "size": size, "side": side, "time": timestamp}
            )

    def apply_bbo(self, message: dict[str, Any]) -> None:
        """Record OKX's free 10 ms best-bid/offer snapshot."""
        for data in message.get("data", []):
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if bids and len(bids[0]) >= 2:
                self.bbo_bid = _number(bids[0][0])
                self.bbo_bid_size = _number(bids[0][1])
            if asks and len(asks[0]) >= 2:
                self.bbo_ask = _number(asks[0][0])
                self.bbo_ask_size = _number(asks[0][1])
            if self.bbo_bid > 0 and self.bbo_ask > self.bbo_bid:
                self.last_bbo_time = time.time()

    def apply_book(self, message: dict[str, Any]) -> None:
        action = str(message.get("action") or "update")
        for data in message.get("data", []):
            seq_id = int(_number(data.get("seqId")))
            prev_seq = int(_number(data.get("prevSeqId")))
            if (
                action != "snapshot"
                and self.last_seq_id is not None
                and prev_seq not in {0, self.last_seq_id}
            ):
                raise RuntimeError("order-book sequence gap")
            if action == "snapshot":
                self.reset_book()
            now = time.time()
            self._apply_side("bid", self.bids, data.get("bids", []), now)
            self._apply_side("ask", self.asks, data.get("asks", []), now)
            if seq_id:
                self.last_seq_id = seq_id
            self.last_book_time = now
            frame = self.book_frame(now)
            if frame:
                self.frames.append(frame)

    def _apply_side(
        self,
        side: str,
        levels: dict[float, float],
        updates: list[list[Any]],
        now: float,
    ) -> None:
        for level in updates:
            if len(level) < 2:
                continue
            price = _number(level[0])
            new_size = _number(level[1])
            if price <= 0:
                continue
            old_size = levels.get(price, 0.0)
            key = (side, price)
            wall = self.walls.get(key)
            if wall is None and new_size > 0:
                wall = WallState(now, now, new_size, new_size)
                self.walls[key] = wall
            elif wall is not None:
                wall.last_seen = now
                wall.max_size = max(wall.max_size, new_size)
                wall.current_size = new_size
                if new_size > old_size:
                    wall.add_events += 1
                    if old_size > 0:
                        wall.replenished_size += new_size - old_size
                elif old_size > 0 and new_size == 0:
                    wall.cancel_events += 1
            if new_size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = new_size

    def book_frame(self, now: float | None = None, depth: int = 20) -> BookFrame | None:
        bids = sorted(self.bids.items(), reverse=True)[:depth]
        asks = sorted(self.asks.items())[:depth]
        if not bids or not asks:
            return None
        best_bid, bid_q = bids[0]
        best_ask, ask_q = asks[0]
        frame_time = now or time.time()
        if (
            frame_time - self.last_bbo_time <= 0.5
            and self.bbo_bid > 0
            and self.bbo_ask > self.bbo_bid
        ):
            best_bid = self.bbo_bid
            best_ask = self.bbo_ask
            bid_q = self.bbo_bid_size or bid_q
            ask_q = self.bbo_ask_size or ask_q
        mid = (best_bid + best_ask) / 2
        bid_depth = sum(size for _, size in bids)
        ask_depth = sum(size for _, size in asks)
        total = bid_depth + ask_depth
        obi = (bid_depth - ask_depth) / total if total else 0.0
        microprice = (
            (best_ask * bid_q + best_bid * ask_q) / (bid_q + ask_q)
            if bid_q + ask_q > 0
            else mid
        )
        return BookFrame(frame_time, mid, best_bid, best_ask, obi, microprice)

    def recent_trade_flow(self, seconds: float = 2.0) -> dict[str, float]:
        cutoff = time.time() - seconds
        buy = sell = 0.0
        for trade in reversed(self.trades):
            if float(trade["time"]) < cutoff:
                break
            notional = (
                float(trade["price"])
                * float(trade["size"])
                * CONTRACT_VALUE_BASE
            )
            if trade["side"] == "buy":
                buy += notional
            else:
                sell += notional
        total = buy + sell
        return {
            "buy_notional": buy,
            "sell_notional": sell,
            "buy_ratio": buy / total if total else 0.5,
            "sell_ratio": sell / total if total else 0.5,
        }


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def infer_dom_intent(
    state: DomState,
    *,
    wall_multiple: float,
    min_wall_age_ms: int,
) -> dict[str, Any]:
    """Infer genuine-wall, spoof-risk and iceberg scores from public data.

    These are behavioural probabilities, not identity claims.  A public book
    cannot reveal who owns an order or whether cancellation intent was illicit.
    """
    frame = state.book_frame()
    if frame is None:
        return {}
    bid_levels = sorted(state.bids.items(), reverse=True)[:20]
    ask_levels = sorted(state.asks.items())[:20]
    sizes = [size for _, size in [*bid_levels, *ask_levels] if size > 0]
    baseline = statistics.median(sizes) if sizes else 0.0
    if baseline <= 0:
        return {}
    now = time.time()
    flow = state.recent_trade_flow(2.0)

    def side_scores(side: str, levels: list[tuple[float, float]]) -> dict[str, float]:
        candidates: list[tuple[float, float, WallState]] = []
        for price, size in levels:
            if size < baseline * wall_multiple:
                continue
            wall = state.walls.get((side, price))
            if wall:
                candidates.append((price, size, wall))
        if not candidates:
            return {
                "intent": 0.0,
                "spoof": 0.0,
                "iceberg": 0.0,
                "wall_price": 0.0,
                "wall_age_ms": 0.0,
            }
        price, size, wall = max(candidates, key=lambda item: item[1])
        age_ms = max(0.0, (now - wall.first_seen) * 1000)
        persistence = _clamp(age_ms / max(min_wall_age_ms, 1))
        size_score = _clamp(size / max(baseline * wall_multiple * 2, 1e-9))
        event_total = wall.add_events + wall.cancel_events
        cancel_ratio = wall.cancel_events / event_total if event_total else 0.0
        opposite_taker = flow["sell_ratio"] if side == "bid" else flow["buy_ratio"]
        traded_near = 0.0
        tick_band = max(frame.mid * 0.00015, abs(frame.best_ask - frame.best_bid) * 2)
        for trade in reversed(state.trades):
            if float(trade["time"]) < now - 2:
                break
            if abs(float(trade["price"]) - price) <= tick_band:
                trade_side = str(trade["side"])
                if (side == "bid" and trade_side == "sell") or (
                    side == "ask" and trade_side == "buy"
                ):
                    traded_near += float(trade["size"])
        wall.executed_near = max(wall.executed_near, traded_near)
        replenish_ratio = wall.replenished_size / max(traded_near, baseline)
        iceberg = _clamp(
            0.45 * _clamp(replenish_ratio)
            + 0.30 * _clamp(traded_near / max(baseline * 2, 1e-9))
            + 0.25 * persistence
        )
        spoof = _clamp(
            0.55 * cancel_ratio
            + 0.25 * (1 - persistence)
            + 0.20 * (1 - _clamp(traded_near / max(baseline, 1e-9)))
        )
        intent = _clamp(
            0.30 * persistence
            + 0.20 * size_score
            + 0.30 * iceberg
            + 0.20 * opposite_taker
            - 0.45 * spoof
        )
        return {
            "intent": intent,
            "spoof": spoof,
            "iceberg": iceberg,
            "wall_price": price,
            "wall_age_ms": age_ms,
        }

    bid = side_scores("bid", bid_levels)
    ask = side_scores("ask", ask_levels)
    return {
        "bid_intent": bid["intent"],
        "ask_intent": ask["intent"],
        "bid_spoof_risk": bid["spoof"],
        "ask_spoof_risk": ask["spoof"],
        "bid_iceberg": bid["iceberg"],
        "ask_iceberg": ask["iceberg"],
        "bid_wall_price": bid["wall_price"],
        "ask_wall_price": ask["wall_price"],
        "bid_wall_age_ms": bid["wall_age_ms"],
        "ask_wall_age_ms": ask["wall_age_ms"],
    }


def market_depth_metrics(
    state: DomState, bands_bps: tuple[int, ...] = (5, 10, 25)
) -> dict[str, float]:
    """Measure executable notional and imbalance across several depth bands."""
    frame = state.book_frame()
    if frame is None:
        return {}
    result: dict[str, float] = {}
    for band in bands_bps:
        width = band / 10_000
        bid_notional = sum(
            price * size * CONTRACT_VALUE_BASE
            for price, size in state.bids.items()
            if frame.mid * (1 - width) <= price <= frame.mid
        )
        ask_notional = sum(
            price * size * CONTRACT_VALUE_BASE
            for price, size in state.asks.items()
            if frame.mid <= price <= frame.mid * (1 + width)
        )
        total = bid_notional + ask_notional
        result[f"bid_depth_{band}bps"] = bid_notional
        result[f"ask_depth_{band}bps"] = ask_notional
        result[f"depth_imbalance_{band}bps"] = (
            (bid_notional - ask_notional) / total if total else 0.0
        )
    bid_5 = result.get("bid_depth_5bps", 0.0)
    ask_5 = result.get("ask_depth_5bps", 0.0)
    bid_25 = result.get("bid_depth_25bps", 0.0)
    ask_25 = result.get("ask_depth_25bps", 0.0)
    result["bid_depth_concentration"] = bid_5 / bid_25 if bid_25 else 0.0
    result["ask_depth_concentration"] = ask_5 / ask_25 if ask_25 else 0.0
    result["depth_ratio_25bps"] = bid_25 / ask_25 if ask_25 else math.inf
    return result


def depth_limited_margin(
    *,
    direction: str,
    requested_margin: float,
    leverage: int,
    depth: dict[str, float],
    max_depth_share_pct: float,
) -> float:
    """Approve the full one-shot order only when both legs have liquidity.

    The strategy no longer scales the requested size down into smaller
    tranches.  It either finds enough visible 25 bps depth to enter *and* later
    exit the complete position inside the configured participation limit, or
    it does not trade.
    """
    if direction not in {"LONG", "SHORT"} or leverage <= 0:
        raise ValueError("invalid depth sizing direction or leverage")
    entry_depth = depth.get(
        "ask_depth_25bps" if direction == "LONG" else "bid_depth_25bps", 0.0
    )
    exit_depth = depth.get(
        "bid_depth_25bps" if direction == "LONG" else "ask_depth_25bps", 0.0
    )
    if requested_margin <= 0 or entry_depth <= 0 or exit_depth <= 0:
        return 0.0
    requested_notional = requested_margin * leverage
    maximum_round_trip_notional = (
        min(entry_depth, exit_depth) * max_depth_share_pct / 100
    )
    return requested_margin if requested_notional <= maximum_round_trip_notional else 0.0


def detect_liquidity_sweep(
    state: DomState, *, val: float, vah: float, lookback_seconds: float = 8.0
) -> dict[str, bool]:
    frame = state.book_frame()
    if frame is None:
        return {"long_reclaim": False, "short_reclaim": False}
    cutoff = time.time() - lookback_seconds
    mids = [item.mid for item in state.frames if item.timestamp >= cutoff]
    if not mids:
        return {"long_reclaim": False, "short_reclaim": False}
    return {
        "long_reclaim": val > 0 and min(mids) < val <= frame.mid,
        "short_reclaim": vah > 0 and max(mids) > vah >= frame.mid,
    }


def detect_retail_stop_run(
    state: DomState,
    *,
    lookback_seconds: float,
    recent_seconds: float,
    excursion_bps: float,
    now: float | None = None,
) -> dict[str, float | bool]:
    """Detect a fast run through the prior micro swing high or low.

    Public market data cannot identify individual traders' stops.  For an
    ultra-short strategy the defensible proxy is a fresh break of the recent
    pre-event high/low: stops from short positions tend to cluster above the
    former, and stops from long positions below the latter.  The reference
    window excludes the newest frames so the level cannot move away while the
    sweep is happening.
    """
    if lookback_seconds <= recent_seconds or recent_seconds <= 0:
        raise ValueError("invalid retail-stop windows")
    if excursion_bps <= 0:
        raise ValueError("retail-stop excursion must be positive")
    observed_at = now if now is not None else time.time()
    cutoff = observed_at - lookback_seconds
    recent_cutoff = observed_at - recent_seconds
    reference = [
        item for item in state.frames if cutoff <= item.timestamp < recent_cutoff
    ]
    recent = [item for item in state.frames if item.timestamp >= recent_cutoff]
    if not reference or not recent:
        return {
            "upper_stop_run": False,
            "lower_stop_run": False,
            "prior_high": 0.0,
            "prior_low": 0.0,
            "recent_high": 0.0,
            "recent_low": 0.0,
        }
    prior_high = max(item.mid for item in reference)
    prior_low = min(item.mid for item in reference)
    recent_high = max(item.mid for item in recent)
    recent_low = min(item.mid for item in recent)
    excursion = excursion_bps / 10_000
    return {
        "upper_stop_run": recent_high >= prior_high * (1 + excursion),
        "lower_stop_run": recent_low <= prior_low * (1 - excursion),
        "prior_high": prior_high,
        "prior_low": prior_low,
        "recent_high": recent_high,
        "recent_low": recent_low,
    }


def dom_signal(
    *,
    frame: BookFrame,
    flow: dict[str, float],
    intent: dict[str, Any],
    depth: dict[str, float],
    sweep: dict[str, bool],
    vwap: float,
    poc: float,
    config: Config,
) -> dict[str, Any]:
    micro_bps = (frame.microprice / frame.mid - 1) * 10_000
    long_checks = {
        "book": frame.obi >= config.min_obi and micro_bps > 0,
        "trades": flow["buy_ratio"] >= config.min_taker_ratio,
        "value": frame.mid > vwap > 0 and frame.mid > poc > 0,
        "intent": intent.get("bid_intent", 0) >= config.min_intent_score,
        "depth": depth.get("depth_imbalance_10bps", 0)
        >= config.min_depth_imbalance,
        "iceberg_or_sweep": intent.get("bid_iceberg", 0)
        >= config.min_iceberg_score
        or sweep["long_reclaim"],
        "not_spoof": intent.get("bid_spoof_risk", 1) <= config.max_spoof_risk,
    }
    short_checks = {
        "book": frame.obi <= -config.min_obi and micro_bps < 0,
        "trades": flow["sell_ratio"] >= config.min_taker_ratio,
        "value": 0 < frame.mid < vwap and 0 < frame.mid < poc,
        "intent": intent.get("ask_intent", 0) >= config.min_intent_score,
        "depth": depth.get("depth_imbalance_10bps", 0)
        <= -config.min_depth_imbalance,
        "iceberg_or_sweep": intent.get("ask_iceberg", 0)
        >= config.min_iceberg_score
        or sweep["short_reclaim"],
        "not_spoof": intent.get("ask_spoof_risk", 1) <= config.max_spoof_risk,
    }
    long_score = sum(long_checks.values())
    short_score = sum(short_checks.values())
    direction = "FLAT"
    checks: dict[str, bool] = {}
    if long_score >= 6 and long_score > short_score:
        direction, checks = "LONG", long_checks
    elif short_score >= 6 and short_score > long_score:
        direction, checks = "SHORT", short_checks
    return {
        "direction": direction,
        "score": max(long_score, short_score),
        "microprice_bps": micro_bps,
        "checks": checks,
        "long_score": long_score,
        "short_score": short_score,
    }


def dynamic_barriers(
    *,
    direction: str,
    price: float,
    vwap: float,
    poc: float,
    val: float,
    vah: float,
    intent: dict[str, Any],
    config: Config,
) -> dict[str, float]:
    """Convert DOM/VWAP/VP structure into bounded executor percentages."""
    if direction not in {"LONG", "SHORT"} or price <= 0:
        raise ValueError("invalid barrier inputs")
    if direction == "LONG":
        supports = [x for x in (val, poc, vwap, intent.get("bid_wall_price", 0)) if 0 < x < price]
        targets = [x for x in (vah, intent.get("ask_wall_price", 0)) if x > price]
        support = max(supports) if supports else price * (1 - config.min_stop_pct / 100)
        raw_stop = (price - support) / price * 100 + 0.04
        raw_target = (min(targets) - price) / price * 100 if targets else 0.0
    else:
        resistances = [x for x in (vah, poc, vwap, intent.get("ask_wall_price", 0)) if x > price]
        targets = [x for x in (val, intent.get("bid_wall_price", 0)) if 0 < x < price]
        resistance = min(resistances) if resistances else price * (1 + config.min_stop_pct / 100)
        raw_stop = (resistance - price) / price * 100 + 0.04
        raw_target = (price - max(targets)) / price * 100 if targets else 0.0
    stop_pct = _clamp(raw_stop, config.min_stop_pct, config.max_stop_pct)
    target_pct = max(raw_target, stop_pct * config.minimum_reward_risk)
    take_profit_pct = _clamp(
        target_pct, config.min_take_profit_pct, config.max_take_profit_pct
    )
    return {"stop_loss_pct": stop_pct, "take_profit_pct": take_profit_pct}


def fee_covered_trailing_barrier(
    *,
    structural_stop_pct: float,
    round_trip_fee_pct: float,
    break_even_buffer_pct: float,
    trailing_delta_stop_ratio: float,
    min_trailing_delta_pct: float,
    max_trailing_delta_pct: float,
) -> dict[str, float]:
    """Arm a trailing stop only after its first protected level covers costs.

    Hummingbot expresses the activation and trail as deltas from price.  The
    strategy still chooses them from market structure: the trail is a fraction
    of the current VWAP/VP/DOM-derived structural stop.  Activation includes
    that trail distance plus observed round-trip fees and a slippage buffer, so
    the first trailing level is already above entry after estimated costs.
    """
    if structural_stop_pct <= 0 or round_trip_fee_pct < 0:
        raise ValueError("invalid trailing barrier inputs")
    if break_even_buffer_pct < 0 or trailing_delta_stop_ratio <= 0:
        raise ValueError("invalid trailing barrier inputs")
    if not 0 < min_trailing_delta_pct <= max_trailing_delta_pct:
        raise ValueError("invalid trailing delta bounds")
    trailing_delta_pct = _clamp(
        structural_stop_pct * trailing_delta_stop_ratio,
        min_trailing_delta_pct,
        max_trailing_delta_pct,
    )
    locked_profit_pct = round_trip_fee_pct + break_even_buffer_pct
    activation_pct = locked_profit_pct + trailing_delta_pct
    return {
        "activation_pct": activation_pct,
        "trailing_delta_pct": trailing_delta_pct,
        "locked_profit_pct": locked_profit_pct,
    }


def active_reserved_margin(rows: list[dict[str, Any]], leverage: int) -> float:
    """Estimate the entry margin reserved by all active ETH executors."""
    total = 0.0
    for row in rows:
        if str(row.get("status") or "").upper() not in {
            "RUNNING",
            "CREATED",
            "SHUTTING_DOWN",
        }:
            continue
        config = row.get("config") if isinstance(row.get("config"), dict) else {}
        amount = _number(config.get("amount") or row.get("amount"))
        entry_price = _number(
            config.get("entry_price")
            or row.get("entry_price")
            or row.get("custom_info", {}).get("entry_price")
        )
        filled_quote = _number(row.get("filled_amount_quote"))
        notional = amount * entry_price if amount > 0 and entry_price > 0 else filled_quote
        total += max(0.0, notional / leverage)
    return total


def executor_lifecycle(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Return active, closing and entry-blocking executor rows.

    A ``SHUTTING_DOWN`` executor still owns an exchange position until its
    close settles.  Treating it as absent caused the routine to submit another
    tranche while the previous one was still closing.
    """
    active: list[dict[str, Any]] = []
    closing: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status in {"RUNNING", "CREATED"}:
            active.append(row)
        elif status == "SHUTTING_DOWN":
            closing.append(row)
    return active, closing, [*active, *closing]


def executor_direction(row: dict[str, Any]) -> str | None:
    """Normalize both legacy numeric and current string executor sides.

    Active Hummingbot executors return ``BUY``/``SELL`` while archived rows may
    return ``1``/``2``.  Treating every non-numeric side as short caused live
    long executors to be evaluated and reported as shorts.
    """
    config = row.get("config") if isinstance(row.get("config"), dict) else {}
    custom = (
        row.get("custom_info")
        if isinstance(row.get("custom_info"), dict)
        else {}
    )
    raw = config.get("side") or row.get("side") or custom.get("side")
    value = str(raw or "").strip().upper()
    if value in {"1", "1.0", "BUY", "LONG"} or value.endswith(".BUY"):
        return "LONG"
    if value in {"2", "2.0", "SELL", "SHORT"} or value.endswith(".SELL"):
        return "SHORT"
    return None


def executor_entry_price(row: dict[str, Any]) -> float:
    """Return the actual average fill when available, then the requested price."""
    config = row.get("config") if isinstance(row.get("config"), dict) else {}
    custom = (
        row.get("custom_info")
        if isinstance(row.get("custom_info"), dict)
        else {}
    )
    return _number(
        custom.get("current_position_average_price")
        or custom.get("entry_price")
        or config.get("entry_price")
        or row.get("entry_price")
    )


def executor_has_fill(row: dict[str, Any]) -> bool:
    """Whether a running executor owns a filled position, not only an order."""
    return bool(row.get("is_trading")) or _number(row.get("filled_amount_quote")) > 0


def shutdown_blocks_entry(
    row: dict[str, Any],
    *,
    observed_seconds: float,
    grace_seconds: float,
    account_has_position: bool = True,
) -> bool:
    """Keep real positions blocking, but release cancelled unfilled ghosts.

    Hummingbot can leave an unfilled executor in ``SHUTTING_DOWN`` after its
    exchange order has been cancelled.  After a conservative grace window it
    must not consume strategy margin when the venue itself confirms the pair is
    flat.  A filled/trading executor continues blocking while the account still
    has exposure; this avoids trusting an executor's delayed terminal state.
    """
    if observed_seconds < 0 or grace_seconds <= 0:
        raise ValueError("invalid shutdown grace inputs")
    return observed_seconds < grace_seconds or (
        executor_has_fill(row) and account_has_position
    )


def favorable_move_pct(direction: str, entry_price: float, exit_price: float) -> float:
    """Gross price move in the executor's favour, in percentage points."""
    if direction not in {"LONG", "SHORT"} or entry_price <= 0 or exit_price <= 0:
        raise ValueError("invalid favourable-move inputs")
    if direction == "LONG":
        return (exit_price / entry_price - 1) * 100
    return (entry_price / exit_price - 1) * 100


def protected_exit_reason(
    *,
    candidate_reason: str | None,
    candidate_confirmations: int,
    required_confirmations: int,
    current_gross_pct: float,
    peak_gross_pct: float,
    round_trip_fee_pct: float,
    break_even_buffer_pct: float,
    trailing_activation_pct: float,
    trailing_delta_pct: float,
    held_seconds: float,
    minimum_hold_seconds: float,
) -> str | None:
    """Permit discretionary exits only when they protect fee-inclusive profit.

    Native executor stop-loss remains responsible for genuinely invalid losing
    trades. DOM/VWAP/VP reversals are noisy and must not crystallise a small
    loss. Once gross profit covers the observed round-trip fees and buffer,
    however, the first adverse liquidity anomaly exits immediately so a scalp
    winner is not allowed to turn into a loser.
    """
    if round_trip_fee_pct < 0 or break_even_buffer_pct < 0:
        raise ValueError("invalid exit cost inputs")
    fee_covered_floor = round_trip_fee_pct + break_even_buffer_pct
    trailing_armed = peak_gross_pct >= trailing_activation_pct
    trailing_hit = (
        trailing_armed
        and trailing_delta_pct > 0
        and current_gross_pct <= peak_gross_pct - trailing_delta_pct
    )
    if trailing_hit:
        return "fee-covered trailing profit protection"
    if (
        candidate_reason
        and candidate_reason.startswith("defensive ")
        and current_gross_pct >= fee_covered_floor
    ):
        return f"profitable liquidity anomaly: {candidate_reason}"
    profit_target_reached = bool(
        candidate_reason and candidate_reason.startswith("retail ")
    )
    if (
        not candidate_reason
        or (held_seconds < minimum_hold_seconds and not profit_target_reached)
    ):
        return None
    if candidate_confirmations < required_confirmations:
        return None
    if current_gross_pct < fee_covered_floor:
        return None
    return candidate_reason


def one_shot_position_margin(
    *, equity: float, position_margin_pct: float, has_exposure: bool
) -> float:
    """Return exactly one fixed-size allocation or no execution permission."""
    if equity < 0 or position_margin_pct != 20:
        raise ValueError("invalid one-shot margin inputs")
    # An empty demo account is a valid research state, not a strategy error.
    # Keep the DOM collector and feature distiller alive while granting no
    # execution permission.  This preserves the hard risk lock: no synthetic
    # balance and no order can be created until the venue reports equity again.
    if equity == 0:
        return 0.0
    if has_exposure:
        return 0.0
    return equity * position_margin_pct / 100


def pair_position_rows(
    payload: Any,
    trading_pair: str = PAIR,
    min_notional_usdt: float = 0,
) -> list[dict[str, Any]]:
    """Extract executable account positions while ignoring venue dust."""
    if min_notional_usdt < 0:
        raise ValueError("minimum position notional cannot be negative")
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    return [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("trading_pair") or "").upper() == trading_pair
        and abs(_number(row.get("amount"))) > 0
        and abs(_number(row.get("amount")))
        * _number(row.get("entry_price"))
        >= min_notional_usdt
    ]


def pair_active_order_rows(
    payload: Any, trading_pair: str = PAIR
) -> list[dict[str, Any]]:
    """Return all non-terminal venue orders for the configured pair.

    The Hummingbot executor and position endpoints can lag one another.  The
    exchange-order view is therefore an independent execution lock: while it
    reports an order as live, the strategy may neither open another position
    nor let the orphan-position fallback submit a competing close.
    """
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    terminal = {"CANCELLED", "CANCELED", "FILLED", "FAILED", "REJECTED"}
    return [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("trading_pair") or "").upper() == trading_pair
        and str(row.get("status") or "").upper() not in terminal
    ]


def account_position_direction(row: dict[str, Any]) -> str | None:
    """Normalize a venue position direction without trusting one field alone."""
    side = str(row.get("side") or "").upper()
    amount = _number(row.get("amount"))
    if side in {"LONG", "BUY"} or amount > 0:
        return "LONG"
    if side in {"SHORT", "SELL"} or amount < 0:
        return "SHORT"
    return None


def pending_entry_should_expire(
    *,
    pending_seconds: float,
    timeout_seconds: float,
    account_has_position: bool,
    venue_order_active: bool = False,
) -> bool:
    """Cancel a stale maker entry only while the venue still reports no position.

    The active order remains an execution lock after cancellation is requested,
    so a late fill cannot permit a second entry.
    """
    if pending_seconds < 0 or timeout_seconds <= 0:
        raise ValueError("invalid pending-entry timeout inputs")
    return pending_seconds >= timeout_seconds and not account_has_position


class FeatureDistiller:
    """Write delayed labels and compact signal statistics for future research."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.pending: Deque[dict[str, Any]] = deque()
        self.summary: dict[str, dict[str, float]] = defaultdict(
            lambda: {"count": 0.0, "sum_return_1s": 0.0, "sum_return_5s": 0.0, "sum_return_30s": 0.0}
        )

    def observe(self, row: dict[str, Any]) -> None:
        now = float(row["timestamp"])
        price = float(row["mid"])
        for item in self.pending:
            elapsed = now - float(item["timestamp"])
            for horizon in (1, 5, 30):
                key = f"future_return_{horizon}s_bps"
                if elapsed >= horizon and key not in item:
                    item[key] = (price / float(item["mid"]) - 1) * 10_000
        self.pending.append(dict(row))
        self._flush_ready(now)

    def _flush_ready(self, now: float) -> None:
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        path = self.root / f"features-{day}.jsonl"
        while self.pending and now - float(self.pending[0]["timestamp"]) >= 30:
            item = self.pending.popleft()
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
            label = str(item.get("signal") or "FLAT")
            bucket = self.summary[label]
            bucket["count"] += 1
            for horizon in (1, 5, 30):
                bucket[f"sum_return_{horizon}s"] += _number(
                    item.get(f"future_return_{horizon}s_bps")
                )
        self._write_summary()

    def _write_summary(self) -> None:
        distilled: dict[str, Any] = {}
        for label, bucket in self.summary.items():
            count = bucket["count"]
            distilled[label] = {
                "count": int(count),
                **{
                    f"mean_return_{horizon}s_bps": (
                        bucket[f"sum_return_{horizon}s"] / count if count else 0.0
                    )
                    for horizon in (1, 5, 30)
                },
            }
        temporary = self.root / "distilled-summary.tmp"
        temporary.write_text(
            json.dumps(distilled, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(self.root / "distilled-summary.json")


async def _dom_stream(state: DomState) -> None:
    delay = 1.0
    while True:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_read=25)
            ) as session:
                async with session.ws_connect(OKX_PUBLIC_WS, heartbeat=15) as ws:
                    await ws.send_json(
                        {
                            "op": "subscribe",
                            "args": [
                                {"channel": "bbo-tbt", "instId": INST_ID},
                                {"channel": "books", "instId": INST_ID},
                                {"channel": "trades", "instId": INST_ID},
                            ],
                        }
                    )
                    delay = 1.0
                    async for event in ws:
                        if event.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(event.data)
                        channel = str(payload.get("arg", {}).get("channel") or "")
                        if channel == "books":
                            try:
                                state.apply_book(payload)
                            except RuntimeError:
                                state.reset_book()
                                raise
                        elif channel == "bbo-tbt":
                            state.apply_bbo(payload)
                        elif channel == "trades":
                            for trade in payload.get("data", []):
                                state.add_trade(trade)
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError, RuntimeError):
            state.reconnects += 1
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2)


async def _market_structure(session: aiohttp.ClientSession) -> dict[str, float]:
    candles, trades = await asyncio.gather(
        _okx_candles(session, INST_ID),
        _okx_public_json(session, "/api/v5/market/trades", {"instId": INST_ID, "limit": "500"}),
    )
    normalized = _normalize_candles(candles)
    if len(normalized) < 30:
        raise RuntimeError("insufficient ETH candles")
    vwap, _ = candle_vwap(normalized, 30)
    poc, val, vah = trade_volume_profile_levels(trades, 30)
    return {"vwap": vwap, "poc": poc, "val": val, "vah": vah}


async def _okx_public_json(
    session: aiohttp.ClientSession, path: str, params: dict[str, str]
) -> list[dict[str, Any]]:
    async with session.get(f"https://www.okx.com{path}", params=params) as response:
        response.raise_for_status()
        payload = await response.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(str(payload.get("msg") or "OKX rejected market request"))
    return [row for row in payload.get("data", []) if isinstance(row, dict)]


def should_exit(
    direction: str,
    *,
    frame: BookFrame,
    flow: dict[str, float],
    intent: dict[str, Any],
    depth: dict[str, float] | None,
    vwap: float,
    poc: float,
    retail_stop_run: dict[str, float | bool] | None = None,
) -> str | None:
    retail_stop_run = retail_stop_run or {}
    depth = depth or {}
    bid_depth = depth.get("bid_depth_10bps", 0.0)
    ask_depth = depth.get("ask_depth_10bps", 0.0)
    depth_imbalance = depth.get("depth_imbalance_5bps", 0.0)
    if direction == "LONG":
        if retail_stop_run.get("upper_stop_run"):
            return "retail short-stop pool reached"
        if (
            bid_depth > 0
            and ask_depth > 0
            and bid_depth < ask_depth * 0.65
            and flow["sell_ratio"] >= 0.56
        ):
            return "defensive bid-liquidity collapse"
        if depth_imbalance <= -0.18 and intent.get("ask_intent", 0) >= 0.58:
            return "defensive near-book ask dominance"
        if frame.mid < min(vwap, poc) and frame.obi < -0.08:
            return "defensive VWAP/POC loss with adverse book"
        if flow["sell_ratio"] >= 0.60 and intent.get("ask_intent", 0) >= 0.60:
            return "defensive confirmed ask absorption"
    elif direction == "SHORT":
        if retail_stop_run.get("lower_stop_run"):
            return "retail long-stop pool reached"
        if (
            bid_depth > 0
            and ask_depth > 0
            and ask_depth < bid_depth * 0.65
            and flow["buy_ratio"] >= 0.56
        ):
            return "defensive ask-liquidity collapse"
        if depth_imbalance >= 0.18 and intent.get("bid_intent", 0) >= 0.58:
            return "defensive near-book bid dominance"
        if frame.mid > max(vwap, poc) and frame.obi > 0.08:
            return "defensive VWAP/POC reclaim with adverse book"
        if flow["buy_ratio"] >= 0.60 and intent.get("bid_intent", 0) >= 0.60:
            return "defensive confirmed bid absorption"
    return None


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Collect ETH DOM features and execute one 20% demo position at a time."""
    if config.connector_name != "okx_perpetual_demo" or config.trading_pair != PAIR:
        return "Refused: ETH-USDT on OKX demo is mandatory"
    chat_id = getattr(context, "_chat_id", 0)
    client = await get_client(chat_id, context=context)
    if not client:
        return "No Hummingbot server available"

    state = DomState()
    stream_task = asyncio.create_task(_dom_stream(state))
    root = Path(os.path.expanduser(config.data_dir))
    if not root.is_absolute():
        root = Path(__file__).resolve().parents[3] / root
    distiller = FeatureDistiller(root)
    report = LiveReport(
        "ETH DOM Liquidity Research — Demo",
        source_name="okx_eth_dom_research",
        tags=["okx", "demo", "eth", "dom", "iceberg", "research"],
    )
    last_structure_at = 0.0
    structure = {"vwap": 0.0, "poc": 0.0, "val": 0.0, "vah": 0.0}
    last_sample_at = 0.0
    last_venue_state_at = 0.0
    account_positions: list[dict[str, Any]] = []
    account_active_orders: list[dict[str, Any]] = []
    last_entry_at = 0.0
    exit_cooldown_until = 0.0
    exit_trackers: dict[str, dict[str, Any]] = {}
    account_exit_tracker: dict[str, Any] = {}
    account_exit_pending_until = 0.0
    manual_limit_exits: dict[str, dict[str, Any]] = {}
    shutdown_seen_at: dict[str, float] = {}
    last_signal = "FLAT"
    last_trailing = {
        "activation_pct": 0.0,
        "trailing_delta_pct": 0.0,
        "locked_profit_pct": 0.0,
    }
    events: list[dict[str, Any]] = []

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            while True:
                await _ensure_initialized_client(client)
                now = time.time()
                if now - last_structure_at >= 10:
                    structure = await retry_network_call(
                        lambda: _market_structure(session), max_delay=30
                    )
                    last_structure_at = now
                frame = state.book_frame(now)
                if frame is None or (now - state.last_book_time) * 1000 > config.book_stale_ms:
                    last_signal = "STALE"
                    await asyncio.sleep(config.decision_interval_ms / 1000)
                    continue

                flow = state.recent_trade_flow(2.0)
                intent = infer_dom_intent(
                    state,
                    wall_multiple=config.wall_multiple,
                    min_wall_age_ms=config.min_wall_age_ms,
                )
                depth = market_depth_metrics(state)
                sweep = detect_liquidity_sweep(
                    state, val=structure["val"], vah=structure["vah"]
                )
                retail_stop_run = detect_retail_stop_run(
                    state,
                    lookback_seconds=config.retail_stop_lookback_seconds,
                    recent_seconds=config.retail_stop_recent_seconds,
                    excursion_bps=config.retail_stop_excursion_bps,
                    now=now,
                )
                signal = dom_signal(
                    frame=frame,
                    flow=flow,
                    intent=intent,
                    depth=depth,
                    sweep=sweep,
                    vwap=structure["vwap"],
                    poc=structure["poc"],
                    config=config,
                )
                last_signal = signal["direction"]

                if now - last_sample_at >= config.feature_sample_ms / 1000:
                    distiller.observe(
                        {
                            "timestamp": now,
                            "pair": PAIR,
                            "mid": frame.mid,
                            "spread_bps": (frame.best_ask / frame.best_bid - 1) * 10_000,
                            "obi": frame.obi,
                            "microprice_bps": signal["microprice_bps"],
                            "buy_ratio": flow["buy_ratio"],
                            "sell_ratio": flow["sell_ratio"],
                            "bid_depth_5bps": depth.get("bid_depth_5bps", 0),
                            "ask_depth_5bps": depth.get("ask_depth_5bps", 0),
                            "bid_depth_10bps": depth.get("bid_depth_10bps", 0),
                            "ask_depth_10bps": depth.get("ask_depth_10bps", 0),
                            "bid_depth_25bps": depth.get("bid_depth_25bps", 0),
                            "ask_depth_25bps": depth.get("ask_depth_25bps", 0),
                            "depth_imbalance_5bps": depth.get("depth_imbalance_5bps", 0),
                            "depth_imbalance_10bps": depth.get("depth_imbalance_10bps", 0),
                            "depth_imbalance_25bps": depth.get("depth_imbalance_25bps", 0),
                            "bid_depth_concentration": depth.get("bid_depth_concentration", 0),
                            "ask_depth_concentration": depth.get("ask_depth_concentration", 0),
                            "vwap_distance_bps": (frame.mid / structure["vwap"] - 1) * 10_000 if structure["vwap"] else 0,
                            "poc_distance_bps": (frame.mid / structure["poc"] - 1) * 10_000 if structure["poc"] else 0,
                            "long_sweep_reclaim": sweep["long_reclaim"],
                            "short_sweep_reclaim": sweep["short_reclaim"],
                            "upper_retail_stop_run": retail_stop_run["upper_stop_run"],
                            "lower_retail_stop_run": retail_stop_run["lower_stop_run"],
                            "retail_stop_prior_high": retail_stop_run["prior_high"],
                            "retail_stop_prior_low": retail_stop_run["prior_low"],
                            "signal": signal["direction"],
                            **intent,
                        }
                    )
                    last_sample_at = now

                equity = _number(
                    await retry_network_call(
                        lambda: client.portfolio.get_total_value(
                            account_name=config.account_name,
                            connector_name=config.connector_name,
                        )
                    )
                )
                payload = await retry_network_call(
                    lambda: client.executors.search_executors(
                        account_names=[config.account_name],
                        connector_names=[config.connector_name],
                        controller_ids=[config.controller_id],
                        limit=500,
                    )
                )
                rows = _executor_rows(payload)
                if now - last_venue_state_at >= 1:
                    positions_payload = await retry_network_call(
                        lambda: client.trading.get_positions(
                            account_names=[config.account_name],
                            connector_names=[config.connector_name],
                            limit=100,
                        )
                    )
                    orders_payload = await retry_network_call(
                        lambda: client.trading.get_active_orders(
                            account_names=[config.account_name],
                            connector_names=[config.connector_name],
                            trading_pairs=[PAIR],
                            limit=100,
                        )
                    )
                    account_positions = pair_position_rows(
                        positions_payload,
                        min_notional_usdt=config.dust_position_notional_usdt,
                    )
                    account_active_orders = pair_active_order_rows(orders_payload)
                    last_venue_state_at = now
                active, closing, _ = executor_lifecycle(rows)
                closing_ids: set[str] = set()
                effective_closing: list[dict[str, Any]] = []
                ignored_unfilled_shutdowns = 0
                for row in closing:
                    executor_id = str(
                        row.get("id") or row.get("executor_id") or ""
                    )
                    if not executor_id:
                        effective_closing.append(row)
                        continue
                    closing_ids.add(executor_id)
                    first_seen = shutdown_seen_at.setdefault(executor_id, now)
                    if shutdown_blocks_entry(
                        row,
                        observed_seconds=now - first_seen,
                        grace_seconds=config.unfilled_shutdown_grace_seconds,
                        account_has_position=bool(account_positions),
                    ):
                        effective_closing.append(row)
                    else:
                        ignored_unfilled_shutdowns += 1
                for executor_id in list(shutdown_seen_at):
                    if executor_id not in closing_ids:
                        shutdown_seen_at.pop(executor_id, None)
                blocking = [*active, *effective_closing]
                fee_rates = observed_fee_rates(rows)
                estimated_round_trip = 2 * fee_rates.get(
                    PAIR, config.default_fee_rate_pct
                )
                exited_this_tick = False
                active_ids: set[str] = set()
                for row in active:
                    executor_id = str(row.get("id") or row.get("executor_id") or "")
                    direction = executor_direction(row)
                    if not executor_id or direction is None:
                        continue
                    active_ids.add(executor_id)
                    tracker = exit_trackers.setdefault(
                        executor_id,
                        {
                            "first_seen": now,
                            "filled_since": 0.0,
                            "peak_gross_pct": 0.0,
                            "candidate_reason": None,
                            "candidate_confirmations": 0,
                        },
                    )
                    if not executor_has_fill(row):
                        pending_seconds = now - _number(tracker.get("first_seen"))
                        # Maker entries retain price control. Once timed out,
                        # ask the executor to cancel but keep all venue-state
                        # locks engaged until the order and any late fill have
                        # definitively disappeared.
                        if pending_entry_should_expire(
                            pending_seconds=pending_seconds,
                            timeout_seconds=config.entry_timeout_seconds,
                            account_has_position=bool(account_positions),
                        ):
                            await client.executors.stop_executor(
                                executor_id=executor_id, keep_position=False
                            )
                            exit_cooldown_until = (
                                now + config.exit_cooldown_seconds
                            )
                            exited_this_tick = True
                            events.append(
                                {
                                    "Time": time.strftime("%H:%M:%S"),
                                    "Action": "CANCEL",
                                    "Side": direction,
                                    "Margin": "—",
                                    "Reason": "unfilled market entry expired",
                                }
                            )
                            exit_trackers.pop(executor_id, None)
                        continue
                    entry_price = executor_entry_price(row)
                    if entry_price <= 0:
                        continue
                    mark_price = (
                        frame.best_bid if direction == "LONG" else frame.best_ask
                    )
                    current_gross_pct = favorable_move_pct(
                        direction, entry_price, mark_price
                    )
                    if _number(tracker.get("filled_since")) <= 0:
                        tracker["filled_since"] = now
                    tracker["peak_gross_pct"] = max(
                        _number(tracker.get("peak_gross_pct")), current_gross_pct
                    )
                    candidate = should_exit(
                        direction,
                        frame=frame,
                        flow=flow,
                        intent=intent,
                        depth=depth,
                        vwap=structure["vwap"],
                        poc=structure["poc"],
                        retail_stop_run=retail_stop_run,
                    )
                    if candidate == tracker.get("candidate_reason"):
                        tracker["candidate_confirmations"] = int(
                            tracker.get("candidate_confirmations", 0)
                        ) + 1
                    else:
                        tracker["candidate_reason"] = candidate
                        tracker["candidate_confirmations"] = 1 if candidate else 0
                    required_confirmations = (
                        1
                        if candidate
                        and candidate.startswith("retail ")
                        else config.soft_exit_confirmations
                    )
                    reason = protected_exit_reason(
                        candidate_reason=candidate,
                        candidate_confirmations=int(
                            tracker.get("candidate_confirmations", 0)
                        ),
                        required_confirmations=required_confirmations,
                        current_gross_pct=current_gross_pct,
                        peak_gross_pct=_number(tracker.get("peak_gross_pct")),
                        round_trip_fee_pct=estimated_round_trip,
                        break_even_buffer_pct=config.break_even_buffer_pct,
                        trailing_activation_pct=last_trailing["activation_pct"],
                        trailing_delta_pct=last_trailing["trailing_delta_pct"],
                        held_seconds=now - _number(tracker.get("filled_since")),
                        minimum_hold_seconds=config.minimum_hold_seconds,
                    )
                    if reason:
                        # Detach the position without letting the executor use
                        # its default market close. Once its existing child
                        # orders are gone, the state machine below submits one
                        # passive LIMIT_MAKER close.
                        if executor_id not in manual_limit_exits:
                            await client.executors.stop_executor(
                                executor_id=executor_id, keep_position=True
                            )
                            manual_limit_exits[executor_id] = {
                                "direction": direction,
                                "reason": reason,
                                "requested_at": now,
                                "submitted": False,
                            }
                        exit_cooldown_until = now + config.exit_cooldown_seconds
                        exited_this_tick = True
                        events.append(
                            {
                                "Time": time.strftime("%H:%M:%S"),
                                "Action": "EXIT LIMIT QUEUED",
                                "Side": direction,
                                "Margin": "—",
                                "Reason": (
                                    f"{reason}; gross {current_gross_pct:+.3f}%; "
                                    f"peak {_number(tracker.get('peak_gross_pct')):+.3f}%; "
                                    f"cost floor {estimated_round_trip + config.break_even_buffer_pct:.3f}%"
                                ),
                            }
                        )
                        exit_trackers.pop(executor_id, None)
                for executor_id in list(exit_trackers):
                    if executor_id not in active_ids:
                        exit_trackers.pop(executor_id, None)

                # Complete a profitable anomaly exit with one passive maker
                # order only after the executor's own child orders have
                # disappeared. The submitted flag prevents retries while the
                # connector's order/position views catch up.
                if manual_limit_exits and account_positions:
                    position = account_positions[0] if len(account_positions) == 1 else None
                    direction = account_position_direction(position or {})
                    for executor_id, request in list(manual_limit_exits.items()):
                        if (
                            position
                            and direction == request.get("direction")
                            and not request.get("submitted")
                            and not account_active_orders
                        ):
                            limit_price = maker_close_price(direction, frame)
                            await retry_network_call(
                                lambda: client.trading.place_order(
                                    account_name=config.account_name,
                                    connector_name=config.connector_name,
                                    trading_pair=PAIR,
                                    trade_type=(
                                        "SELL" if direction == "LONG" else "BUY"
                                    ),
                                    amount=abs(_number(position.get("amount"))),
                                    order_type="LIMIT_MAKER",
                                    price=limit_price,
                                    position_action="CLOSE",
                                )
                            )
                            request["submitted"] = True
                            request["submitted_at"] = now
                            account_active_orders = [
                                {
                                    "trading_pair": PAIR,
                                    "status": "SUBMITTED",
                                    "position_action": "CLOSE",
                                }
                            ]
                            events.append(
                                {
                                    "Time": time.strftime("%H:%M:%S"),
                                    "Action": "EXIT LIMIT",
                                    "Side": direction,
                                    "Margin": "—",
                                    "Reason": str(request.get("reason") or "anomaly"),
                                }
                            )
                elif manual_limit_exits:
                    for executor_id, request in list(manual_limit_exits.items()):
                        if request.get("submitted") or now - _number(
                            request.get("requested_at")
                        ) >= 30:
                            manual_limit_exits.pop(executor_id, None)

                # A position can outlive its executor when a maker entry fills
                # during cancellation or the executor process disappears.  A
                # one-position strategy must adopt that venue exposure for
                # exits instead of merely blocking the next entry forever.
                # Never let the fallback account-position manager race an
                # executor or an exchange order whose fill status is delayed.
                # That race can submit a second close and reverse the venue
                # position. An orphan is adopted only when all three sources
                # agree that no other execution owner exists.
                account_position_managed = bool(
                    account_positions
                    and not blocking
                    and not account_active_orders
                    and not manual_limit_exits
                )
                if len(account_positions) == 1 and account_position_managed:
                    position = account_positions[0]
                    direction = account_position_direction(position)
                    entry_price = _number(position.get("entry_price"))
                    if direction and entry_price > 0:
                        tracker_key = (
                            f"{direction}:{entry_price:.12g}:"
                            f"{abs(_number(position.get('amount'))):.12g}"
                        )
                        if account_exit_tracker.get("key") != tracker_key:
                            account_exit_tracker = {
                                "key": tracker_key,
                                "first_seen": now,
                                "peak_gross_pct": 0.0,
                                "candidate_reason": None,
                                "candidate_confirmations": 0,
                            }
                        mark_price = (
                            frame.best_bid if direction == "LONG" else frame.best_ask
                        )
                        current_gross_pct = favorable_move_pct(
                            direction, entry_price, mark_price
                        )
                        account_exit_tracker["peak_gross_pct"] = max(
                            _number(account_exit_tracker.get("peak_gross_pct")),
                            current_gross_pct,
                        )
                        candidate = should_exit(
                            direction,
                            frame=frame,
                            flow=flow,
                            intent=intent,
                            depth=depth,
                            vwap=structure["vwap"],
                            poc=structure["poc"],
                            retail_stop_run=retail_stop_run,
                        )
                        if candidate == account_exit_tracker.get("candidate_reason"):
                            account_exit_tracker["candidate_confirmations"] = int(
                                account_exit_tracker.get("candidate_confirmations", 0)
                            ) + 1
                        else:
                            account_exit_tracker["candidate_reason"] = candidate
                            account_exit_tracker["candidate_confirmations"] = (
                                1 if candidate else 0
                            )
                        held_seconds = now - _number(
                            account_exit_tracker.get("first_seen")
                        )
                        required_confirmations = (
                            1
                            if candidate and candidate.startswith("retail ")
                            else config.soft_exit_confirmations
                        )
                        reason = protected_exit_reason(
                            candidate_reason=candidate,
                            candidate_confirmations=int(
                                account_exit_tracker.get(
                                    "candidate_confirmations", 0
                                )
                            ),
                            required_confirmations=required_confirmations,
                            current_gross_pct=current_gross_pct,
                            peak_gross_pct=_number(
                                account_exit_tracker.get("peak_gross_pct")
                            ),
                            round_trip_fee_pct=estimated_round_trip,
                            break_even_buffer_pct=config.break_even_buffer_pct,
                            trailing_activation_pct=last_trailing["activation_pct"],
                            trailing_delta_pct=last_trailing["trailing_delta_pct"],
                            held_seconds=held_seconds,
                            minimum_hold_seconds=config.minimum_hold_seconds,
                        )
                        if current_gross_pct <= -config.max_stop_pct:
                            reason = "defensive account hard-stop"
                        elif held_seconds >= config.time_limit_seconds:
                            reason = "defensive account time limit"
                        if reason and now >= account_exit_pending_until:
                            is_emergency = emergency_exit_reason(reason)
                            close_order_type = "MARKET" if is_emergency else "LIMIT_MAKER"
                            close_price = (
                                None
                                if is_emergency
                                else maker_close_price(direction, frame)
                            )
                            await retry_network_call(
                                lambda: client.trading.place_order(
                                    account_name=config.account_name,
                                    connector_name=config.connector_name,
                                    trading_pair=PAIR,
                                    trade_type=(
                                        "SELL" if direction == "LONG" else "BUY"
                                    ),
                                    amount=abs(_number(position.get("amount"))),
                                    order_type=close_order_type,
                                    price=close_price,
                                    position_action="CLOSE",
                                )
                            )
                            # Lock execution immediately; do not wait up to a
                            # second for the active-order endpoint to reflect
                            # the newly submitted close.
                            account_active_orders = [
                                {
                                    "trading_pair": PAIR,
                                    "status": "SUBMITTED",
                                    "position_action": "CLOSE",
                                }
                            ]
                            account_exit_pending_until = now + 5
                            exit_cooldown_until = now + config.exit_cooldown_seconds
                            exited_this_tick = True
                            events.append(
                                {
                                    "Time": time.strftime("%H:%M:%S"),
                                    "Action": "EXIT",
                                    "Side": direction,
                                    "Margin": "ACCOUNT",
                                    "Reason": (
                                        f"adopted position: {reason}; gross "
                                        f"{current_gross_pct:+.3f}%"
                                    ),
                                }
                            )
                elif not account_positions:
                    account_exit_tracker = {}
                    account_exit_pending_until = 0.0

                reserved = active_reserved_margin(blocking, config.leverage)
                position_margin = one_shot_position_margin(
                    equity=equity,
                    position_margin_pct=config.position_margin_pct,
                    has_exposure=bool(
                        blocking or account_positions or account_active_orders
                    ),
                )
                can_enter = (
                    signal["direction"] in {"LONG", "SHORT"}
                    and not exited_this_tick
                    and not active
                    and not effective_closing
                    and not account_positions
                    and not account_active_orders
                    and now >= exit_cooldown_until
                    and not blocking
                    and position_margin > 0
                    and now - last_entry_at >= config.min_seconds_between_entries
                )
                if can_enter:
                    direction = signal["direction"]
                    position_margin = depth_limited_margin(
                        direction=direction,
                        requested_margin=position_margin,
                        leverage=config.leverage,
                        depth=depth,
                        max_depth_share_pct=config.max_visible_depth_share_pct,
                    )
                    if position_margin <= 0:
                        can_enter = False
                if can_enter:
                    direction = signal["direction"]
                    barriers = dynamic_barriers(
                        direction=direction,
                        price=frame.mid,
                        vwap=structure["vwap"],
                        poc=structure["poc"],
                        val=structure["val"],
                        vah=structure["vah"],
                        intent=intent,
                        config=config,
                    )
                    trailing = fee_covered_trailing_barrier(
                        structural_stop_pct=barriers["stop_loss_pct"],
                        round_trip_fee_pct=estimated_round_trip,
                        break_even_buffer_pct=config.break_even_buffer_pct,
                        trailing_delta_stop_ratio=config.trailing_delta_stop_ratio,
                        min_trailing_delta_pct=config.min_trailing_delta_pct,
                        max_trailing_delta_pct=config.max_trailing_delta_pct,
                    )
                    # Do not let a fixed structural target close the executor
                    # before fee-covered trailing protection has armed.
                    barriers["take_profit_pct"] = _clamp(
                        max(
                            barriers["take_profit_pct"],
                            trailing["activation_pct"]
                            + trailing["trailing_delta_pct"],
                        ),
                        config.min_take_profit_pct,
                        config.max_take_profit_pct,
                    )
                    if barriers["take_profit_pct"] <= estimated_round_trip * 2:
                        can_enter = False
                    else:
                        rules_payload = await retry_network_call(
                            lambda: client.connectors.get_trading_rules(
                                config.connector_name, [PAIR]
                            )
                        )
                        rules = rules_payload.get(PAIR, {})
                        amount = quantize_amount(
                            position_margin * config.leverage, frame.mid, rules
                        )
                        await retry_network_call(
                            lambda: client.trading.set_leverage(
                                account_name=config.account_name,
                                connector_name=config.connector_name,
                                trading_pair=PAIR,
                                leverage=config.leverage,
                            )
                        )
                        maximums = await retry_network_call(
                            lambda: _demo_max_size(client, config, PAIR)
                        )
                        amount = cap_amount_to_exchange_max(
                            amount,
                            maximums["max_buy" if direction == "LONG" else "max_sell"],
                            rules,
                        )
                        entry_price = frame.best_bid if direction == "LONG" else frame.best_ask
                        result = await retry_network_call(
                            lambda: executor_create.create_position_executor(
                                client,
                                connector_name=config.connector_name,
                                trading_pair=PAIR,
                                side=1 if direction == "LONG" else 2,
                                amount=amount,
                                entry_price=entry_price,
                                leverage=config.leverage,
                                stop_loss=barriers["stop_loss_pct"] / 100,
                                take_profit=barriers["take_profit_pct"] / 100,
                                time_limit=config.time_limit_seconds,
                                trailing_stop_activation_price=trailing["activation_pct"] / 100,
                                trailing_stop_trailing_delta=trailing["trailing_delta_pct"] / 100,
                                open_order_type=ENTRY_ORDER_TYPE,
                                take_profit_order_type=TAKE_PROFIT_ORDER_TYPE,
                                stop_loss_order_type=RISK_EXIT_ORDER_TYPE,
                                time_limit_order_type=RISK_EXIT_ORDER_TYPE,
                                level_id="eth-dom-research",
                                account_name=config.account_name,
                                controller_id=config.controller_id,
                            )
                        )
                        submitted, submission_detail = executor_submission_status(result)
                        if submitted:
                            last_trailing = trailing
                        last_entry_at = now
                        events.append(
                            {
                                "Time": time.strftime("%H:%M:%S"),
                                "Action": "ENTRY" if submitted else "ENTRY REJECTED",
                                "Side": direction,
                                "Margin": f"{position_margin:.2f} / {equity * 0.20:.2f}",
                                "Reason": f"DOM {signal['score']}/7; depth {depth.get('depth_imbalance_10bps', 0):+.2f}; SL {barriers['stop_loss_pct']:.2f}%; TP {barriers['take_profit_pct']:.2f}%; trail {trailing['activation_pct']:.2f}/{trailing['trailing_delta_pct']:.2f}% locks +{trailing['locked_profit_pct']:.2f}%; {submission_detail}",
                            }
                        )

                stats = performance_summary(rows)
                report.clear()
                report.builder.manual_order()
                report.builder.kpi("Mode", "DEMO ONLY")
                report.builder.kpi("Pair", PAIR)
                report.builder.kpi("Feed", "OKX bbo-tbt 10ms + books 100ms")
                report.builder.kpi("Signal", last_signal)
                report.builder.kpi(
                    "Position mode", "ONE SHOT · 20% MARGIN · NO SCALE-IN"
                )
                report.builder.kpi(
                    "Account ETH positions", str(len(account_positions))
                )
                report.builder.kpi(
                    "Venue active orders", str(len(account_active_orders))
                )
                report.builder.kpi(
                    "Account position control",
                    "ADOPTED FOR EXIT" if account_position_managed else "EXECUTOR/FLAT",
                )
                report.builder.kpi(
                    "Ignored empty shutdowns", str(ignored_unfilled_shutdowns)
                )
                report.builder.kpi(
                    "Margin permission",
                    f"{reserved:.2f}/{equity * config.position_margin_pct / 100:.2f} USDT (20% one-shot cap)",
                )
                report.builder.kpi("OBI", f"{frame.obi:+.3f}")
                report.builder.kpi(
                    "Depth 10 bps",
                    f"{depth.get('bid_depth_10bps', 0):.0f}/{depth.get('ask_depth_10bps', 0):.0f} USDT",
                )
                report.builder.kpi(
                    "Depth imbalance",
                    f"{depth.get('depth_imbalance_10bps', 0):+.3f}",
                )
                report.builder.kpi("Microprice", f"{signal['microprice_bps']:+.2f} bps")
                report.builder.kpi("Bid intent", f"{intent.get('bid_intent', 0):.2f}")
                report.builder.kpi("Ask intent", f"{intent.get('ask_intent', 0):.2f}")
                report.builder.kpi("Bid iceberg", f"{intent.get('bid_iceberg', 0):.2f}")
                report.builder.kpi("Ask iceberg", f"{intent.get('ask_iceberg', 0):.2f}")
                report.builder.kpi(
                    "Trailing protection",
                    (
                        f"arm +{last_trailing['activation_pct']:.2f}% / "
                        f"trail {last_trailing['trailing_delta_pct']:.2f}% / "
                        f"lock +{last_trailing['locked_profit_pct']:.2f}%"
                        if last_trailing["activation_pct"] > 0
                        else "waiting for next entry"
                    ),
                )
                report.builder.kpi("Reconnects", str(state.reconnects))
                report.builder.kpi("Closed", str(stats["closed"]))
                report.builder.kpi("Net PnL", f"{stats['net_pnl']:+.4f} USDT")
                report.builder.kpi("Fees", f"{stats['fees']:.4f} USDT")
                report.builder.kpi("Dataset", str(root))
                report.builder.table(events[-40:])
                await report.update()
                await asyncio.sleep(config.decision_interval_ms / 1000)
    except asyncio.CancelledError:
        payload = await client.executors.search_executors(
            connector_names=[config.connector_name],
            controller_ids=[config.controller_id],
            limit=500,
        )
        await _stop_active(client, _executor_rows(payload))
        return "Stopped; ETH demo executors were asked to close and research data was preserved"
    finally:
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
