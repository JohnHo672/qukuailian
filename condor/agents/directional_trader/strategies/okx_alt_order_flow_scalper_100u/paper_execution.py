"""Deterministic paper execution for the OKX alt order-flow strategy.

This module deliberately has no exchange client and cannot submit orders.  It turns
scanner signals and market ticks into a restart-safe paper ledger while enforcing the
same invariants required by the eventual live adapter: idempotency, precision,
staleness, order expiry, exposure caps and loss halts.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Any


class OrderState(str, Enum):
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class InstrumentRules:
    amount_step: float
    min_amount: float
    min_notional: float

    def __post_init__(self) -> None:
        if self.amount_step <= 0 or self.min_amount < 0 or self.min_notional < 0:
            raise ValueError("instrument rules must be non-negative with a positive step")


@dataclass(frozen=True)
class MarketTick:
    timestamp: float
    bid: float
    ask: float

    def __post_init__(self) -> None:
        if not all(math.isfinite(value) and value > 0 for value in (self.bid, self.ask)):
            raise ValueError("bid and ask must be positive finite numbers")
        if self.ask < self.bid:
            raise ValueError("ask must not be below bid")

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    def is_stale(self, now: float, max_age_seconds: float) -> bool:
        return now < self.timestamp or now - self.timestamp > max_age_seconds


@dataclass(frozen=True)
class PaperSignal:
    pair: str
    side: str
    scan_timestamp: float
    scan_price: float
    leverage: int
    margin_usdt: float
    stop_loss_pct: float
    take_profit_pct: float
    time_limit_seconds: int = 300

    def __post_init__(self) -> None:
        side = self.side.upper()
        object.__setattr__(self, "side", side)
        if side not in {"LONG", "SHORT"}:
            raise ValueError("side must be LONG or SHORT")
        values = (
            self.scan_price,
            self.margin_usdt,
            self.stop_loss_pct,
            self.take_profit_pct,
        )
        if not all(math.isfinite(value) and value > 0 for value in values):
            raise ValueError("signal prices, margin and exits must be positive")
        if self.leverage not in {5, 7, 10}:
            raise ValueError("leverage must be one of 5, 7 or 10")
        if self.time_limit_seconds <= 0:
            raise ValueError("time limit must be positive")

    @property
    def notional_usdt(self) -> float:
        return self.margin_usdt * self.leverage

    @property
    def key(self) -> str:
        material = (
            f"{self.pair}|{self.side}|{self.scan_timestamp:.6f}|"
            f"{self.scan_price:.12g}|{self.leverage}"
        )
        return hashlib.sha256(material.encode()).hexdigest()[:20]


@dataclass
class PaperOrder:
    order_id: str
    signal: PaperSignal
    amount_base: float
    limit_price: float
    created_at: float
    expires_at: float
    state: OrderState = OrderState.ENTRY_PENDING
    filled_at: float = 0.0
    entry_price: float = 0.0
    exit_price: float = 0.0
    closed_at: float = 0.0
    exit_reason: str = ""
    entry_fee_usdt: float = 0.0
    exit_fee_usdt: float = 0.0
    funding_usdt: float = 0.0
    slippage_usdt: float = 0.0
    gross_pnl_usdt: float = 0.0
    realized_pnl_usdt: float = 0.0


@dataclass(frozen=True)
class PaperRiskLimits:
    max_positions: int = 2
    max_total_margin_usdt: float = 30.0
    daily_loss_limit_usdt: float = 3.0
    consecutive_stop_limit: int = 3
    entry_timeout_seconds: int = 30
    stale_after_seconds: float = 3.0
    max_entry_drift_pct: float = 0.15
    maker_fee_pct: float = 0.02
    taker_fee_pct: float = 0.05
    simulated_slippage_pct: float = 0.02


def round_amount(notional_usdt: float, price: float, rules: InstrumentRules) -> float:
    """Round base size down to the exchange step and enforce both minima."""
    if notional_usdt <= 0 or price <= 0:
        raise ValueError("notional and price must be positive")
    raw = Decimal(str(notional_usdt)) / Decimal(str(price))
    step = Decimal(str(rules.amount_step))
    amount = (raw / step).to_integral_value(rounding=ROUND_DOWN) * step
    amount_float = float(amount)
    if amount_float + 1e-15 < rules.min_amount:
        raise ValueError("rounded amount is below the exchange minimum")
    if amount_float * price + 1e-12 < rules.min_notional:
        raise ValueError("rounded notional is below the exchange minimum")
    return amount_float


@dataclass
class PaperExecutionEngine:
    starting_equity_usdt: float = 100.0
    limits: PaperRiskLimits = field(default_factory=PaperRiskLimits)
    pending: dict[str, PaperOrder] = field(default_factory=dict)
    open_positions: dict[str, PaperOrder] = field(default_factory=dict)
    closed: list[PaperOrder] = field(default_factory=list)
    seen_signal_keys: set[str] = field(default_factory=set)
    daily_realized_pnl_usdt: float = 0.0
    consecutive_stops: int = 0
    halted_reason: str = ""
    feed_connected: bool = True

    def _active_margin(self) -> float:
        return sum(
            order.signal.margin_usdt
            for order in [*self.pending.values(), *self.open_positions.values()]
        )

    def _active_count(self) -> int:
        return len(self.pending) + len(self.open_positions)

    def disconnect_feed(self) -> None:
        self.feed_connected = False

    def reconnect_feed(self) -> None:
        self.feed_connected = True

    def submit(
        self,
        signal: PaperSignal,
        tick: MarketTick,
        rules: InstrumentRules,
        now: float,
    ) -> PaperOrder:
        if self.halted_reason:
            raise RuntimeError(f"risk halt: {self.halted_reason}")
        if not self.feed_connected:
            raise RuntimeError("market-data feed is disconnected")
        if tick.is_stale(now, self.limits.stale_after_seconds):
            raise RuntimeError("market data is stale")
        if signal.key in self.seen_signal_keys:
            raise RuntimeError("duplicate signal")
        if any(
            order.signal.pair == signal.pair
            for order in [*self.pending.values(), *self.open_positions.values()]
        ):
            raise RuntimeError("pair already has an active order or position")
        if self._active_count() >= self.limits.max_positions:
            raise RuntimeError("maximum active positions reached")
        if self._active_margin() + signal.margin_usdt > self.limits.max_total_margin_usdt + 1e-12:
            raise RuntimeError("aggregate margin cap exceeded")
        drift_pct = abs(tick.mid / signal.scan_price - 1) * 100
        if drift_pct > self.limits.max_entry_drift_pct:
            raise RuntimeError("price moved too far from the scan")

        amount = round_amount(signal.notional_usdt, tick.mid, rules)
        limit_price = tick.bid if signal.side == "LONG" else tick.ask
        order = PaperOrder(
            order_id=f"paper-{signal.key}",
            signal=signal,
            amount_base=amount,
            limit_price=limit_price,
            created_at=now,
            expires_at=now + self.limits.entry_timeout_seconds,
        )
        self.pending[order.order_id] = order
        self.seen_signal_keys.add(signal.key)
        return order

    def add_funding(self, order_id: str, funding_usdt: float) -> None:
        order = self.open_positions.get(order_id)
        if order is None:
            raise KeyError(order_id)
        if not math.isfinite(funding_usdt):
            raise ValueError("funding must be finite")
        order.funding_usdt += funding_usdt

    def on_tick(self, tick: MarketTick, now: float) -> list[str]:
        events: list[str] = []
        if not self.feed_connected or tick.is_stale(now, self.limits.stale_after_seconds):
            return events

        for order_id, order in list(self.pending.items()):
            if now >= order.expires_at:
                order.state = OrderState.CANCELLED
                order.closed_at = now
                order.exit_reason = "entry_timeout"
                self.closed.append(order)
                del self.pending[order_id]
                events.append(f"{order_id}:cancelled")
                continue
            crossed = (
                tick.ask <= order.limit_price
                if order.signal.side == "LONG"
                else tick.bid >= order.limit_price
            )
            if crossed:
                order.state = OrderState.OPEN
                order.filled_at = now
                order.entry_price = order.limit_price
                entry_notional = order.entry_price * order.amount_base
                order.entry_fee_usdt = entry_notional * self.limits.maker_fee_pct / 100
                self.open_positions[order_id] = order
                del self.pending[order_id]
                events.append(f"{order_id}:filled")

        for order_id, order in list(self.open_positions.items()):
            entry = order.entry_price
            stop = order.signal.stop_loss_pct / 100
            target = order.signal.take_profit_pct / 100
            if order.signal.side == "LONG":
                stop_hit = tick.bid <= entry * (1 - stop)
                target_hit = tick.bid >= entry * (1 + target)
            else:
                stop_hit = tick.ask >= entry * (1 + stop)
                target_hit = tick.ask <= entry * (1 - target)
            timed_out = now - order.filled_at >= order.signal.time_limit_seconds
            if stop_hit:
                self._close(order, tick, now, "stop_loss")
            elif target_hit:
                self._close(order, tick, now, "take_profit")
            elif timed_out:
                self._close(order, tick, now, "time_limit")
            else:
                continue
            del self.open_positions[order_id]
            self.closed.append(order)
            events.append(f"{order_id}:{order.exit_reason}")

        self._apply_halts()
        return events

    def _close(self, order: PaperOrder, tick: MarketTick, now: float, reason: str) -> None:
        slip = self.limits.simulated_slippage_pct / 100
        if order.signal.side == "LONG":
            raw_exit = tick.bid
            exit_price = raw_exit * (1 - slip)
            gross = (exit_price - order.entry_price) * order.amount_base
        else:
            raw_exit = tick.ask
            exit_price = raw_exit * (1 + slip)
            gross = (order.entry_price - exit_price) * order.amount_base
        order.state = OrderState.CLOSED
        order.exit_price = exit_price
        order.closed_at = now
        order.exit_reason = reason
        order.gross_pnl_usdt = gross
        order.slippage_usdt = abs(raw_exit - exit_price) * order.amount_base
        order.exit_fee_usdt = exit_price * order.amount_base * self.limits.taker_fee_pct / 100
        order.realized_pnl_usdt = (
            gross
            - order.entry_fee_usdt
            - order.exit_fee_usdt
            - order.funding_usdt
        )
        self.daily_realized_pnl_usdt += order.realized_pnl_usdt
        if reason == "stop_loss":
            self.consecutive_stops += 1
        else:
            self.consecutive_stops = 0

    def _apply_halts(self) -> None:
        if self.daily_realized_pnl_usdt <= -self.limits.daily_loss_limit_usdt:
            self.halted_reason = "daily loss limit reached"
        elif self.consecutive_stops >= self.limits.consecutive_stop_limit:
            self.halted_reason = "consecutive stop limit reached"

    def snapshot(self) -> str:
        def encode(order: PaperOrder) -> dict[str, Any]:
            data = asdict(order)
            data["state"] = order.state.value
            return data

        payload = {
            "version": 1,
            "starting_equity_usdt": self.starting_equity_usdt,
            "limits": asdict(self.limits),
            "pending": [encode(order) for order in self.pending.values()],
            "open_positions": [encode(order) for order in self.open_positions.values()],
            "closed": [encode(order) for order in self.closed],
            "seen_signal_keys": sorted(self.seen_signal_keys),
            "daily_realized_pnl_usdt": self.daily_realized_pnl_usdt,
            "consecutive_stops": self.consecutive_stops,
            "halted_reason": self.halted_reason,
            "feed_connected": self.feed_connected,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def restore(cls, snapshot: str) -> "PaperExecutionEngine":
        payload = json.loads(snapshot)
        if payload.get("version") != 1:
            raise ValueError("unsupported paper snapshot version")
        engine = cls(
            starting_equity_usdt=float(payload["starting_equity_usdt"]),
            limits=PaperRiskLimits(**payload["limits"]),
        )

        def decode(data: dict[str, Any]) -> PaperOrder:
            item = dict(data)
            item["signal"] = PaperSignal(**item["signal"])
            item["state"] = OrderState(item["state"])
            return PaperOrder(**item)

        for data in payload.get("pending", []):
            order = decode(data)
            engine.pending[order.order_id] = order
        for data in payload.get("open_positions", []):
            order = decode(data)
            engine.open_positions[order.order_id] = order
        engine.closed = [decode(data) for data in payload.get("closed", [])]
        engine.seen_signal_keys = set(payload.get("seen_signal_keys", []))
        engine.daily_realized_pnl_usdt = float(payload.get("daily_realized_pnl_usdt", 0))
        engine.consecutive_stops = int(payload.get("consecutive_stops", 0))
        engine.halted_reason = str(payload.get("halted_reason", ""))
        engine.feed_connected = bool(payload.get("feed_connected", True))
        return engine


