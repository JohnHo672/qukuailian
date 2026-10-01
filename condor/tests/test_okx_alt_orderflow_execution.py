"""Safety and paper-execution tests for the OKX order-flow strategy."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "agents"
    / "directional_trader"
    / "strategies"
    / "okx_alt_order_flow_scalper_100u"
    / "paper_execution.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("okx_alt_execution_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


execution = _load()


def rules():
    return execution.InstrumentRules(amount_step=0.001, min_amount=0.001, min_notional=5)


def signal(index=1, side="LONG", margin=10, stop=0.8, target=1.2):
    return execution.PaperSignal(
        pair=f"ALT{index}-USDT",
        side=side,
        scan_timestamp=float(index),
        scan_price=100,
        leverage=5,
        margin_usdt=margin,
        stop_loss_pct=stop,
        take_profit_pct=target,
        time_limit_seconds=300,
    )


def tick(ts, bid=99.99, ask=100.01):
    return execution.MarketTick(timestamp=ts, bid=bid, ask=ask)


def fill(engine, order, now):
    if order.signal.side == "LONG":
        market = tick(now, bid=99.98, ask=order.limit_price)
    else:
        market = tick(now, bid=order.limit_price, ask=100.02)
    engine.on_tick(market, now)
    assert order.state == execution.OrderState.OPEN


def test_amount_rounds_down_and_enforces_exchange_minima():
    assert execution.round_amount(50, 99, rules()) == pytest.approx(0.505)
    with pytest.raises(ValueError, match="minimum"):
        execution.round_amount(1, 100, rules())


def test_duplicate_signal_and_pair_are_rejected():
    engine = execution.PaperExecutionEngine()
    first = signal()
    engine.submit(first, tick(1), rules(), 1)
    with pytest.raises(RuntimeError, match="duplicate"):
        engine.submit(first, tick(1), rules(), 1)
    with pytest.raises(RuntimeError, match="pair already"):
        engine.submit(
            execution.PaperSignal(**{**first.__dict__, "scan_timestamp": 2}),
            tick(2),
            rules(),
            2,
        )


def test_stale_or_disconnected_market_data_blocks_entries():
    engine = execution.PaperExecutionEngine()
    with pytest.raises(RuntimeError, match="stale"):
        engine.submit(signal(), tick(1), rules(), 10)
    engine.disconnect_feed()
    with pytest.raises(RuntimeError, match="disconnected"):
        engine.submit(signal(2), tick(2), rules(), 2)
    engine.reconnect_feed()
    assert engine.submit(signal(2), tick(2), rules(), 2).state == execution.OrderState.ENTRY_PENDING


def test_unfilled_limit_order_times_out_without_chasing():
    engine = execution.PaperExecutionEngine()
    order = engine.submit(signal(), tick(1), rules(), 1)
    events = engine.on_tick(tick(32, bid=100.20, ask=100.21), 32)
    assert order.state == execution.OrderState.CANCELLED
    assert order.exit_reason == "entry_timeout"
    assert events == [f"{order.order_id}:cancelled"]
    assert not engine.pending and not engine.open_positions


def test_restart_snapshot_recovers_pending_and_open_positions():
    engine = execution.PaperExecutionEngine()
    open_order = engine.submit(signal(1), tick(1), rules(), 1)
    fill(engine, open_order, 2)
    pending_order = engine.submit(signal(2, side="SHORT"), tick(2), rules(), 2)
    restored = execution.PaperExecutionEngine.restore(engine.snapshot())
    assert set(restored.open_positions) == {open_order.order_id}
    assert set(restored.pending) == {pending_order.order_id}
    assert restored.seen_signal_keys == engine.seen_signal_keys


def test_fees_funding_and_slippage_are_in_realized_pnl():
    engine = execution.PaperExecutionEngine()
    order = engine.submit(signal(), tick(1), rules(), 1)
    fill(engine, order, 2)
    engine.add_funding(order.order_id, 0.03)
    engine.on_tick(tick(3, bid=101.30, ask=101.31), 3)
    assert order.state == execution.OrderState.CLOSED
    assert order.exit_reason == "take_profit"
    assert order.entry_fee_usdt > 0
    assert order.exit_fee_usdt > 0
    assert order.slippage_usdt > 0
    assert order.realized_pnl_usdt == pytest.approx(
        order.gross_pnl_usdt - order.entry_fee_usdt - order.exit_fee_usdt - 0.03
    )


def test_three_consecutive_stops_trigger_risk_halt():
    engine = execution.PaperExecutionEngine()
    for index in range(1, 4):
        order = engine.submit(signal(index), tick(index * 10), rules(), index * 10)
        fill(engine, order, index * 10 + 1)
        engine.on_tick(
            tick(index * 10 + 2, bid=98.90, ask=98.91), index * 10 + 2
        )
        assert order.exit_reason == "stop_loss"
    assert engine.halted_reason == "consecutive stop limit reached"
    with pytest.raises(RuntimeError, match="risk halt"):
        engine.submit(signal(4), tick(40), rules(), 40)


def test_daily_loss_limit_triggers_even_without_three_stops():
    limits = execution.PaperRiskLimits(daily_loss_limit_usdt=0.1, consecutive_stop_limit=10)
    engine = execution.PaperExecutionEngine(limits=limits)
    order = engine.submit(signal(), tick(1), rules(), 1)
    fill(engine, order, 2)
    engine.on_tick(tick(3, bid=98.90, ask=98.91), 3)
    assert engine.halted_reason == "daily loss limit reached"


def test_margin_and_position_caps_are_enforced():
    engine = execution.PaperExecutionEngine()
    engine.submit(signal(1, margin=15), tick(1), rules(), 1)
    engine.submit(signal(2, side="SHORT", margin=15), tick(2), rules(), 2)
    with pytest.raises(RuntimeError, match="maximum active"):
        engine.submit(signal(3, margin=1), tick(3), rules(), 3)


def test_twenty_round_paper_run_is_deterministic_and_flat_at_end():
    engine = execution.PaperExecutionEngine()
    for index in range(1, 21):
        now = index * 10.0
        side = "LONG" if index % 2 else "SHORT"
        order = engine.submit(signal(index, side=side), tick(now), rules(), now)
        fill(engine, order, now + 1)
        if side == "LONG":
            close_tick = tick(now + 2, bid=101.30, ask=101.31)
        else:
            close_tick = tick(now + 2, bid=98.69, ask=98.70)
        engine.on_tick(close_tick, now + 2)
        assert order.state == execution.OrderState.CLOSED
    assert len(engine.closed) == 20
    assert not engine.pending and not engine.open_positions
    assert engine.halted_reason == ""
    restored = execution.PaperExecutionEngine.restore(engine.snapshot())
    assert len(restored.closed) == 20
    assert restored.daily_realized_pnl_usdt == pytest.approx(engine.daily_realized_pnl_usdt)

