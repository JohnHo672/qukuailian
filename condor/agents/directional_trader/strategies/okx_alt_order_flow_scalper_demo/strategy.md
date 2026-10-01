---
name: OKX Alt Order-Flow Scalper 100U Demo
description: Twenty-tick OKX demo-perpetual validation loop for non-major movers using order-book imbalance, book VWAP, aggressive trades and Volume Profile.
agent_key: null
skills: []
default_config:
  server_name: local
  frequency_sec: 30
  tick_timeout_sec: 180
  execution_mode: loop
  total_amount_quote: 100
  max_ticks: 20
  risk_limits:
    max_position_size_quote: 150
    max_drawdown_pct: 3
    max_open_executors: 2
    max_leverage: 10
default_trading_context: |
  Authorized simulation only. Use connector `okx_perpetual_demo` exclusively and
  never substitute `okx_perpetual` or any other connector. Treat only 100 USDT of
  the demo equity as strategy capital: 30% total margin cap, 15% per-position cap,
  0.6% target account risk per trade, at most two positions. Exclude major coins.
---

# OKX Alt Order-Flow Scalper — Demo 100U

## Non-negotiable boundary

This is an OKX simulated-trading session. Every balance read, position read,
market-data request and executor action must name `okx_perpetual_demo`. Never fall
back to `okx_perpetual`, even when the demo connector errors or has no balance.
The demo account may contain more than 100 USDT, but sizing equity is exactly
`min(demo connector equity, 100 USDT)`.

## Each tick

1. Read the demo connector balance, active PositionExecutors and open demo positions.
   HOLD if any read is unavailable, the connector cannot be verified, or an unknown
   live-connector position appears in the returned data.
2. Stop new entries at two active positions, 30 USDT aggregate margin, -3 USDT
   session PnL, three consecutive stopped trades, or eight completed trades per UTC day.
3. Run `okx_alt_orderflow_scan` with this exact configuration:

```text
manage_routines(
  action="run",
  name="okx_alt_orderflow_scan",
  config={
    "connector_name": "okx_perpetual_demo",
    "equity_cap_usdt": 100,
    "equity_preview_usdt": 100,
    "top_per_side": 3,
    "min_turnover_usdt": 10000000,
    "total_margin_pct": 30,
    "per_position_margin_pct": 15,
    "risk_per_trade_pct": 0.6,
    "min_obi": 0.20,
    "min_taker_ratio": 0.58,
    "max_spread_pct": 0.12,
    "max_vwap_slippage_pct": 0.12
  }
)
```

The scanner is analysis-only. A row is eligible only when the signal is LONG or
SHORT, the reason is exactly `all gates passed`, and sizing equity says live
connector rather than preview. WATCH, ERROR, stale data or missing fields means HOLD.

## Entry and percentage sizing

Immediately re-read the selected pair's demo order book and price. Reject the entry
when price moved more than 0.15%, spread is over 0.12%, balance cannot cover margin,
trading rules make the amount invalid, or isolated margin / one-way mode / leverage
cannot be verified. Never round leverage upward.

```text
test_equity = min(demo_connector_equity, 100)
margin_pct = min(15%, 0.6% / [leverage * (stop_pct + 0.12%)])
margin_usdt = test_equity * margin_pct
notional_usdt = margin_usdt * leverage
amount_base = notional_usdt / current_price
```

Use 5x with 0.80% stop / 1.20% target, 7x with 0.65% / 0.975%, or 10x
with 0.50% / 0.75%. Time limit is 300 seconds. Submit one post-only
PositionExecutor through `okx_perpetual_demo`; a pending entry is cancelled on the
next tick and is never chased by market order. Never average down, martingale,
widen a stop, or re-enter the same base within 30 minutes.

The executor must use the scanner's base amount, 5/7/10 leverage, isolated margin,
the stop/target above, 300-second time limit, 0.25% trailing delta after activation,
and `controller_id` equal to this Agent ID. Retry a failed create at most once and
only after re-reading price, book, rules and the typed error.

## Exit and journal

Let the executor's exchange-side stop, target and time limit manage exits. Stop an
open executor if order flow reverses against it on two consecutive ticks. Record
pair, ranking side, OBI, taker ratio, book VWAP/slippage, POC/VAL/VAH, leverage,
margin percent, stop, target, fees, funding and realized PnL. Notify only on entry,
exit, risk halt, data failure or required operator action. After tick 20, stop the
loop and leave no pending entry orders.
