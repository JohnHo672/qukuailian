---
name: OKX Alt Order-Flow Scalper 100U
description: Percentage-risk scalp loop for liquid non-major OKX USDT perpetual movers, confirmed by persistent order-book imbalance, book VWAP, aggressive trades and Volume Profile.
agent_key: null
skills: []
default_config:
  frequency_sec: 30
  execution_mode: dry_run
  total_amount_quote: 100
  max_ticks: 0
  risk_limits:
    max_position_size_quote: 150
    max_drawdown_pct: 3
    max_open_executors: 2
    max_leverage: 10
default_trading_context: |
  OKX USDT perpetuals through connector `okx_perpetual`. Account sizing is
  percentage-based: 30% total margin cap, 15% per-position margin cap, 0.6% equity
  target risk per trade. Exclude major coins and never enable withdrawal permission.
---

# OKX Alt Order-Flow Scalper 100U

## Safety state

This strategy is shipped **inactive**. Never start or deploy it merely because the
file exists. Before the first live tick, require the operator's explicit live approval,
an OKX key with Read + Trade only, isolated margin, and a successful dry run. Never
trade if the routine is using preview equity instead of a live portfolio value.

## Objective

Use the 24h gainer/loser tails only as a candidate pool. Enter a short-duration
position only after deterministic order-flow confirmation. Preserve a 100 USDT test
account; activity is never a goal.

## Tick procedure

### 1. Read account and existing risk

Call `get_portfolio_overview`, `list_executors(executor_types=["position_executor"])`
and `list_positions_held()`.

- Stop the tick when equity is unavailable.
- Maximum two active positions and 30% aggregate margin.
- Do not open a second position in the same base or the same ranking direction.
- Stop trading for the day at -3% session PnL or after three consecutive stopped trades.

### 2. Run the deterministic scanner

Call:

```text
manage_routines(
  action="run",
  name="okx_alt_orderflow_scan",
  config={
    "connector_name": "okx_perpetual",
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

The routine is analysis-only. Accept `LONG` or `SHORT` only when its row says
`all gates passed`. `WATCH`, `ERROR`, preview equity, stale data or a missing field
always means HOLD.

### 3. Revalidate immediately before entry

For the selected row, call `get_market_data` for the current order book and price.
Reject the entry if:

- price moved more than 0.15% from the scan price;
- current spread exceeds the scan's limit;
- available quote balance cannot cover the proposed margin;
- trading rules make the calculated base amount invalid;
- leverage, position mode or connector credentials cannot be verified.

Use isolated margin and one-way position mode. Leverage is the scanner's 5x, 7x or
10x suggestion; never round it upward.

### 4. Percentage sizing

Use the live equity and recompute, never trust a fixed USDT amount:

```text
margin_pct = min(15%, 0.6% / [leverage × (stop_pct + 0.12%)])
margin_usdt = live_equity × margin_pct
notional_usdt = margin_usdt × leverage
amount_base = notional_usdt / current_price
```

Verify `amount_base × current_price == notional_usdt` within rounding tolerance.
Aggregate margin after the proposed entry must remain at or below 30% of live equity.

Stop and target percentages:

| Leverage | Stop | Take profit | Time limit |
|---|---:|---:|---:|
| 5x | 0.80% | 1.20% | 300s |
| 7x | 0.65% | 0.975% | 300s |
| 10x | 0.50% | 0.75% | 300s |

### 5. Executor call

Only after the live confirmation gate is satisfied, create one PositionExecutor:

```text
create_position_executor(
  connector_name="okx_perpetual",
  trading_pair=<pair>,
  side=<1 LONG, 2 SHORT>,
  amount=<BASE amount>,
  entry_price=<best bid for LONG, best ask for SHORT>,
  leverage=<5|7|10>,
  stop_loss=<0.008|0.0065|0.005>,
  take_profit=<0.012|0.00975|0.0075>,
  time_limit=300,
  trailing_stop_activation_price=<same as stop_loss>,
  trailing_stop_trailing_delta=0.0025,
  open_order_type=3,
  take_profit_order_type=1,
  stop_loss_order_type=1,
  time_limit_order_type=1,
  level_id="okx-alt-orderflow",
  controller_id=<this Agent ID>
)
```

If the post-only entry is not filled by the next tick, stop that executor and do not
chase with a market order. A failed create may be retried once only after re-reading
the current price, book, trading rules and typed tool error.

### 6. Manage and report

- Let the exchange-side stop, target and five-minute time limit manage the position.
- If the scanner reverses against an active position on two consecutive ticks, call
  `stop_executor` and record `order-flow invalidation`.
- Never average down, martingale, widen a stop, or re-enter the same base for 30 minutes.
- Maximum eight completed trades per UTC day.
- Journal the pair, ranking side, OBI, aggressive ratio, VWAP/POC/VAH/VAL, leverage,
  margin percentage, stop, target, fees and realized PnL.
- Notify only on entry, exit, risk halt, data failure or required operator action.

## Dry-run gate

Before live mode, run at least 20 dry-run signals. Require all fields to be present,
zero risk-limit violations and plausible base sizing. A dry run does not establish
profitability; it only validates wiring and decisions. Live launch still requires a
separate explicit confirmation from the operator.
