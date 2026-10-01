---
name: OKX ETH DOM Liquidity Research Demo
description: Demo-only ETH order-book scalping with one 20% margin position, no scale-in, and liquidity-driven entry and exit.
version: 2
active: false
---

# OKX ETH DOM Liquidity Research — Demo

Use the `okx_eth_dom_research` continuous routine to observe ETH-USDT only.

The strategy consumes OKX public incremental depth and trades, estimates
probabilistic wall persistence, spoof risk and iceberg-style replenishment, and
requires real trade-flow confirmation before creating a demo executor. It never
places orders intended to mislead other market participants.

Execution is restricted to `okx_perpetual_demo`. A new trade uses exactly 20%
of current demo equity as margin at 10x leverage. The strategy never scales in:
an ETH position, pending or closing executor, or any non-terminal venue order
blocks every new entry.
The full-size order is allowed only when the visible 25 bps depth can support
both its entry and its exit within the participation cap. Entry attempts are
separated by at least 60 seconds so a late maker fill cannot become an
accidental scale-in.

Entries use post-only maker limits after the round-trip depth gate passes, so
the strategy does not chase a scalp with a market order. A stale entry is
cancelled after 15 seconds, but its venue order remains an execution lock until
the cancellation or any late fill is fully reflected. If the account position
appears before the executor fill fields update, the position is authoritative
and cancellation is suppressed entirely.

Entries require aligned DOM imbalance, taker flow, VWAP/POC location,
persistent intent or iceberg-style replenishment, and low spoof risk. Exits are
driven by a reached stop-liquidity pool, VWAP/POC loss, opposing absorption, or
a collapse of supporting near-book liquidity. Once gross profit covers the
observed round-trip fees plus 0.01%, the first adverse liquidity anomaly queues
an immediate marketable limit close. Take-profit orders are post-only maker
limits; stop-loss and time-limit exits are price-protected limits. No strategy
entry or exit uses a bare market order. Fee-covered trailing protection remains
a second profit lock and the bounded structural stop remains the final
backstop. The executor time limit is 120 seconds.
If a venue position outlives its executor, the routine adopts that account
position only after the executor list and active-order list are both empty.
This single-owner rule prevents delayed fill reporting from submitting two
competing closes and reversing the position. The adopted position receives
only reduce-only liquidity or risk exits; it still cannot open another
position. Residual exposure below 5 USDT is treated as exchange precision dust
after a close so it cannot permanently deadlock the strategy.

Feature rows are delayed until 1-second, 5-second and 30-second forward-return
labels can be attached. The routine writes JSONL observations and a compact
distilled summary under `data/research/eth_dom/` for later offline validation.

This is research infrastructure, not evidence of profitability and not an
authorization path for live trading.
