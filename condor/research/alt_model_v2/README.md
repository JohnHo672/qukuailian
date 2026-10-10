# Alt perpetual model v2 readiness

The v1 model is a strict `NO_GO`. V2 changes the data before another fit:

- only perpetuals whose base has a live OKX `BASE-USDT` crypto spot market;
- top-five queue additions and removals, top-of-book changes and depth churn;
- cancellation proxies after subtracting visible aggressive trade notional;
- maker-entry fill proxies based on opposite-side public trades reaching the
  recorded best bid or ask;
- conservative maker-entry plus taker-exit labels, with non-fills returning
  zero cash PnL instead of an impossible mark-to-mid profit;
- a continuously updated `quality.json` containing completeness, stale-book,
  invalid-row and WebSocket reconnect counters.

Do not retrain merely because another file exists. Run:

```bash
uv run python -m research.alt_model_v2.readiness
```

All gates must pass:

- at least 7 completed UTC daily files;
- at least 1,000,000 labelled rows;
- at least 20 crypto symbols and 5,000 rows per symbol;
- at least 10,000 long and 10,000 short 30-second maker-fill proxies;
- feature completeness at least 85%;
- zero invalid feature rows in the current process.

After the data gate passes, the next model must still pass purged chronological
validation and never-seen-symbol validation. Both must be positive after maker
fees, slippage, conservative exit cost and measured fill rates before a separate
simulated-paper process may be considered. No result here authorizes live trade.

## Earliest schedule

V2 collection begins on 2026-10-10, part-way through the UTC day. The partial
first day does not count toward the seven-day gate. The earliest seven complete
UTC days are 2026-10-11 through 2026-10-17, so the first eligible retrain is
**2026-10-18 after 08:00 Asia/Shanghai**. If completeness or fill counts miss a
gate, the retrain moves forward one day; it is never forced on the calendar.

Passing the data gate permits a research retrain, not simulation. A V2 model may
be recommended for a separately authorized simulated-paper process only when
all of these are true on untouched data:

- at least 300 non-overlapping chronological test decisions;
- at least 100 decisions on symbols never present during fitting;
- expected net return after measured non-fills and all costs is above +0.5 bps
  per decision on both test sets;
- profit factor is at least 1.20 on the chronological test and 1.10 on unseen
  symbols;
- block-bootstrap 95% lower confidence bound for chronological mean net return
  is above zero;
- at least 4 of the last 5 completed test days are profitable;
- no single symbol contributes more than 25% of total test PnL;
- maximum drawdown is below 50% of cumulative expected test profit;
- the model beats the best transparent OBI/microprice baseline by at least
  0.5 bps per decision;
- maker-fill proxy rate is at least 20% on each side and does not vary by more
  than 20 percentage points between validation and test;
- all data-quality gates remain green during the evaluation window.

Even after every gate passes, starting simulation requires a new explicit user
authorization. Simulation and live execution are separate decisions; no V2
research result automatically enables either one.
