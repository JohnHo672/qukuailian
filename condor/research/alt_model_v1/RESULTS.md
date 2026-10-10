# Alt perpetual model v1 — result

Run: `20261010T134531Z`  
Status: `NO_GO_RESEARCH_ONLY`

The first reproducible model was trained successfully, but it did **not** show a
tradable net edge and must not be connected to simulated or live execution.

## Data and validation

- Input: 2,922,357 raw rows from completed daily files through 2026-10-09 UTC.
- Clean set: 246,331 rows across 27 crypto perpetual targets.
- Non-crypto stock, ETF and commodity perpetuals were removed.
- The active 2026-10-10 file was excluded so the input stayed immutable.
- Correlated snapshots were reduced to one row per symbol per 10 seconds.
- Cross-sectional ranks were recomputed after cleaning.
- Chronological boundaries used a 60-second purge around a 30-second label.
- Six symbols were never shown during fitting: CAP, ENA, HYPE, LIT, OKB and ZEC.
- Every decision includes 4.0 bps maker fees, 1.0 bps slippage, 1.5 bps adverse
  fill cost and a 60% fill probability assumption.

## Selected baseline model

- Estimator: histogram gradient boosting.
- Features: 36 relative order-book, trade-flow, rolling-rank and ETH-context
  features.
- Entry threshold: 10.0 bps predicted 30-second movement, calibrated without
  seeing validation or test periods.
- Only the strongest candidate in each non-overlapping 30-second bucket counts.

## Results

| Evaluation | Trades | Net/filled trade | Expected after non-fills | Win rate | Profit factor |
|---|---:|---:|---:|---:|---:|
| Chronological validation | 492 | -6.616 bps | -3.969 bps | 36.99% | 0.620 |
| Chronological test, seen symbols | 612 | -10.875 bps | -6.525 bps | 33.82% | 0.455 |
| Chronological test, unseen symbols | 770 | -7.102 bps | -4.261 bps | 15.84% | 0.227 |

The test information coefficients were near zero, and the learned model did not
beat the simple microprice baseline. The correct action is to keep collecting
data and revise labels/features rather than optimize this losing model.

## Local artifacts

The complete metrics, file SHA-256 provenance and load-tested model artifact are
stored under the ignored runtime directory:

`data/research/alt_model_v1/20261010T134531Z/`

Reproduce with:

```bash
uv run python -m research.alt_model_v1.train \
  --config research/alt_model_v1/config.yaml
```
