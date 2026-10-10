# Alt perpetual model v1

This is an offline, research-only baseline trained on the read-only OKX
cross-sectional collector output. It cannot place orders.

The pipeline:

1. removes documented stock, ETF and commodity perpetuals;
2. excludes the active daily file so the input cannot change during fitting;
3. recomputes cross-sectional ranks using only the retained crypto universe;
4. excludes ETH as a target while retaining the embedded ETH context features;
5. downsamples correlated snapshots to one observation per symbol per 10 seconds;
6. purges 60 seconds around chronological split boundaries;
7. reserves deterministic symbols that are never shown during fitting;
8. compares a dummy model, gradient boosting and simple
   order-book baselines;
9. evaluates one non-overlapping decision every 30 seconds, including maker
   fees, slippage, adverse-fill cost and a conservative fill probability;
10. saves the fitted research artifact, JSON metrics and a Markdown report.

Run from the Condor directory:

```bash
uv run python -m research.alt_model_v1.train \
  --config research/alt_model_v1/config.yaml
```

Generated model files remain under the ignored `data/` directory. Source,
configuration and tests are version-controlled. A positive backtest is not
authorization to deploy or trade; the report explicitly decides only whether
the model merits a separate simulated-paper phase.
