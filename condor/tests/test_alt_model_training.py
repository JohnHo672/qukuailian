from __future__ import annotations

import numpy as np
import pandas as pd

from research.alt_model_v1.train import (
    FEATURES,
    TrainingConfig,
    _engineer_chunk,
    policy_metrics,
    recompute_crypto_cross_section,
    split_dataset,
    stable_symbol_holdout,
)


def _row(timestamp: float, symbol: str = "ALT-USDT-SWAP", base: str = "ALT"):
    row = {column: 0.5 for column in FEATURES}
    row.update(
        {
            "schema_version": "okx-alt-cross-section-v1",
            "timestamp": timestamp,
            "inst_id": symbol,
            "base": base,
            "is_benchmark": False,
            "book_age_ms": 100.0,
            "trade_age_ms": 100.0,
            "future_return_30s_bps": 10.0,
            "depth_total_top5_usdt": 1000.0,
            "turnover_usdt_24h": 1_000_000.0,
            "trade_notional_5s": 100.0,
            "trade_notional_60s": 1000.0,
            "volume_multiple": 2.0,
            "eth_volume_multiple": 2.0,
        }
    )
    return row


def test_engineer_chunk_filters_non_crypto_benchmark_stale_and_downsamples():
    config = TrainingConfig(non_crypto_bases=["SOXL"])
    rows = [
        _row(100.0),
        _row(101.0),
        _row(120.0, "SOXL-USDT-SWAP", "SOXL"),
        {**_row(130.0, "ETH-USDT-SWAP", "ETH"), "is_benchmark": True},
        {**_row(140.0), "book_age_ms": 9000.0},
    ]
    result = _engineer_chunk(pd.DataFrame(rows), config)
    assert len(result) == 1
    assert result.iloc[0]["inst_id"] == "ALT-USDT-SWAP"
    assert "log_depth_total_top5_usdt" in result.columns


def test_symbol_holdout_is_deterministic():
    symbols = [f"C{i}-USDT-SWAP" for i in range(10)]
    assert stable_symbol_holdout(symbols, 0.2, 7) == stable_symbol_holdout(
        reversed(symbols), 0.2, 7
    )
    assert len(stable_symbol_holdout(symbols, 0.2, 7)) == 2


def test_split_is_time_ordered_embargoed_and_symbol_held_out():
    config = TrainingConfig(embargo_seconds=5, min_symbol_rows=1)
    rows = []
    for symbol in ["A-USDT-SWAP", "B-USDT-SWAP", "C-USDT-SWAP"]:
        for timestamp in range(100):
            rows.append({**_row(float(timestamp), symbol, symbol.split("-")[0])})
    frame = pd.DataFrame(rows)
    splits, metadata = split_dataset(frame, config)
    heldout = set(metadata["heldout_symbols"])
    assert not set(splits["train"]["inst_id"]) & heldout
    assert set(splits["test_unseen_symbols"]["inst_id"]) <= heldout
    assert splits["train"]["timestamp"].max() < splits["calibration"]["timestamp"].min()
    assert (
        splits["calibration"]["timestamp"].max()
        < splits["validation"]["timestamp"].min()
    )


def test_policy_applies_cost_fill_probability_and_one_trade_per_bucket():
    config = TrainingConfig(
        decision_seconds=30,
        maker_round_trip_bps=4,
        slippage_bps=1,
        adverse_fill_bps=1,
        fill_probability=0.5,
    )
    frame = pd.DataFrame(
        [
            _row(1.0, "A-USDT-SWAP", "A"),
            _row(2.0, "B-USDT-SWAP", "B"),
            _row(31.0, "A-USDT-SWAP", "A"),
        ]
    )
    frame["future_return_30s_bps"] = [10.0, 20.0, -10.0]
    metrics = policy_metrics(frame, np.array([1.0, 2.0, -2.0]), 0.0, config)
    assert metrics["trades"] == 2
    assert metrics["filled_net_mean_bps"] == 9.0
    assert metrics["expected_mean_bps"] == 4.5


def test_cross_section_is_recomputed_after_filtering():
    rows = []
    for value in [1.0, 2.0]:
        row = _row(100.0 + value)
        row["sample_bucket"] = 10
        for source in [
            "spread_bps",
            "depth_total_top5_usdt",
            "obi_top5",
            "volume_multiple",
            "trade_notional_5s",
            "microprice_bps",
            "up_liquidity_gap_bps",
            "down_liquidity_gap_bps",
        ]:
            row[source] = value
        rows.append(row)
    result = recompute_crypto_cross_section(pd.DataFrame(rows))
    assert result["spread_bps_cross_section_pct"].tolist() == [0.25, 0.75]
