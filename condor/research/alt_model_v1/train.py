"""Reproducible, research-only baseline for OKX alt perpetual snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
import scipy
import sklearn
import yaml
from sklearn.base import clone
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline

CROSS_SECTION_SOURCES = [
    "spread_bps",
    "depth_total_top5_usdt",
    "obi_top5",
    "volume_multiple",
    "trade_notional_5s",
    "microprice_bps",
    "up_liquidity_gap_bps",
    "down_liquidity_gap_bps",
]

RAW_FEATURES = [
    "spread_bps",
    "microprice_bps",
    "obi_top5",
    "bid_depth_share",
    "up_liquidity_gap_bps",
    "down_liquidity_gap_bps",
    "change_pct_24h",
    "tick_size_bps",
    "buy_ratio",
    "trade_return_5s_bps",
    "eth_obi_top5",
    "eth_trade_return_5s_bps",
    "spread_bps_rolling_pct",
    "depth_total_top5_usdt_rolling_pct",
    "obi_top5_rolling_pct",
    "volume_multiple_rolling_pct",
    "trade_notional_5s_rolling_pct",
    "microprice_bps_rolling_pct",
    "up_liquidity_gap_bps_rolling_pct",
    "down_liquidity_gap_bps_rolling_pct",
]

LOG_FEATURES = {
    "depth_total_top5_usdt": "log_depth_total_top5_usdt",
    "turnover_usdt_24h": "log_turnover_usdt_24h",
    "trade_notional_5s": "log_trade_notional_5s",
    "trade_notional_60s": "log_trade_notional_60s",
    "volume_multiple": "log_volume_multiple",
    "eth_volume_multiple": "log_eth_volume_multiple",
    "book_age_ms": "log_book_age_ms",
    "trade_age_ms": "log_trade_age_ms",
}

CROSS_SECTION_FEATURES = [f"{key}_cross_section_pct" for key in CROSS_SECTION_SOURCES]
FEATURES = RAW_FEATURES + CROSS_SECTION_FEATURES + list(LOG_FEATURES.values())
META_COLUMNS = [
    "schema_version",
    "timestamp",
    "inst_id",
    "base",
    "is_benchmark",
    "book_age_ms",
    "trade_age_ms",
]


@dataclass
class TrainingConfig:
    data_glob: str = "data/research/alt_cross_section/features-*.jsonl.gz"
    output_dir: str = "data/research/alt_model_v1"
    schema_version: str = "okx-alt-cross-section-v1"
    exclude_latest_file: bool = True
    target_column: str = "future_return_30s_bps"
    sample_seconds: int = 10
    decision_seconds: int = 30
    embargo_seconds: int = 60
    chunk_rows: int = 100_000
    max_train_rows: int = 300_000
    min_symbol_rows: int = 500
    holdout_symbol_fraction: float = 0.2
    random_state: int = 672
    max_book_age_ms: float = 2_000.0
    max_trade_age_ms: float = 5_000.0
    maker_round_trip_bps: float = 4.0
    slippage_bps: float = 1.0
    adverse_fill_bps: float = 1.5
    fill_probability: float = 0.6
    min_calibration_trades: int = 50
    non_crypto_bases: list[str] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> "TrainingConfig":
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls(**payload)


def stable_symbol_holdout(
    symbols: Iterable[str], fraction: float, seed: int
) -> list[str]:
    ordered = sorted(
        set(symbols),
        key=lambda value: hashlib.sha256(f"{seed}:{value}".encode()).hexdigest(),
    )
    count = max(1, math.ceil(len(ordered) * fraction))
    return sorted(ordered[:count])


def _required_columns(config: TrainingConfig) -> list[str]:
    return sorted(
        set(
            META_COLUMNS
            + RAW_FEATURES
            + CROSS_SECTION_SOURCES
            + list(LOG_FEATURES)
            + [config.target_column]
        )
    )


def _engineer_chunk(chunk: pd.DataFrame, config: TrainingConfig) -> pd.DataFrame:
    starting_rows = len(chunk)
    if not starting_rows:
        return chunk
    chunk = chunk.loc[chunk["schema_version"] == config.schema_version].copy()
    chunk["base"] = chunk["base"].astype(str).str.upper()
    chunk = chunk.loc[~chunk["base"].isin(set(config.non_crypto_bases))]
    chunk = chunk.loc[~chunk["is_benchmark"].astype(bool)]
    for column in ["timestamp", "book_age_ms", "trade_age_ms", config.target_column]:
        chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
    chunk = chunk.loc[
        chunk["timestamp"].notna()
        & chunk[config.target_column].notna()
        & np.isfinite(chunk[config.target_column])
        & chunk["book_age_ms"].between(0, config.max_book_age_ms)
        & chunk["trade_age_ms"].between(0, config.max_trade_age_ms)
    ]
    chunk["sample_bucket"] = (chunk["timestamp"] // config.sample_seconds).astype(
        "int64"
    )
    chunk = chunk.sort_values("timestamp").drop_duplicates(
        ["inst_id", "sample_bucket"], keep="last"
    )
    for column in RAW_FEATURES + list(LOG_FEATURES):
        chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
    for source, target in LOG_FEATURES.items():
        values = chunk[source].clip(lower=0)
        if source == "volume_multiple" or source == "eth_volume_multiple":
            values = values.clip(upper=100)
        chunk[target] = np.log1p(values)
    temporary = list(
        dict.fromkeys(
            RAW_FEATURES + CROSS_SECTION_SOURCES + list(LOG_FEATURES.values())
        )
    )
    chunk[temporary] = chunk[temporary].replace([np.inf, -np.inf], np.nan)
    return chunk[
        ["timestamp", "inst_id", "base", "sample_bucket", config.target_column]
        + temporary
    ]


def recompute_crypto_cross_section(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    grouped = frame.groupby("sample_bucket", sort=False)
    for source in CROSS_SECTION_SOURCES:
        rank = grouped[source].rank(method="average")
        count = grouped[source].transform("count")
        frame[f"{source}_cross_section_pct"] = (rank - 0.5) / count
    return frame


def load_dataset(
    root: Path, config: TrainingConfig
) -> tuple[pd.DataFrame, dict[str, Any]]:
    files = sorted(root.glob(config.data_glob))
    if not files:
        raise FileNotFoundError(f"No input files matched {config.data_glob!r}")
    excluded_active_file = None
    if config.exclude_latest_file:
        if len(files) < 2:
            raise ValueError(
                "At least two daily files are required when excluding the active file"
            )
        excluded_active_file = str(files[-1])
        files = files[:-1]
    parts: list[pd.DataFrame] = []
    input_rows = 0
    excluded_non_crypto: dict[str, int] = {}
    excluded_benchmark_rows = 0
    per_file: list[dict[str, Any]] = []
    required = _required_columns(config)
    for path in files:
        file_rows = 0
        retained_rows = 0
        digest = hashlib.sha256()
        with path.open("rb") as raw:
            for block in iter(lambda: raw.read(1024 * 1024), b""):
                digest.update(block)
        reader = pd.read_json(
            path,
            lines=True,
            compression="gzip",
            chunksize=config.chunk_rows,
            convert_dates=False,
        )
        for chunk in reader:
            missing = set(required) - set(chunk.columns)
            if missing:
                raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
            input_rows += len(chunk)
            file_rows += len(chunk)
            bases = chunk["base"].astype(str).str.upper()
            rejected = bases[bases.isin(set(config.non_crypto_bases))].value_counts()
            for base, count in rejected.items():
                excluded_non_crypto[str(base)] = excluded_non_crypto.get(
                    str(base), 0
                ) + int(count)
            excluded_benchmark_rows += int(chunk["is_benchmark"].astype(bool).sum())
            cleaned = _engineer_chunk(chunk[required], config)
            retained_rows += len(cleaned)
            if not cleaned.empty:
                parts.append(cleaned)
        per_file.append(
            {
                "path": str(path),
                "rows": file_rows,
                "retained_after_chunk_cleaning": retained_rows,
                "bytes": path.stat().st_size,
                "sha256": digest.hexdigest(),
            }
        )
    if not parts:
        raise ValueError("All rows were removed during cleaning")
    frame = pd.concat(parts, ignore_index=True)
    before_cross_chunk_dedupe = len(frame)
    frame = (
        frame.sort_values("timestamp")
        .drop_duplicates(["inst_id", "sample_bucket"], keep="last")
        .reset_index(drop=True)
    )
    duplicates_removed = before_cross_chunk_dedupe - len(frame)
    symbol_counts = frame["inst_id"].value_counts()
    eligible = symbol_counts[symbol_counts >= config.min_symbol_rows].index
    low_coverage_removed = int((~frame["inst_id"].isin(eligible)).sum())
    frame = frame.loc[frame["inst_id"].isin(eligible)].reset_index(drop=True)
    frame = recompute_crypto_cross_section(frame)
    frame = frame[
        ["timestamp", "inst_id", "base", "sample_bucket", config.target_column]
        + FEATURES
    ]
    missing_rates = {column: float(frame[column].isna().mean()) for column in FEATURES}
    audit = {
        "input_rows": int(input_rows),
        "retained_rows": int(len(frame)),
        "removed_rows": int(input_rows - len(frame)),
        "cross_chunk_duplicates_removed": int(duplicates_removed),
        "low_coverage_rows_removed": low_coverage_removed,
        "excluded_non_crypto_rows_by_base": excluded_non_crypto,
        "excluded_benchmark_rows": excluded_benchmark_rows,
        "excluded_active_file": excluded_active_file,
        "symbols": sorted(frame["inst_id"].unique().tolist()),
        "symbol_counts": {
            str(key): int(value)
            for key, value in frame["inst_id"].value_counts().items()
        },
        "missing_rates": missing_rates,
        "first_timestamp": float(frame["timestamp"].min()),
        "last_timestamp": float(frame["timestamp"].max()),
        "input_files": per_file,
    }
    return frame, audit


def split_dataset(
    frame: pd.DataFrame, config: TrainingConfig
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    symbols = sorted(frame["inst_id"].unique())
    holdout = stable_symbol_holdout(
        symbols, config.holdout_symbol_fraction, config.random_state
    )
    timestamps = frame["timestamp"].to_numpy()
    cut_train, cut_cal, cut_validation = np.quantile(timestamps, [0.60, 0.75, 0.87])
    embargo = config.embargo_seconds
    seen = ~frame["inst_id"].isin(holdout)
    splits = {
        "train": frame.loc[seen & (frame["timestamp"] <= cut_train - embargo)],
        "calibration": frame.loc[
            seen
            & (frame["timestamp"] >= cut_train + embargo)
            & (frame["timestamp"] <= cut_cal - embargo)
        ],
        "validation": frame.loc[
            seen
            & (frame["timestamp"] >= cut_cal + embargo)
            & (frame["timestamp"] <= cut_validation - embargo)
        ],
        "test_seen": frame.loc[seen & (frame["timestamp"] >= cut_validation + embargo)],
        "test_unseen_symbols": frame.loc[
            (~seen) & (frame["timestamp"] >= cut_validation + embargo)
        ],
    }
    for key, value in list(splits.items()):
        splits[key] = value.sort_values("timestamp").reset_index(drop=True)
        if splits[key].empty:
            raise ValueError(f"Split {key!r} is empty")
    metadata = {
        "heldout_symbols": holdout,
        "cutoffs": {
            "train_end": float(cut_train),
            "calibration_end": float(cut_cal),
            "validation_end": float(cut_validation),
            "embargo_seconds": embargo,
        },
        "rows": {key: int(len(value)) for key, value in splits.items()},
        "time_ranges": {
            key: [float(value["timestamp"].min()), float(value["timestamp"].max())]
            for key, value in splits.items()
        },
    }
    return splits, metadata


def cap_training_rows(
    frame: pd.DataFrame, maximum: int, random_state: int
) -> pd.DataFrame:
    if len(frame) <= maximum:
        return frame
    sampled = frame.sample(maximum, random_state=random_state)
    return sampled.sort_values("timestamp").reset_index(drop=True)


def model_candidates(config: TrainingConfig) -> dict[str, Any]:
    return {
        "dummy_median": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("model", DummyRegressor(strategy="median")),
            ]
        ),
        "hist_gradient_boosting": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        learning_rate=0.05,
                        max_iter=180,
                        max_leaf_nodes=15,
                        min_samples_leaf=100,
                        l2_regularization=5.0,
                        random_state=config.random_state,
                    ),
                ),
            ]
        ),
    }


def regression_metrics(y_true: np.ndarray, prediction: np.ndarray) -> dict[str, float]:
    finite = np.isfinite(y_true) & np.isfinite(prediction)
    y_true = y_true[finite]
    prediction = prediction[finite]
    variable = (
        len(y_true) > 2 and float(np.ptp(y_true)) > 0 and float(np.ptp(prediction)) > 0
    )

    def safe_correlation(left: np.ndarray, right: np.ndarray) -> float:
        left_z = (left - np.mean(left)) / np.std(left)
        right_z = (right - np.mean(right)) / np.std(right)
        return float(np.mean(left_z * right_z))

    pearson = safe_correlation(y_true, prediction) if variable else 0.0
    if variable:
        left_rank = pd.Series(y_true).rank(method="average").to_numpy()
        right_rank = pd.Series(prediction).rank(method="average").to_numpy()
        spearman = safe_correlation(left_rank, right_rank)
    else:
        spearman = 0.0
    return {
        "mae_bps": float(mean_absolute_error(y_true, prediction)),
        "rmse_bps": float(math.sqrt(mean_squared_error(y_true, prediction))),
        "pearson_ic": float(pearson) if math.isfinite(pearson) else 0.0,
        "spearman_ic": float(spearman) if math.isfinite(spearman) else 0.0,
    }


def policy_metrics(
    frame: pd.DataFrame,
    prediction: np.ndarray,
    threshold: float,
    config: TrainingConfig,
) -> dict[str, float | int]:
    if len(frame) != len(prediction):
        raise ValueError("Prediction length does not match evaluation frame")
    policy = frame[["timestamp", "inst_id", config.target_column]].copy()
    policy["prediction"] = prediction
    policy = policy.loc[np.isfinite(policy["prediction"])]
    policy["score"] = np.abs(prediction)
    policy = policy.loc[policy["score"] >= threshold]
    if policy.empty:
        return {"trades": 0, "threshold_bps": float(threshold)}
    policy["decision_bucket"] = (policy["timestamp"] // config.decision_seconds).astype(
        "int64"
    )
    policy = (
        policy.sort_values(["decision_bucket", "score"], ascending=[True, False])
        .drop_duplicates("decision_bucket", keep="first")
        .sort_values("timestamp")
    )
    side = np.sign(policy["prediction"].to_numpy())
    gross = side * policy[config.target_column].to_numpy()
    cost = config.maker_round_trip_bps + config.slippage_bps + config.adverse_fill_bps
    filled_net = gross - cost
    expected = config.fill_probability * filled_net
    curve = np.cumsum(expected)
    running_max = np.maximum.accumulate(np.r_[0.0, curve])
    drawdown = running_max[1:] - curve
    wins = filled_net[filled_net > 0]
    losses = filled_net[filled_net < 0]
    std = float(np.std(expected, ddof=1)) if len(expected) > 1 else 0.0
    stderr = std / math.sqrt(len(expected)) if std > 1e-12 else float("inf")
    return {
        "trades": int(len(policy)),
        "threshold_bps": float(threshold),
        "gross_mean_bps": float(np.mean(gross)),
        "filled_net_mean_bps": float(np.mean(filled_net)),
        "expected_mean_bps": float(np.mean(expected)),
        "expected_total_bps": float(np.sum(expected)),
        "win_rate_after_cost": float(np.mean(filled_net > 0)),
        "profit_factor_after_cost": float(
            wins.sum() / abs(losses.sum()) if len(losses) and losses.sum() else math.inf
        ),
        "max_drawdown_bps": float(np.max(drawdown)) if len(drawdown) else 0.0,
        "mean_t_stat_naive": (
            float(np.mean(expected) / stderr)
            if math.isfinite(stderr) and stderr > 0
            else 0.0
        ),
        "assumed_fill_probability": float(config.fill_probability),
        "all_in_cost_if_filled_bps": float(cost),
    }


def choose_threshold(
    frame: pd.DataFrame, prediction: np.ndarray, config: TrainingConfig
) -> tuple[float, list[dict[str, float | int]]]:
    absolute = np.abs(prediction[np.isfinite(prediction)])
    if not len(absolute):
        return 0.0, []
    candidates = {
        0.0,
        0.5,
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
        6.5,
        8.0,
        10.0,
        15.0,
        *[
            float(np.quantile(absolute, q))
            for q in (0.50, 0.70, 0.80, 0.90, 0.95, 0.98)
        ],
    }
    rows = [
        policy_metrics(frame, prediction, value, config) for value in sorted(candidates)
    ]
    viable = [
        row
        for row in rows
        if int(row.get("trades", 0)) >= config.min_calibration_trades
    ]
    if not viable:
        viable = rows

    def score(row: dict[str, float | int]) -> float:
        trades = max(int(row.get("trades", 0)), 1)
        mean = float(row.get("expected_mean_bps", -math.inf))
        t_stat = float(row.get("mean_t_stat_naive", 0.0))
        return mean + min(t_stat, 0.0) / math.sqrt(trades)

    winner = max(viable, key=score)
    return float(winner["threshold_bps"]), rows


def baseline_predictions(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    return {
        "obi_rule": frame["obi_top5"].fillna(0).to_numpy() * 10.0,
        "microprice_rule": frame["microprice_bps"].fillna(0).to_numpy(),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return value


def _format_time(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()


def render_report(result: dict[str, Any]) -> str:
    chosen = result["selection"]["chosen_model"]
    threshold = result["selection"]["threshold_bps"]
    seen = result["models"][chosen]["test_seen"]
    unseen = result["models"][chosen]["test_unseen_symbols"]
    decision = result["decision"]
    audit = result["data_audit"]
    split = result["split"]
    lines = [
        "# Alt perpetual model v1 — training report",
        "",
        f"**Decision:** `{decision['status']}`",
        "",
        decision["reason"],
        "",
        "## Data audit",
        "",
        f"- Raw rows: {audit['input_rows']:,}",
        f"- Clean/downsampled rows: {audit['retained_rows']:,}",
        f"- Crypto target symbols: {len(audit['symbols'])}",
        f"- Period (UTC): {_format_time(audit['first_timestamp'])} to {_format_time(audit['last_timestamp'])}",
        f"- Explicitly excluded non-crypto bases: {', '.join(result['config']['non_crypto_bases'])}",
        f"- Active file excluded for reproducibility: {audit['excluded_active_file']}",
        f"- Held-out symbols never used for fitting: {', '.join(split['heldout_symbols'])}",
        "- ETH rows are excluded as targets; ETH order-flow context remains in every alt feature row.",
        "- Cross-sectional ranks are recomputed after non-crypto rows are removed.",
        "- Snapshots are reduced to one row per symbol per 10 seconds to limit serial duplication.",
        "- Split boundaries are purged by 60 seconds to prevent 30-second label leakage.",
        "",
        "## Selected model",
        "",
        f"- Model: `{chosen}`",
        f"- Entry threshold selected only on calibration data: {threshold:.4f} bps predicted move",
        f"- Cost if filled: {seen.get('all_in_cost_if_filled_bps', 0):.2f} bps",
        f"- Assumed maker fill probability: {seen.get('assumed_fill_probability', 0):.0%}",
        "- At most one decision is counted in each non-overlapping 30-second bucket.",
        "",
        "## Untouched chronological test — seen symbols",
        "",
        f"- Trades: {seen.get('trades', 0):,}",
        f"- Net mean if filled: {seen.get('filled_net_mean_bps', 0):.4f} bps",
        f"- Expected mean after non-fills: {seen.get('expected_mean_bps', 0):.4f} bps",
        f"- Win rate after costs: {seen.get('win_rate_after_cost', 0):.2%}",
        f"- Profit factor: {seen.get('profit_factor_after_cost', 0):.3f}",
        f"- Max drawdown: {seen.get('max_drawdown_bps', 0):.2f} bps",
        "",
        "## Unseen-symbol test",
        "",
        f"- Trades: {unseen.get('trades', 0):,}",
        f"- Net mean if filled: {unseen.get('filled_net_mean_bps', 0):.4f} bps",
        f"- Expected mean after non-fills: {unseen.get('expected_mean_bps', 0):.4f} bps",
        f"- Win rate after costs: {unseen.get('win_rate_after_cost', 0):.2%}",
        f"- Profit factor: {unseen.get('profit_factor_after_cost', 0):.3f}",
        "",
        "## Interpretation",
        "",
        "The result is a research backtest, not proof of executable profit. Public books5 data does not reveal queue position, so fill probability and adverse-selection costs are explicit stress assumptions rather than measured fills. No live or simulated order was placed by this pipeline.",
        "",
        "## Reproduction",
        "",
        "```bash",
        "uv run python -m research.alt_model_v1.train --config research/alt_model_v1/config.yaml",
        "```",
    ]
    return "\n".join(lines) + "\n"


def run_training(root: Path, config: TrainingConfig) -> dict[str, Any]:
    frame, audit = load_dataset(root, config)
    splits, split_meta = split_dataset(frame, config)
    train = cap_training_rows(
        splits["train"], config.max_train_rows, config.random_state
    )
    x_train = train[FEATURES]
    y_train = train[config.target_column].to_numpy()
    target_bounds = np.quantile(y_train, [0.001, 0.999])
    y_train_fit = np.clip(y_train, target_bounds[0], target_bounds[1])
    models = model_candidates(config)
    evaluations: dict[str, Any] = {}
    fitted: dict[str, Any] = {}
    for name, model in models.items():
        model.fit(x_train, y_train_fit)
        fitted[name] = model
        calibration_prediction = model.predict(splits["calibration"][FEATURES])
        threshold, threshold_grid = choose_threshold(
            splits["calibration"], calibration_prediction, config
        )
        evaluations[name] = {
            "threshold_bps": threshold,
            "calibration_regression": regression_metrics(
                splits["calibration"][config.target_column].to_numpy(),
                calibration_prediction,
            ),
            "calibration_threshold_grid": threshold_grid,
        }
        for split_name in ("validation", "test_seen", "test_unseen_symbols"):
            prediction = model.predict(splits[split_name][FEATURES])
            evaluations[name][f"{split_name}_regression"] = regression_metrics(
                splits[split_name][config.target_column].to_numpy(), prediction
            )
            evaluations[name][split_name] = policy_metrics(
                splits[split_name], prediction, threshold, config
            )
    baselines: dict[str, Any] = {}
    calibration_baselines = baseline_predictions(splits["calibration"])
    for name, prediction in calibration_baselines.items():
        threshold, grid = choose_threshold(splits["calibration"], prediction, config)
        baselines[name] = {
            "threshold_bps": threshold,
            "calibration_threshold_grid": grid,
        }
        for split_name in ("validation", "test_seen", "test_unseen_symbols"):
            split_prediction = baseline_predictions(splits[split_name])[name]
            baselines[name][split_name] = policy_metrics(
                splits[split_name], split_prediction, threshold, config
            )
    eligible_models = [name for name in models if name != "dummy_median"]
    chosen = max(
        eligible_models,
        key=lambda name: float(
            evaluations[name]["validation"].get("expected_mean_bps", -math.inf)
        ),
    )
    threshold = float(evaluations[chosen]["threshold_bps"])
    seen_test = evaluations[chosen]["test_seen"]
    unseen_test = evaluations[chosen]["test_unseen_symbols"]
    best_baseline_seen = max(
        float(value["test_seen"].get("expected_mean_bps", -math.inf))
        for value in baselines.values()
    )
    go = (
        int(seen_test.get("trades", 0)) >= 100
        and int(unseen_test.get("trades", 0)) >= 30
        and float(seen_test.get("expected_mean_bps", -math.inf)) > 0
        and float(unseen_test.get("expected_mean_bps", -math.inf)) > 0
        and float(seen_test.get("expected_mean_bps", -math.inf)) > best_baseline_seen
    )
    decision = {
        "status": "GO_TO_SEPARATE_SIMULATION" if go else "NO_GO_RESEARCH_ONLY",
        "reason": (
            "The model passed both chronological and unseen-symbol net-cost gates and beat the simple order-book baselines. It may proceed only to an isolated simulated-paper phase."
            if go
            else "The model did not pass every chronological, unseen-symbol, cost and baseline gate. Keep collecting data and do not deploy it to simulated execution yet."
        ),
    }
    pretest = pd.concat(
        [splits["train"], splits["calibration"], splits["validation"]],
        ignore_index=True,
    )
    pretest = cap_training_rows(pretest, config.max_train_rows, config.random_state)
    final_y = pretest[config.target_column].to_numpy()
    final_target_bounds = np.quantile(final_y, [0.001, 0.999])
    final_model = clone(models[chosen]).fit(
        pretest[FEATURES],
        np.clip(final_y, final_target_bounds[0], final_target_bounds[1]),
    )
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = root / config.output_dir / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    artifact = {
        "model": final_model,
        "features": FEATURES,
        "target_column": config.target_column,
        "threshold_bps": threshold,
        "trained_until": split_meta["cutoffs"]["validation_end"],
        "heldout_symbols": split_meta["heldout_symbols"],
        "target_clip_bounds_bps": final_target_bounds.tolist(),
        "config": asdict(config),
        "research_only": True,
    }
    joblib.dump(artifact, output_dir / "model.joblib")
    result = {
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "config": asdict(config),
        "features": FEATURES,
        "data_audit": audit,
        "split": split_meta,
        "fit_rows": int(len(train)),
        "training_target_clip_bounds_bps": target_bounds.tolist(),
        "models": evaluations,
        "baselines": baselines,
        "selection": {"chosen_model": chosen, "threshold_bps": threshold},
        "decision": decision,
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scipy": scipy.__version__,
            "scikit_learn": sklearn.__version__,
        },
        "artifacts": {
            "model": str(output_dir / "model.joblib"),
            "metrics": str(output_dir / "metrics.json"),
            "report": str(output_dir / "report.md"),
        },
    }
    (output_dir / "metrics.json").write_text(
        json.dumps(_json_safe(result), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "report.md").write_text(render_report(result), encoding="utf-8")
    latest = root / config.output_dir / "latest.json"
    latest.write_text(
        json.dumps({"run_id": run_id, "output_dir": str(output_dir)}, indent=2),
        encoding="utf-8",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("research/alt_model_v1/config.yaml")
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    config = TrainingConfig.from_yaml(args.config)
    result = run_training(args.root.resolve(), config)
    print(
        json.dumps(
            {
                "run_id": result["run_id"],
                "decision": result["decision"],
                "chosen_model": result["selection"],
                "artifacts": result["artifacts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
