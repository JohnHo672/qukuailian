"""Check whether the fill-aware v2 dataset is large and clean enough to retrain."""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReadinessThresholds:
    completed_days: int = 7
    labelled_rows: int = 1_000_000
    symbols: int = 20
    rows_per_symbol: int = 5_000
    long_fill_labels: int = 10_000
    short_fill_labels: int = 10_000
    feature_completeness: float = 0.85
    invalid_feature_rows: int = 0


def evaluate_readiness(
    stats: dict[str, Any], thresholds: ReadinessThresholds
) -> dict[str, Any]:
    per_symbol = stats.get("rows_per_symbol") or {}
    gates = {
        "completed_days": int(stats.get("completed_days", 0))
        >= thresholds.completed_days,
        "labelled_rows": int(stats.get("labelled_rows", 0)) >= thresholds.labelled_rows,
        "symbols": int(stats.get("symbols", 0)) >= thresholds.symbols,
        "rows_per_symbol": bool(per_symbol)
        and min(int(value) for value in per_symbol.values())
        >= thresholds.rows_per_symbol,
        "long_fill_labels": int(stats.get("long_fill_labels", 0))
        >= thresholds.long_fill_labels,
        "short_fill_labels": int(stats.get("short_fill_labels", 0))
        >= thresholds.short_fill_labels,
        "feature_completeness": float(stats.get("feature_completeness", 0.0))
        >= thresholds.feature_completeness,
        "invalid_feature_rows": int(stats.get("invalid_feature_rows", -1))
        <= thresholds.invalid_feature_rows,
    }
    return {
        "ready": all(gates.values()),
        "gates": gates,
        "stats": stats,
        "thresholds": asdict(thresholds),
        "decision": (
            "READY_TO_RETRAIN_RESEARCH_MODEL"
            if all(gates.values())
            else "KEEP_COLLECTING_DO_NOT_SIMULATE"
        ),
    }


def scan_dataset(root: Path) -> dict[str, Any]:
    files = sorted(root.glob("features-*.jsonl.gz"))
    completed = files[:-1] if files else []
    rows = 0
    symbols: Counter[str] = Counter()
    long_fills = short_fills = 0
    schema_versions: Counter[str] = Counter()
    for path in completed:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                rows += 1
                symbols[str(row.get("inst_id") or "")] += 1
                schema_versions[str(row.get("schema_version") or "")] += 1
                long_fills += int(row.get("long_maker_entry_fill_proxy_30s") or 0)
                short_fills += int(row.get("short_maker_entry_fill_proxy_30s") or 0)
    quality_path = root / "quality.json"
    quality = json.loads(quality_path.read_text()) if quality_path.exists() else {}
    return {
        "completed_days": len(completed),
        "completed_files": [str(path) for path in completed],
        "active_file": str(files[-1]) if files else None,
        "labelled_rows": rows,
        "symbols": len([key for key in symbols if key]),
        "rows_per_symbol": {key: value for key, value in symbols.items() if key},
        "long_fill_labels": long_fills,
        "short_fill_labels": short_fills,
        "schema_versions": dict(schema_versions),
        "feature_completeness": float(quality.get("feature_completeness", 0.0)),
        "invalid_feature_rows": int(quality.get("invalid_feature_rows", -1)),
        "quality_updated_at": quality.get("updated_at"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/research/alt_cross_section_v2"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate_readiness(scan_dataset(args.data_dir), ReadinessThresholds())
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
