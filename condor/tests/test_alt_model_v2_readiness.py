from research.alt_model_v2.readiness import ReadinessThresholds, evaluate_readiness


def _passing_stats():
    return {
        "completed_days": 7,
        "labelled_rows": 1_000_000,
        "symbols": 20,
        "rows_per_symbol": {f"C{i}": 5_000 for i in range(20)},
        "long_fill_labels": 10_000,
        "short_fill_labels": 10_000,
        "feature_completeness": 0.9,
        "invalid_feature_rows": 0,
    }


def test_all_gates_are_required_before_retraining():
    result = evaluate_readiness(_passing_stats(), ReadinessThresholds())
    assert result["ready"] is True
    assert result["decision"] == "READY_TO_RETRAIN_RESEARCH_MODEL"


def test_one_failed_gate_keeps_simulation_disabled():
    stats = _passing_stats()
    stats["long_fill_labels"] = 9_999
    result = evaluate_readiness(stats, ReadinessThresholds())
    assert result["ready"] is False
    assert result["gates"]["long_fill_labels"] is False
    assert result["decision"] == "KEEP_COLLECTING_DO_NOT_SIMULATE"
