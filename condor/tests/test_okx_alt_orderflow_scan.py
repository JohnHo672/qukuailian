"""Deterministic checks for the OKX alt order-flow scanner."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROUTINE_PATH = (
    Path(__file__).parents[1]
    / "agents"
    / "directional_trader"
    / "routines"
    / "okx_alt_orderflow_scan.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "okx_alt_orderflow_scan_test", ROUTINE_PATH
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scan = _load()


def test_demo_connector_uses_okx_public_candles_only():
    assert scan.public_candle_connector("okx_perpetual_demo") == "okx_perpetual"
    assert scan.public_candle_connector("binance_perpetual") == "binance_perpetual"


def test_rank_tickers_uses_both_tails_and_excludes_majors():
    rows = [
        {
            "instId": "BTC-USDT-SWAP",
            "last": "110",
            "open24h": "100",
            "volCcy24h": "1000000",
        },
        {
            "instId": "ALT1-USDT-SWAP",
            "last": "1.20",
            "open24h": "1.00",
            "volCcy24h": "10000000",
        },
        {
            "instId": "ALT2-USDT-SWAP",
            "last": "0.80",
            "open24h": "1.00",
            "volCcy24h": "20000000",
        },
        {
            "instId": "THIN-USDT-SWAP",
            "last": "2.00",
            "open24h": "1.00",
            "volCcy24h": "10",
        },
    ]

    result = scan.rank_tickers(rows, {"BTC"}, min_turnover=1_000_000, top_n=1)

    assert [(row["pair"], row["ranking_side"]) for row in result] == [
        ("ALT1-USDT", "GAINER"),
        ("ALT2-USDT", "LOSER"),
    ]


def test_book_metrics_calculates_normalized_imbalance_and_quote_vwap():
    metrics = scan.book_metrics(
        {
            "bids": [[100.0, 8.0], [99.9, 4.0]],
            "asks": [[100.1, 2.0], [100.2, 2.0]],
        },
        depth_band_pct=0.25,
        quote_target=150.0,
    )

    assert metrics is not None
    assert metrics["obi"] > 0.45
    assert metrics["spread_pct"] == pytest.approx(0.09995, rel=1e-3)
    assert 0 < metrics["buy_vwap_slippage_pct"] < 0.1
    assert 0 < metrics["sell_vwap_slippage_pct"] < 0.1


def test_volume_profile_levels_surround_poc():
    candles = []
    for index in range(20):
        price = 100 + (index % 4) * 0.1
        candles.append(
            {
                "timestamp": index,
                "open": price,
                "high": price + 0.1,
                "low": price - 0.1,
                "close": price + 0.05,
                "volume": 100 if index % 4 == 1 else 10,
            }
        )

    poc, val, vah = scan.volume_profile_levels(candles, buckets=8)

    assert val <= poc <= vah
    assert 99.9 <= val <= 100.5
    assert 99.9 <= vah <= 100.5


def test_trade_volume_profile_uses_actual_print_sizes():
    trades = []
    for index in range(90):
        price = 100.0 if index < 60 else 101.0 + (index % 3) * 0.1
        trades.append({"px": str(price), "sz": "10" if price == 100.0 else "1"})

    poc, val, vah = scan.trade_volume_profile_levels(trades, buckets=10)

    assert poc < 100.2
    assert val <= poc <= vah


def test_percentage_sizing_is_equity_based_and_capped():
    sizing = scan.percentage_size(
        equity=100,
        leverage=7,
        stop_pct=0.65,
        cost_pct=0.12,
        risk_pct=0.6,
        margin_cap_pct=15,
    )

    assert sizing["margin_pct"] == pytest.approx(11.1317, rel=1e-3)
    assert sizing["margin_usdt"] == pytest.approx(11.1317, rel=1e-3)
    assert sizing["notional_usdt"] == pytest.approx(77.922, rel=1e-3)
    assert sizing["estimated_risk_usdt"] == pytest.approx(0.6, rel=1e-6)

    capped = scan.percentage_size(
        equity=200,
        leverage=5,
        stop_pct=0.2,
        cost_pct=0,
        risk_pct=1,
        margin_cap_pct=15,
    )
    assert capped["margin_pct"] == 15
    assert capped["margin_usdt"] == 30


def test_connector_equity_is_hard_capped_to_100u_test_budget():
    equity, source = scan.bounded_equity(71_847.39, fallback=100, cap=100)

    assert equity == 100
    assert source == "live connector, capped from 71847.39"


def test_equity_fallback_cannot_exceed_the_hard_cap():
    equity, source = scan.bounded_equity(None, fallback=250, cap=100)

    assert equity == 100
    assert source == "preview"
