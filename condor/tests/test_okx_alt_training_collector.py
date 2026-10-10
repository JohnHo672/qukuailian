from __future__ import annotations

from collections import deque
from pathlib import Path

from agents.directional_trader.routines.okx_alt_training_collector import (
    Config,
    InstrumentMeta,
    MarketState,
    ROLLING_KEYS,
    SCHEMA_VERSION,
    add_benchmark_context,
    add_percentiles,
    compute_features,
    instrument_map,
    labelled_row,
    percentile_rank,
    select_universe,
)


def _instrument(inst_id: str, *, listed_at: float = 1.0) -> dict:
    return {
        "instId": inst_id,
        "state": "live",
        "ctType": "linear",
        "ctVal": "0.1",
        "ctMult": "1",
        "tickSz": "0.01",
        "lotSz": "1",
        "listTime": str(int(listed_at * 1000)),
    }


def _ticker(inst_id: str, turnover: float, *, spread_bps: float = 2) -> dict:
    last = 10.0
    bid = last
    ask = bid * (1 + spread_bps / 10_000)
    return {
        "instId": inst_id,
        "last": str(last),
        "bidPx": str(bid),
        "askPx": str(ask),
        "open24h": "9.5",
        "volCcy24h": str(turnover / last),
    }


def test_universe_is_liquid_non_major_and_keeps_eth_benchmark():
    now = 100 * 86400
    raw = [
        _instrument("ETH-USDT-SWAP"),
        _instrument("AAA-USDT-SWAP"),
        _instrument("BBB-USDT-SWAP"),
        _instrument("BTC-USDT-SWAP"),
        _instrument("WIDE-USDT-SWAP"),
        _instrument("NEW-USDT-SWAP", listed_at=95 * 86400),
    ]
    instruments = instrument_map(raw)
    selected = select_universe(
        [
            _ticker("ETH-USDT-SWAP", 100_000_000),
            _ticker("AAA-USDT-SWAP", 50_000_000),
            _ticker("BBB-USDT-SWAP", 20_000_000),
            _ticker("BTC-USDT-SWAP", 200_000_000),
            _ticker("WIDE-USDT-SWAP", 80_000_000, spread_bps=50),
            _ticker("NEW-USDT-SWAP", 80_000_000),
        ],
        instruments,
        now=now,
        spot_bases={"ETH", "AAA", "BBB", "BTC", "WIDE", "NEW"},
        excluded={"BTC"},
        size=2,
        min_turnover=5_000_000,
        max_spread_bps=12,
        min_listing_days=30,
    )
    assert [item["inst_id"] for item in selected] == [
        "AAA-USDT-SWAP",
        "BBB-USDT-SWAP",
        "ETH-USDT-SWAP",
    ]


def test_percentile_rank_midpoint_ties():
    assert percentile_rank(deque([1.0, 2.0, 2.0, 4.0]), 2.0) == 0.5
    assert percentile_rank(deque(), 10.0) == 0.5


def test_features_are_scale_normalized_and_notional_aware():
    meta = InstrumentMeta("AAA-USDT-SWAP", "AAA", 0.1, 1, 0.01, 1, 0)
    state = MarketState(meta, 20_000_000, 3.0, 2.0)
    state.update_book(
        {
            "ts": "100000",
            "bids": [["100", "20"], ["99.9", "10"]],
            "asks": [["100.1", "10"], ["100.3", "10"]],
        }
    )
    state.update_book(
        {
            "ts": "99900",
            "bids": [["100", "15"], ["99.9", "12"]],
            "asks": [["100.1", "12"], ["100.3", "8"]],
        }
    )
    state.add_trades(
        [
            {"ts": "99000", "px": "100", "sz": "10", "side": "buy"},
            {"ts": "99500", "px": "100.1", "sz": "5", "side": "sell"},
        ]
    )
    row = compute_features(state, now=100.0)
    assert row is not None
    assert row["schema_version"] == SCHEMA_VERSION
    assert row["spread_bps"] > 0
    assert -1 <= row["obi_top5"] <= 1
    assert row["depth_total_top5_usdt"] > 0
    assert 0 <= row["buy_ratio"] <= 1
    assert row["book_update_count_5s"] == 1
    assert row["bid_depth_removed_5s_usdt"] > 0
    assert 0 <= row["bid_cancel_proxy_ratio_5s"] <= 1


def test_percentiles_and_eth_context_are_added():
    meta_a = InstrumentMeta("AAA-USDT-SWAP", "AAA", 1, 1, 0.01, 1, 0)
    meta_e = InstrumentMeta("ETH-USDT-SWAP", "ETH", 1, 1, 0.01, 1, 0)
    states = {
        meta_a.inst_id: MarketState(meta_a, 1, 0, 1),
        meta_e.inst_id: MarketState(meta_e, 1, 0, 1),
    }
    template = {key: 1.0 for key in ROLLING_KEYS}
    rows = [
        {
            "inst_id": meta_a.inst_id,
            "is_benchmark": False,
            "trade_return_5s_bps": 2.0,
            **template,
        },
        {
            "inst_id": meta_e.inst_id,
            "is_benchmark": True,
            "trade_return_5s_bps": 3.0,
            **template,
        },
    ]
    add_percentiles(rows, states, 300)
    add_benchmark_context(rows)
    assert rows[0]["spread_bps_rolling_pct"] == 0.5
    assert rows[0]["spread_bps_cross_section_pct"] == 0.5
    assert rows[0]["eth_trade_return_5s_bps"] == 3.0


def test_delayed_labels_include_fee_adjusted_long_and_short_targets():
    config = Config(maker_fee_bps_per_side=2)
    pending = {
        "timestamp": 100.0,
        "mid": 100.0,
        "best_bid": 99.9,
        "best_ask": 100.1,
    }
    frames = deque(
        [
            {"timestamp": 101.0, "mid": 101.0, "bid": 100.9, "ask": 101.1},
            {"timestamp": 105.0, "mid": 102.0, "bid": 101.9, "ask": 102.1},
            {"timestamp": 115.0, "mid": 99.0, "bid": 98.9, "ask": 99.1},
            {"timestamp": 130.0, "mid": 104.0, "bid": 103.9, "ask": 104.1},
        ]
    )
    trades = deque(
        [
            {
                "timestamp": 102.0,
                "price": 99.9,
                "side": "sell",
                "quote_notional": 10.0,
            },
            {
                "timestamp": 103.0,
                "price": 100.1,
                "side": "buy",
                "quote_notional": 10.0,
            },
        ]
    )
    row = labelled_row(pending, frames, trades, config)
    assert row is not None
    assert round(row["future_return_30s_bps"], 6) == 400.0
    assert round(row["future_long_net_maker_30s_bps"], 6) == 396.0
    assert round(row["future_short_net_maker_30s_bps"], 6) == -404.0
    assert row["long_maker_entry_fill_proxy_1s"] == 0
    assert row["long_maker_entry_fill_proxy_5s"] == 1
    assert row["short_maker_entry_fill_proxy_5s"] == 1
    assert row["future_long_maker_entry_taker_exit_net_30s_bps"] > 0


def test_universe_rejects_perpetual_without_crypto_spot_market():
    now = 100 * 86400
    instruments = instrument_map(
        [
            _instrument("AAA-USDT-SWAP"),
            _instrument("SOXL-USDT-SWAP"),
            _instrument("ETH-USDT-SWAP"),
        ]
    )
    selected = select_universe(
        [
            _ticker("AAA-USDT-SWAP", 20_000_000),
            _ticker("SOXL-USDT-SWAP", 50_000_000),
            _ticker("ETH-USDT-SWAP", 100_000_000),
        ],
        instruments,
        now=now,
        spot_bases={"AAA", "ETH"},
        excluded=set(),
        size=1,
        min_turnover=5_000_000,
        max_spread_bps=12,
        min_listing_days=30,
    )
    assert [item["inst_id"] for item in selected] == [
        "AAA-USDT-SWAP",
        "ETH-USDT-SWAP",
    ]


def test_collector_source_has_no_private_trading_dependency():
    source = Path(
        "agents/directional_trader/routines/okx_alt_training_collector.py"
    ).read_text(encoding="utf-8")
    assert "executor_create" not in source
    assert "get_client" not in source
    assert "create_position_executor" not in source
    assert "okx_perpetual_demo" not in source
