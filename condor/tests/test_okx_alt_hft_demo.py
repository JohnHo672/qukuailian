"""Pure safety and profitability checks for the OKX demo HFT forward test."""

import asyncio

from agents.directional_trader.routines.okx_alt_hft_demo import (
    Config,
    cap_amount_to_exchange_max,
    conviction_margin_cap,
    daily_pair_from_rows,
    fee_edge_decision,
    liquidity_sized_position,
    observed_fee_rates,
    performance_summary,
    position_size,
    profitability_verdict,
    quantize_amount,
    signal_decision,
    stale_unfilled_executors,
    _demo_trading_pairs,
    _ensure_initialized_client,
    _okx_candles,
)


def _decision(**overrides):
    values = {
        "direction": "LONG",
        "obi": 0.12,
        "buy_ratio": 0.56,
        "sell_ratio": 0.44,
        "price": 101,
        "vwap": 100,
        "poc": 100,
        "val": 99,
        "vah": 100.5,
        "spread_pct": 0.04,
        "slippage_pct": 0.03,
        "min_obi": 0.08,
        "min_taker_ratio": 0.54,
        "min_confirmations": 3,
        "max_spread_pct": 0.12,
        "max_vwap_slippage_pct": 0.08,
    }
    values.update(overrides)
    return signal_decision(**values)


def test_connector_is_hard_locked_to_okx_demo():
    config = Config()
    assert config.connector_name == "okx_perpetual_demo"
    assert config.controller_id.startswith("okx-alt-hft-demo-")
    assert config.run_until_depleted is True
    assert config.minimum_equity_usdt == 1
    try:
        Config(connector_name="okx_perpetual")
    except ValueError as exc:
        assert "demo" in str(exc)
    else:
        raise AssertionError("live connector was accepted")


def test_percentage_risk_envelope_is_fixed_to_10x_and_at_most_30_percent():
    config = Config()
    assert config.leverage == 10
    assert config.margin_per_trade_pct == 30
    assert config.min_margin_per_trade_pct == 1
    assert config.max_open_positions == 1
    assert config.dynamic_pair_selection is True
    assert config.cooldown_seconds == 60
    assert position_size(100, 30, 10) == (30, 300)
    for bad in (
        {"leverage": 9},
        {"margin_per_trade_pct": 31},
        {"max_open_positions": 2},
    ):
        try:
            Config(**bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe config was accepted: {bad}")


def test_liquidity_sizing_reduces_margin_until_slippage_fits():
    sized = liquidity_sized_position(
        book={
            "bids": [["99.99", "1000"]],
            "asks": [["100.01", "50"], ["101", "1000"]],
        },
        direction="LONG",
        account_equity=10_000,
        leverage=10,
        min_margin_pct=1,
        max_margin_pct=30,
        max_slippage_pct=0.08,
    )
    assert sized is not None
    assert sized["margin_pct"] == 5
    assert sized["margin_usdt"] == 500
    assert sized["notional_usdt"] == 5_000


def test_liquidity_sizing_skips_when_even_one_percent_is_too_large():
    sized = liquidity_sized_position(
        book={
            "bids": [["99.99", "1"]],
            "asks": [["100.01", "1"], ["101", "1000"]],
        },
        direction="LONG",
        account_equity=10_000,
        leverage=10,
        min_margin_pct=1,
        max_margin_pct=30,
        max_slippage_pct=0.08,
    )
    assert sized is None


def test_conviction_sizing_uses_5_15_30_percent_tiers():
    common = {"min_margin_pct": 1, "max_margin_pct": 30}
    assert conviction_margin_cap(score=3, obi=0.09, taker_ratio=0.55, **common) == 5
    assert conviction_margin_cap(score=3, obi=0.13, taker_ratio=0.59, **common) == 15
    assert conviction_margin_cap(score=4, obi=0.21, taker_ratio=0.61, **common) == 30
    assert conviction_margin_cap(
        score=4,
        obi=0.30,
        taker_ratio=0.70,
        min_margin_pct=1,
        max_margin_pct=20,
    ) == 20


def test_fee_gate_requires_target_to_cover_fees_spread_and_slippage_twice():
    allowed = fee_edge_decision(
        take_profit_pct=0.45,
        fee_rate_pct=0.05,
        spread_pct=0.03,
        slippage_pct=0.03,
        max_fee_rate_pct=0.08,
        min_edge_cost_multiple=2,
    )
    assert allowed["eligible"] is True
    assert round(allowed["estimated_cost_pct"], 5) == 0.16

    expensive = fee_edge_decision(
        take_profit_pct=0.45,
        fee_rate_pct=0.50,
        spread_pct=0.01,
        slippage_pct=0.01,
        max_fee_rate_pct=0.08,
        min_edge_cost_multiple=2,
    )
    assert expensive["eligible"] is False


def test_observed_fee_rate_uses_real_turnover_and_deduplicates_rows():
    row = {
        "executor_id": "one",
        "trading_pair": "ALT-USDT",
        "filled_amount_quote": 200,
        "cum_fees_quote": 0.1,
    }
    assert observed_fee_rates([row, row]) == {"ALT-USDT": 0.05}


def test_daily_pair_lock_recovers_only_todays_first_pair():
    assert (
        daily_pair_from_rows(
            [
                {"created_at": 1790780400, "trading_pair": "TODAY-USDT"},
                {"created_at": 1790694000, "trading_pair": "OLD-USDT"},
            ],
            "2026-09-30",
        )
        == "TODAY-USDT"
    )


def test_signal_needs_three_confirmations_and_liquidity():
    assert _decision()["eligible"] is True
    assert _decision(obi=0.0, buy_ratio=0.50)["eligible"] is False
    assert _decision(spread_pct=0.2)["eligible"] is False


def test_short_signal_uses_mirrored_flow():
    result = _decision(
        direction="SHORT",
        obi=-0.12,
        buy_ratio=0.44,
        sell_ratio=0.56,
        price=99,
        vwap=100,
        poc=100,
        val=99.5,
        vah=101,
    )
    assert result["eligible"] is True


def test_amount_rounds_down_to_exchange_step():
    amount = quantize_amount(
        35,
        7.9,
        {
            "min_base_amount_increment": 0.1,
            "min_order_size": 0.1,
            "min_notional_size": 5,
        },
    )
    assert amount == 4.4


def test_amount_is_capped_below_exchange_maximum_with_buffer():
    rules = {"min_base_amount_increment": 10, "min_order_size": 10}
    assert cap_amount_to_exchange_max(7888, 1400, rules) == 1330
    assert cap_amount_to_exchange_max(200, 1400, rules) == 200


def test_profitability_uses_net_pnl_profit_factor_and_drawdown():
    rows = [
        {
            "status": "TERMINATED",
            "filled_amount_quote": 35,
            "net_pnl_quote": 1.0,
            "cum_fees_quote": 0.1,
        },
        {
            "status": "TERMINATED",
            "filled_amount_quote": 35,
            "net_pnl_quote": -0.4,
            "cum_fees_quote": 0.1,
        },
        {"status": "RUNNING", "filled_amount_quote": 35, "net_pnl_quote": 99},
    ]
    summary = performance_summary(rows)
    assert summary["closed"] == 2
    assert summary["net_pnl"] == 0.6
    assert summary["profit_factor"] == 2.5
    assert summary["max_drawdown"] == 0.4
    assert summary["failed"] == 0
    assert profitability_verdict(summary, 2, 1.15, 3) == "PROFITABLE"
    assert profitability_verdict(summary, 30, 1.15, 3) == "INCONCLUSIVE"


def test_zero_fill_failed_executor_is_not_counted_as_a_trade():
    summary = performance_summary(
        [
            {
                "status": "TERMINATED",
                "close_type": "FAILED",
                "filled_amount_quote": 0,
                "net_pnl_quote": 0,
            }
        ]
    )
    assert summary["closed"] == 0
    assert summary["failed"] == 1


def test_stale_unfilled_maker_entry_is_cancelled_but_filled_is_not():
    rows = [
        {
            "executor_id": "stale",
            "status": "RUNNING",
            "created_at": 100,
            "filled_amount_quote": 0,
        },
        {
            "executor_id": "filled",
            "status": "RUNNING",
            "created_at": 100,
            "filled_amount_quote": 1,
        },
        {
            "executor_id": "fresh",
            "status": "RUNNING",
            "created_at": 190,
            "filled_amount_quote": 0,
        },
    ]
    assert [row["executor_id"] for row in stale_unfilled_executors(
        rows, now=200, timeout_seconds=30
    )] == ["stale"]


def test_okx_candles_preserves_positional_rows():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return {
                "code": "0",
                "data": [["1", "2", "3", "1", "2.5", "9"], {"bad": True}],
            }

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    assert asyncio.run(_okx_candles(Session(), "ALT-USDT-SWAP")) == [
        ["1", "2", "3", "1", "2.5", "9"]
    ]


def test_demo_trading_pairs_uses_authenticated_account_universe():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return {"trading_pairs": ["MINA-USDT", "PUMP-USDT-SWAP", "BTC-USD"]}

    class Session:
        def get(self, url, params):
            assert url.endswith("/connectors/okx_perpetual_demo/account-instruments")
            assert params == {"account_name": "master_account", "inst_type": "SWAP"}
            return Response()

    class Client:
        base_url = "http://hummingbot-api:8000"
        _session = Session()

    assert asyncio.run(_demo_trading_pairs(Client(), Config())) == {"MINA-USDT"}


def test_demo_trading_pairs_fails_closed_on_empty_universe():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return {"trading_pairs": []}

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    class Client:
        base_url = "http://hummingbot-api:8000"
        _session = Session()

    try:
        asyncio.run(_demo_trading_pairs(Client(), Config()))
    except RuntimeError as exc:
        assert "no account-tradable" in str(exc)
    else:
        raise AssertionError("empty demo universe was accepted")


def test_closed_api_client_is_reinitialized_for_long_running_routine():
    class Client:
        _session = None

        def __init__(self):
            self.init_calls = 0

        async def init(self):
            self.init_calls += 1
            self._session = type("Session", (), {"closed": False})()

    client = Client()
    assert asyncio.run(_ensure_initialized_client(client)) is client
    assert client.init_calls == 1
    asyncio.run(_ensure_initialized_client(client))
    assert client.init_calls == 1
