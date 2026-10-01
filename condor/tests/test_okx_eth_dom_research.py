"""Safety and microstructure tests for ETH DOM demo research."""

import time
from collections import deque

from agents.directional_trader.routines.okx_eth_dom_research import (
    BookFrame,
    Config,
    DomState,
    ENTRY_ORDER_TYPE,
    RISK_EXIT_ORDER_TYPE,
    TAKE_PROFIT_ORDER_TYPE,
    WallState,
    account_position_direction,
    active_reserved_margin,
    dom_signal,
    dynamic_barriers,
    depth_limited_margin,
    detect_retail_stop_run,
    executor_direction,
    executor_lifecycle,
    favorable_move_pct,
    fee_covered_trailing_barrier,
    infer_dom_intent,
    market_depth_metrics,
    one_shot_position_margin,
    pair_active_order_rows,
    pair_position_rows,
    pending_entry_should_expire,
    protected_exit_reason,
    shutdown_blocks_entry,
    should_exit,
)


def test_config_is_demo_eth_only_with_one_twenty_percent_position():
    config = Config()
    assert config.connector_name == "okx_perpetual_demo"
    assert config.trading_pair == "ETH-USDT"
    assert config.leverage == 10
    assert config.position_margin_pct == 20
    assert config.max_positions == 1
    assert config.time_limit_seconds == 120
    assert config.min_seconds_between_entries == 60
    assert config.unfilled_shutdown_grace_seconds == 60
    assert config.dust_position_notional_usdt == 5
    assert config.entry_timeout_seconds == 15
    assert config.exit_cooldown_seconds == 20
    assert ENTRY_ORDER_TYPE == 3
    assert TAKE_PROFIT_ORDER_TYPE == 3
    assert RISK_EXIT_ORDER_TYPE == 2
    for unsafe in (
        {"connector_name": "okx_perpetual"},
        {"trading_pair": "BTC-USDT"},
        {"position_margin_pct": 19},
        {"max_positions": 2},
        {"leverage": 20},
    ):
        try:
            Config(**unsafe)
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe config accepted: {unsafe}")


def test_incremental_book_and_microprice_are_maintained():
    state = DomState()
    state.apply_book(
        {
            "action": "snapshot",
            "data": [
                {
                    "seqId": "10",
                    "prevSeqId": "0",
                    "bids": [["1999", "10"], ["1998", "5"]],
                    "asks": [["2001", "2"], ["2002", "5"]],
                }
            ],
        }
    )
    state.apply_book(
        {
            "action": "update",
            "data": [
                {
                    "seqId": "11",
                    "prevSeqId": "10",
                    "bids": [["1999", "12"]],
                    "asks": [["2001", "0"], ["2000.5", "2"]],
                }
            ],
        }
    )
    frame = state.book_frame()
    assert frame is not None
    assert frame.best_bid == 1999
    assert frame.best_ask == 2000.5
    assert frame.obi > 0
    assert frame.microprice > frame.mid


def test_persistent_replenished_wall_scores_as_intent_not_a_claim():
    now = time.time()
    state = DomState(
        bids={1999.0: 100.0, 1998.0: 10.0},
        asks={2001.0: 10.0, 2002.0: 10.0},
    )
    state.walls[("bid", 1999.0)] = WallState(
        first_seen=now - 2,
        last_seen=now,
        max_size=120,
        current_size=100,
        add_events=4,
        cancel_events=0,
        replenished_size=60,
    )
    for _ in range(10):
        state.trades.append(
            {"price": 1999.0, "size": 8.0, "side": "sell", "time": now - 0.2}
        )
    result = infer_dom_intent(state, wall_multiple=4, min_wall_age_ms=750)
    assert result["bid_iceberg"] > 0.45
    assert result["bid_intent"] > result["bid_spoof_risk"]


def test_dom_entry_requires_flow_value_intent_and_low_spoof_risk():
    config = Config()
    result = dom_signal(
        frame=BookFrame(time.time(), 2000, 1999.5, 2000.5, 0.2, 2000.2),
        flow={"buy_ratio": 0.62, "sell_ratio": 0.38},
        intent={
            "bid_intent": 0.75,
            "ask_intent": 0.1,
            "bid_iceberg": 0.65,
            "ask_iceberg": 0.0,
            "bid_spoof_risk": 0.1,
            "ask_spoof_risk": 0.8,
        },
        depth={"depth_imbalance_10bps": 0.20},
        sweep={"long_reclaim": False, "short_reclaim": False},
        vwap=1995,
        poc=1997,
        config=config,
    )
    assert result["direction"] == "LONG"
    assert result["score"] == 7


def test_market_depth_is_measured_in_multiple_bands_and_limits_size():
    state = DomState(
        bids={1999.5: 10, 1999.0: 20, 1996.0: 30},
        asks={2000.5: 5, 2001.0: 5, 2004.0: 10},
    )
    depth = market_depth_metrics(state)
    assert depth["bid_depth_5bps"] == (1999.5 * 10 + 1999.0 * 20) * 0.1
    assert depth["bid_depth_10bps"] > depth["ask_depth_10bps"]
    assert depth["depth_imbalance_10bps"] > 0
    assert depth_limited_margin(
        direction="LONG",
        requested_margin=1000,
        leverage=10,
        depth=depth,
        max_depth_share_pct=10,
    ) == 0
    assert depth_limited_margin(
        direction="LONG",
        requested_margin=10,
        leverage=10,
        depth=depth,
        max_depth_share_pct=10,
    ) == 10


def test_margin_permission_is_exactly_twenty_percent_and_never_scales_in():
    rows = [
        {
            "status": "RUNNING",
            "config": {"amount": 0.05, "entry_price": 2000, "side": 1},
        },
        {
            "status": "RUNNING",
            "config": {"amount": 0.10, "entry_price": 2000, "side": 1},
        },
    ]
    reserved = active_reserved_margin(rows, leverage=10)
    assert reserved == 30
    assert reserved == 30
    assert one_shot_position_margin(
        equity=200, position_margin_pct=20, has_exposure=False
    ) == 40
    assert one_shot_position_margin(
        equity=200, position_margin_pct=20, has_exposure=True
    ) == 0
    assert one_shot_position_margin(
        equity=0, position_margin_pct=20, has_exposure=False
    ) == 0


def test_any_existing_eth_position_blocks_another_entry():
    payload = {
        "data": [
            {"trading_pair": "ETH-USDT", "amount": -1.5},
            {"trading_pair": "BTC-USDT", "amount": 2},
            {"trading_pair": "ETH-USDT", "amount": 0},
        ]
    }
    positions = pair_position_rows(payload)
    assert positions == [{"trading_pair": "ETH-USDT", "amount": -1.5}]
    assert one_shot_position_margin(
        equity=100, position_margin_pct=20, has_exposure=bool(positions)
    ) == 0
    assert account_position_direction(positions[0]) == "SHORT"
    assert account_position_direction({"side": "LONG", "amount": 1}) == "LONG"


def test_uncloseable_position_dust_does_not_block_the_next_trade():
    payload = {
        "data": [
            {
                "trading_pair": "ETH-USDT",
                "amount": -0.0009,
                "entry_price": 2700,
            }
        ]
    }
    assert pair_position_rows(payload, min_notional_usdt=5) == []


def test_market_entry_is_not_cancelled_after_venue_position_appears():
    assert pending_entry_should_expire(
        pending_seconds=16,
        timeout_seconds=15,
        account_has_position=False,
    ) is True
    assert pending_entry_should_expire(
        pending_seconds=60,
        timeout_seconds=15,
        account_has_position=True,
    ) is False
    assert pending_entry_should_expire(
        pending_seconds=60,
        timeout_seconds=15,
        account_has_position=False,
        venue_order_active=True,
    ) is True


def test_any_non_terminal_venue_order_blocks_the_pair():
    payload = {
        "data": [
            {"trading_pair": "ETH-USDT", "status": "OPEN", "order_id": "1"},
            {
                "trading_pair": "ETH-USDT",
                "status": "PARTIALLY_FILLED",
                "order_id": "2",
            },
            {"trading_pair": "ETH-USDT", "status": "FILLED", "order_id": "3"},
            {"trading_pair": "BTC-USDT", "status": "OPEN", "order_id": "4"},
        ]
    }
    assert [row["order_id"] for row in pair_active_order_rows(payload)] == ["1", "2"]


def test_dynamic_barriers_use_profile_and_are_bounded():
    barriers = dynamic_barriers(
        direction="LONG",
        price=2000,
        vwap=1996,
        poc=1997,
        val=1994,
        vah=2010,
        intent={"bid_wall_price": 1998, "ask_wall_price": 2012},
        config=Config(),
    )
    assert 0.12 <= barriers["stop_loss_pct"] <= 0.35
    assert 0.18 <= barriers["take_profit_pct"] <= 0.60
    assert barriers["take_profit_pct"] >= barriers["stop_loss_pct"] * 1.20


def test_trailing_stop_first_lock_covers_fees_and_buffer():
    trailing = fee_covered_trailing_barrier(
        structural_stop_pct=0.18,
        round_trip_fee_pct=0.10,
        break_even_buffer_pct=0.03,
        trailing_delta_stop_ratio=0.50,
        min_trailing_delta_pct=0.06,
        max_trailing_delta_pct=0.25,
    )
    assert trailing["trailing_delta_pct"] == 0.09
    assert trailing["activation_pct"] == 0.22
    assert trailing["locked_profit_pct"] == 0.13
    assert (
        trailing["activation_pct"] - trailing["trailing_delta_pct"]
        >= 0.10 + 0.03
    )


def test_retail_stop_run_uses_prior_micro_swing_not_current_extreme():
    now = time.time()
    frames = deque(maxlen=1200)
    for offset, mid in ((-20, 2000), (-12, 2001), (-8, 1999.5)):
        frames.append(BookFrame(now + offset, mid, mid - 0.5, mid + 0.5, 0, mid))
    frames.append(BookFrame(now - 1, 2001.3, 2000.8, 2001.8, 0, 2001.3))
    result = detect_retail_stop_run(
        DomState(frames=frames),
        lookback_seconds=30,
        recent_seconds=3,
        excursion_bps=0.75,
        now=now,
    )
    assert result["upper_stop_run"] is True
    assert result["lower_stop_run"] is False
    assert result["prior_high"] == 2001


def test_ultra_short_exit_takes_profit_at_retail_stop_pool():
    common = {
        "frame": BookFrame(time.time(), 2000, 1999.5, 2000.5, 0, 2000),
        "flow": {"buy_ratio": 0.5, "sell_ratio": 0.5},
        "intent": {},
        "depth": {},
        "vwap": 2000,
        "poc": 2000,
    }
    assert should_exit(
        "LONG",
        retail_stop_run={"upper_stop_run": True, "lower_stop_run": False},
        **common,
    ) == "retail short-stop pool reached"
    assert should_exit(
        "SHORT",
        retail_stop_run={"upper_stop_run": False, "lower_stop_run": True},
        **common,
    ) == "retail long-stop pool reached"


def test_ultra_short_exit_detects_supporting_liquidity_collapse():
    reason = should_exit(
        "LONG",
        frame=BookFrame(time.time(), 2000, 1999.5, 2000.5, -0.1, 1999.9),
        flow={"buy_ratio": 0.40, "sell_ratio": 0.60},
        intent={"ask_intent": 0.70},
        depth={
            "bid_depth_10bps": 500,
            "ask_depth_10bps": 1000,
            "depth_imbalance_5bps": -0.25,
        },
        vwap=2001,
        poc=2001,
    )
    assert reason == "defensive bid-liquidity collapse"


def test_shutting_down_executor_still_blocks_and_reserves_margin():
    rows = [
        {
            "status": "SHUTTING_DOWN",
            "config": {"amount": 0.1, "entry_price": 2000, "side": 2},
        }
    ]
    active, closing, blocking = executor_lifecycle(rows)
    assert active == []
    assert closing == rows
    assert blocking == rows
    assert active_reserved_margin(blocking, leverage=10) == 20


def test_executor_direction_accepts_live_strings_and_archived_numbers():
    assert executor_direction({"config": {"side": "BUY"}}) == "LONG"
    assert executor_direction({"custom_info": {"side": "SELL"}}) == "SHORT"
    assert executor_direction({"config": {"side": 1}}) == "LONG"
    assert executor_direction({"config": {"side": 2}}) == "SHORT"
    assert executor_direction({"config": {"side": "unknown"}}) is None


def test_gross_winner_smaller_than_fees_is_not_discretionarily_closed():
    # A representative historical short moved +0.04% before exit, but its
    # observed round-trip fees were 0.07%, so closing crystallised a net loss.
    assert favorable_move_pct("SHORT", 2698.55, 2697.47) < 0.07
    assert protected_exit_reason(
        candidate_reason="confirmed bid absorption",
        candidate_confirmations=3,
        required_confirmations=3,
        current_gross_pct=0.04,
        peak_gross_pct=0.04,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.03,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=10,
        minimum_hold_seconds=2,
    ) is None


def test_confirmed_soft_exit_requires_fee_covered_net_profit():
    assert protected_exit_reason(
        candidate_reason="profit liquidity target",
        candidate_confirmations=2,
        required_confirmations=2,
        current_gross_pct=0.12,
        peak_gross_pct=0.12,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.03,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=3,
        minimum_hold_seconds=2,
    ) == "profit liquidity target"


def test_liquidity_collapse_does_not_crystallise_a_noise_loss():
    assert protected_exit_reason(
        candidate_reason="defensive bid-liquidity collapse",
        candidate_confirmations=2,
        required_confirmations=2,
        current_gross_pct=-0.04,
        peak_gross_pct=0.02,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.03,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=1.5,
        minimum_hold_seconds=1,
    ) is None


def test_first_fee_covered_liquidity_anomaly_exits_immediately():
    assert protected_exit_reason(
        candidate_reason="defensive bid-liquidity collapse",
        candidate_confirmations=1,
        required_confirmations=3,
        current_gross_pct=0.12,
        peak_gross_pct=0.12,
        round_trip_fee_pct=0.10,
        break_even_buffer_pct=0.01,
        trailing_activation_pct=0.20,
        trailing_delta_pct=0.08,
        held_seconds=0.1,
        minimum_hold_seconds=2,
    ) == "profitable liquidity anomaly: defensive bid-liquidity collapse"


def test_armed_trailing_exit_prevents_winner_from_turning_into_loser():
    assert protected_exit_reason(
        candidate_reason=None,
        candidate_confirmations=0,
        required_confirmations=3,
        current_gross_pct=0.10,
        peak_gross_pct=0.20,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.03,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=1,
        minimum_hold_seconds=2,
    ) == "fee-covered trailing profit protection"


def test_fee_covered_retail_stop_target_can_exit_without_waiting():
    assert protected_exit_reason(
        candidate_reason="retail short-stop pool reached",
        candidate_confirmations=1,
        required_confirmations=1,
        current_gross_pct=0.14,
        peak_gross_pct=0.14,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.03,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=0.25,
        minimum_hold_seconds=2,
    ) == "retail short-stop pool reached"


def test_unfilled_shutdown_releases_margin_after_grace_period():
    empty_shutdown = {
        "status": "SHUTTING_DOWN",
        "is_trading": False,
        "filled_amount_quote": 0,
    }
    assert shutdown_blocks_entry(
        empty_shutdown, observed_seconds=10, grace_seconds=15
    ) is True
    assert shutdown_blocks_entry(
        empty_shutdown, observed_seconds=16, grace_seconds=15
    ) is False


def test_filled_shutdown_blocks_until_account_is_flat_and_grace_passes():
    filled_shutdown = {
        "status": "SHUTTING_DOWN",
        "is_trading": False,
        "filled_amount_quote": 100,
    }
    assert shutdown_blocks_entry(
        filled_shutdown,
        observed_seconds=600,
        grace_seconds=60,
        account_has_position=True,
    ) is True
    assert shutdown_blocks_entry(
        filled_shutdown,
        observed_seconds=61,
        grace_seconds=60,
        account_has_position=False,
    ) is False
