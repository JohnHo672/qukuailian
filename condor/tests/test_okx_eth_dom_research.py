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
    account_position_tracker_key,
    active_reserved_margin,
    adaptive_market_signal,
    classify_market_regime,
    dom_signal,
    dynamic_barriers,
    depth_limited_margin,
    detect_retail_stop_run,
    entry_location_is_valid,
    executor_direction,
    executor_filled_base_amount,
    executor_lifecycle,
    executor_strategy,
    executor_submission_status,
    favorable_move_pct,
    fee_covered_trailing_barrier,
    infer_dom_intent,
    liquidity_void_direction,
    losing_trade_exit_reason,
    market_depth_metrics,
    matched_executor_close_amount,
    maker_close_price,
    maker_entry_price,
    maker_only_executor_barrier_kwargs,
    one_shot_position_margin,
    orderflow_structure,
    orphan_order_ids_to_cancel,
    pair_active_order_rows,
    pair_position_rows,
    partial_maker_exit_should_retry,
    pending_entry_should_expire,
    protected_exit_reason,
    quantize_close_amount,
    range_reversion_signal,
    shutdown_blocks_entry,
    should_exit,
    strategy_adjusted_barriers,
    strategy_loss_limits,
    volume_price_synchronization,
)


def test_config_is_demo_eth_only_with_one_twenty_percent_position():
    config = Config()
    assert config.connector_name == "okx_perpetual_demo"
    assert config.trading_pair == "ETH-USDT"
    assert config.leverage == 10
    assert config.position_margin_pct == 20
    assert config.max_positions == 1
    assert config.min_seconds_between_entries == 1
    assert config.decision_interval_ms == 100
    assert config.unfilled_shutdown_grace_seconds == 2
    assert config.min_executable_position_amount_base == 0.001
    assert config.entry_timeout_seconds == 1
    assert config.exit_cooldown_seconds == 1
    assert config.flat_confirmation_seconds == 2
    assert config.max_entry_extension_bps == 8
    assert config.volume_sync_window_seconds == 2
    assert config.min_volume_acceleration_ratio == 1.05
    assert config.min_price_sync_bps == 0.25
    assert config.breakout_volume_ratio == 1.30
    assert config.min_range_width_bps == 12
    assert config.min_stop_pct == 0.05
    assert config.early_invalidation_stop_pct == 0.06
    assert config.max_stop_pct == 0.10
    assert ENTRY_ORDER_TYPE == 3
    assert TAKE_PROFIT_ORDER_TYPE == 3
    assert RISK_EXIT_ORDER_TYPE == 3
    assert maker_only_executor_barrier_kwargs() == {"open_order_type": 3}
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


def test_executor_submission_status_never_labels_backend_errors_as_submitted():
    accepted, detail = executor_submission_status(
        {"error": "Invalid executor config: stop loss must be MARKET"}
    )
    assert accepted is False
    assert detail == "Invalid executor config: stop loss must be MARKET"

    accepted, detail = executor_submission_status({"executor_id": "exec-123"})
    assert accepted is True
    assert detail == "exec-123"

    accepted, detail = executor_submission_status({})
    assert accepted is False
    assert detail == "execution API returned no executor_id"


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


def test_free_ten_millisecond_bbo_overlays_the_deeper_book():
    state = DomState(
        bids={1999.0: 10.0, 1998.0: 5.0},
        asks={2001.0: 10.0, 2002.0: 5.0},
    )
    state.apply_bbo(
        {
            "data": [
                {
                    "bids": [["1999.5", "4"]],
                    "asks": [["2000.5", "6"]],
                }
            ]
        }
    )
    frame = state.book_frame()
    assert frame is not None
    assert frame.best_bid == 1999.5
    assert frame.best_ask == 2000.5
    assert frame.mid == 2000.0


def test_profitable_close_rests_on_the_passive_side():
    frame = BookFrame(time.time(), 2000, 1999.5, 2000.5, 0, 2000)
    assert maker_close_price("LONG", frame) == 2000.5
    assert maker_close_price("SHORT", frame) == 1999.5


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
        depth={
            "depth_imbalance_10bps": 0.20,
            "bid_depth_25bps": 1000,
            "ask_depth_25bps": 1000,
            "bid_depth_concentration": 0.60,
            "ask_depth_concentration": 0.20,
        },
        sweep={"long_reclaim": False, "short_reclaim": False},
        vwap=1999.2,
        poc=1999.5,
        val=1998.5,
        vah=2001,
        trend="UP",
        volume_price={"direction": "UP"},
        config=config,
    )
    assert result["direction"] == "LONG"
    assert result["score"] == 10


def test_dom_entry_rejects_signal_without_directional_liquidity_void():
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
        depth={
            "depth_imbalance_10bps": 0.20,
            "bid_depth_25bps": 1000,
            "ask_depth_25bps": 1000,
            "bid_depth_concentration": 0.60,
            "ask_depth_concentration": 0.60,
        },
        sweep={"long_reclaim": False, "short_reclaim": False},
        vwap=1999.2,
        poc=1999.5,
        val=1998.5,
        vah=2001,
        trend="UP",
        volume_price={"direction": "UP"},
        config=config,
    )
    assert result["direction"] == "FLAT"


def test_dom_entry_rejects_price_volume_divergence():
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
        depth={
            "depth_imbalance_10bps": 0.20,
            "bid_depth_25bps": 1000,
            "ask_depth_25bps": 1000,
            "bid_depth_concentration": 0.60,
            "ask_depth_concentration": 0.20,
        },
        sweep={"long_reclaim": False, "short_reclaim": False},
        vwap=1999.2,
        poc=1999.5,
        val=1998.5,
        vah=2001,
        trend="UP",
        volume_price={"direction": "FLAT"},
        config=config,
    )
    assert result["direction"] == "FLAT"


def test_volume_price_sync_requires_price_move_volume_expansion_and_taker_delta():
    now = 100.0
    trades = deque()
    for index in range(4):
        trades.append(
            {
                "time": 96.2 + index * 0.4,
                "price": 1999.8 + index * 0.02,
                "size": 1.0,
                "side": "sell" if index == 0 else "buy",
            }
        )
    for index in range(8):
        trades.append(
            {
                "time": 98.2 + index * 0.2,
                "price": 2000.2 + index * 0.04,
                "size": 1.0,
                "side": "buy" if index < 7 else "sell",
            }
        )
    sync = volume_price_synchronization(
        trades,
        now=now,
        window_seconds=2,
        min_volume_ratio=1.05,
        min_price_bps=0.25,
    )
    assert sync["direction"] == "UP"
    assert sync["price_change_bps"] > 0.25
    assert sync["volume_ratio"] > 1.05
    assert sync["signed_delta"] > 0

    no_expansion = volume_price_synchronization(
        deque(list(trades)[:4] + list(trades)[4:6]),
        now=now,
        window_seconds=2,
        min_volume_ratio=1.05,
        min_price_bps=0.25,
    )
    assert no_expansion["direction"] == "FLAT"


def test_entry_location_rejects_chasing_beyond_vwap_and_profile():
    assert entry_location_is_valid(
        "LONG",
        mid=2000,
        vwap=1999.2,
        poc=1999.5,
        val=1998.5,
        vah=2001,
        max_extension_bps=8,
    ) is True
    assert entry_location_is_valid(
        "LONG",
        mid=2004,
        vwap=1999.2,
        poc=1999.5,
        val=1998.5,
        vah=2001,
        max_extension_bps=8,
    ) is False
    assert entry_location_is_valid(
        "SHORT",
        mid=2000,
        vwap=2000.8,
        poc=2000.5,
        val=1999,
        vah=2001.5,
        max_extension_bps=8,
    ) is True


def test_maker_entry_uses_nearest_structure_and_never_crosses():
    frame = BookFrame(time.time(), 2000, 1999.9, 2000.1, 0, 2000)
    rules = {"min_price_increment": 0.1}
    assert maker_entry_price(
        "LONG",
        frame=frame,
        vwap=1999.2,
        poc=1999.7,
        val=1998.8,
        vah=2001.2,
        intent={"bid_wall_price": 1999.76},
        rules=rules,
        max_offset_bps=8,
    ) == 1999.7
    assert maker_entry_price(
        "SHORT",
        frame=frame,
        vwap=2000.8,
        poc=2000.3,
        val=1999,
        vah=2001.2,
        intent={"ask_wall_price": 2000.24},
        rules=rules,
        max_offset_bps=8,
    ) == 2000.3


def test_market_regime_routes_breakout_trend_range_and_wait():
    config = Config()
    frame = BookFrame(time.time(), 2000.5, 2000.4, 2000.6, 0.2, 2000.55)
    breakout = classify_market_regime(
        frame=frame,
        depth={"depth_imbalance_10bps": 0.20},
        structure={"trend": "UP", "val": 1998, "vah": 2000},
        volume_price={"direction": "UP", "volume_ratio": 1.5},
        liquidity_void="UP",
        config=config,
    )
    assert breakout["regime"] == "BREAKOUT_UP"
    assert breakout["strategy"] == "BREAKOUT_RETEST"

    trend = classify_market_regime(
        frame=BookFrame(time.time(), 1999.5, 1999.4, 1999.6, 0.2, 1999.55),
        depth={"depth_imbalance_10bps": 0.20},
        structure={"trend": "UP", "val": 1998, "vah": 2000},
        volume_price={"direction": "UP", "volume_ratio": 1.1},
        liquidity_void="UP",
        config=config,
    )
    assert trend["strategy"] == "TREND_PULLBACK"

    ranged = classify_market_regime(
        frame=frame,
        depth={"depth_imbalance_10bps": 0},
        structure={"trend": "FLAT", "val": 1998, "vah": 2001},
        volume_price={"direction": "FLAT", "volume_ratio": 0.9},
        liquidity_void="NONE",
        config=config,
    )
    assert ranged["strategy"] == "VP_EDGE_REVERSION"

    wait = classify_market_regime(
        frame=frame,
        depth={"depth_imbalance_10bps": 0},
        structure={"trend": "UP", "val": 1998, "vah": 2001},
        volume_price={"direction": "DOWN", "volume_ratio": 1.5},
        liquidity_void="NONE",
        config=config,
    )
    assert wait["strategy"] == "WAIT"


def test_range_strategy_only_enters_at_defended_profile_edge():
    config = Config()
    frame = BookFrame(time.time(), 1998.1, 1998.0, 1998.2, 0.2, 1998.15)
    signal = range_reversion_signal(
        frame=frame,
        flow={"buy_ratio": 0.60, "sell_ratio": 0.40},
        intent={"bid_intent": 0.75, "bid_spoof_risk": 0.1},
        structure={"val": 1998, "vah": 2001},
        volume_price={"direction": "FLAT"},
        config=config,
    )
    assert signal["direction"] == "LONG"
    away_from_edge = range_reversion_signal(
        frame=BookFrame(time.time(), 1999.5, 1999.4, 1999.6, 0.2, 1999.55),
        flow={"buy_ratio": 0.60, "sell_ratio": 0.40},
        intent={"bid_intent": 0.75, "bid_spoof_risk": 0.1},
        structure={"val": 1998, "vah": 2001},
        volume_price={"direction": "FLAT"},
        config=config,
    )
    assert away_from_edge["direction"] == "FLAT"


def test_adaptive_signal_selects_range_strategy_and_tighter_risk():
    config = Config()
    frame = BookFrame(time.time(), 1998.1, 1998.0, 1998.2, 0.2, 1998.15)
    signal = adaptive_market_signal(
        frame=frame,
        flow={"buy_ratio": 0.60, "sell_ratio": 0.40},
        intent={"bid_intent": 0.75, "bid_spoof_risk": 0.1},
        depth={
            "depth_imbalance_10bps": 0,
            "bid_depth_25bps": 1000,
            "ask_depth_25bps": 1000,
            "bid_depth_concentration": 0.6,
            "ask_depth_concentration": 0.6,
        },
        sweep={"long_reclaim": False, "short_reclaim": False},
        structure={
            "trend": "FLAT",
            "vwap": 1999.5,
            "poc": 1999.5,
            "val": 1998,
            "vah": 2001,
        },
        volume_price={"direction": "FLAT", "volume_ratio": 0.9},
        config=config,
    )
    assert signal["direction"] == "LONG"
    assert signal["strategy"] == "VP_EDGE_REVERSION"
    assert strategy_loss_limits("VP_EDGE_REVERSION", config) == (0.04, 0.06)
    adjusted = strategy_adjusted_barriers(
        "VP_EDGE_REVERSION",
        {"stop_loss_pct": 0.10, "take_profit_pct": 0.50},
        config,
    )
    assert adjusted == {"stop_loss_pct": 0.06, "take_profit_pct": 0.30}


def test_liquidity_void_direction_requires_one_thin_side_only():
    base = {"bid_depth_25bps": 1000, "ask_depth_25bps": 1000}
    assert liquidity_void_direction(
        {**base, "bid_depth_concentration": 0.60, "ask_depth_concentration": 0.20},
        max_near_concentration=0.35,
    ) == "UP"
    assert liquidity_void_direction(
        {**base, "bid_depth_concentration": 0.20, "ask_depth_concentration": 0.60},
        max_near_concentration=0.35,
    ) == "DOWN"
    assert liquidity_void_direction(
        {**base, "bid_depth_concentration": 0.20, "ask_depth_concentration": 0.20},
        max_near_concentration=0.35,
    ) == "NONE"
    assert liquidity_void_direction(
        {**base, "bid_depth_concentration": 0.30, "ask_depth_concentration": 0.20},
        max_near_concentration=0.35,
    ) == "UP"


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


def test_account_position_identity_ignores_demo_amount_jitter():
    first = {"side": "SHORT", "amount": -0.1058, "entry_price": 2682.14}
    later = {"side": "SHORT", "amount": -0.1059, "entry_price": 2682.14}
    assert account_position_tracker_key(first) == account_position_tracker_key(later)


def test_all_exchange_closeable_residuals_block_the_next_trade():
    payload = {
        "data": [
            {
                "trading_pair": "ETH-USDT",
                "amount": -0.001,
                "entry_price": 2700,
            }
        ]
    }
    assert pair_position_rows(payload, min_amount_base=0.001) == payload["data"]


def test_only_sub_minimum_exchange_dust_is_non_blocking():
    payload = {
        "data": [
            {
                "trading_pair": "ETH-USDT",
                "amount": -0.0005,
                "entry_price": 2700,
            }
        ]
    }
    assert pair_position_rows(payload, min_amount_base=0.001) == []


def test_close_amount_is_quantized_and_tolerates_minimum_lot_jitter():
    rules = {
        "min_order_size": 0.001,
        "min_base_amount_increment": 0.001,
        "min_notional_size": 0,
        "min_order_value": 0,
    }
    assert quantize_close_amount(
        base_amount=0.016005, price=2700, rules=rules
    ) == 0.016
    assert quantize_close_amount(
        base_amount=0.0009995, price=2700, rules=rules
    ) == 0.001


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


def test_flat_account_cancels_unowned_close_remainder_once():
    orders = [{"client_order_id": "close-1"}, {"client_order_id": "close-2"}]
    assert orphan_order_ids_to_cancel(
        account_positions=[],
        blocking_executors=[],
        active_orders=orders,
        already_requested={"close-1"},
    ) == ["close-2"]
    assert orphan_order_ids_to_cancel(
        account_positions=[{"amount": 1}],
        blocking_executors=[],
        active_orders=orders,
        already_requested=set(),
    ) == []
    assert orphan_order_ids_to_cancel(
        account_positions=[],
        blocking_executors=[{"status": "RUNNING"}],
        active_orders=orders,
        already_requested=set(),
    ) == []


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
    assert 0.05 <= barriers["stop_loss_pct"] <= 0.10
    assert 0.18 <= barriers["take_profit_pct"] <= 0.60
    assert barriers["take_profit_pct"] >= barriers["stop_loss_pct"] * 1.20


def test_orderflow_structure_builds_vwap_vp_and_direction_without_candles():
    rising = [
        {
            "time": 61 + index,
            "price": 2000 + index * 0.1,
            "size": 1,
            "side": "buy",
        }
        for index in range(40)
    ]
    falling = [
        {
            "time": 61 + index,
            "price": 2004 - index * 0.1,
            "size": 1,
            "side": "sell",
        }
        for index in range(40)
    ]
    up = orderflow_structure(rising, mid=2004.1, now=101)
    down = orderflow_structure(falling, mid=1999.9, now=101)
    assert up["trend"] == "UP"
    assert down["trend"] == "DOWN"
    assert up["vwap"] > 0
    assert up["val"] <= up["poc"] <= up["vah"]


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


def test_executor_strategy_recovers_encoded_regime_playbook():
    assert executor_strategy(
        {"config": {"level_id": "eth-dom-breakout-retest"}}
    ) == "BREAKOUT_RETEST"
    assert executor_strategy(
        {"config": {"level_id": "eth-dom-vp-edge-reversion"}}
    ) == "VP_EDGE_REVERSION"
    assert executor_strategy({"config": {"level_id": "eth-dom-trend-pullback"}}) == "TREND_PULLBACK"


def test_close_amount_uses_actual_matching_fill_not_jittered_position():
    row = {
        "config": {"side": 2, "amount": 48.0, "entry_price": 2691.93},
        "custom_info": {
            "current_position_average_price": 2691.93,
            "held_position_orders": [
                {"position": "OPEN", "executed_amount_base": "47.592"}
            ],
        },
    }
    position = {"side": "SHORT", "amount": -48.0, "entry_price": 2691.93}
    assert executor_filled_base_amount(row) == 47.592
    assert matched_executor_close_amount(position, [row]) == 47.592


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


def test_fee_covered_scalp_exits_without_waiting_for_anomaly():
    assert protected_exit_reason(
        candidate_reason=None,
        candidate_confirmations=0,
        required_confirmations=2,
        current_gross_pct=0.085,
        peak_gross_pct=0.085,
        round_trip_fee_pct=0.07,
        break_even_buffer_pct=0.01,
        trailing_activation_pct=0.19,
        trailing_delta_pct=0.09,
        held_seconds=0.5,
        minimum_hold_seconds=0.5,
    ) == "fee-covered scalp target"


def test_partial_maker_exit_retries_only_after_position_amount_decreases():
    assert partial_maker_exit_should_retry(
        submitted_amount=48.494,
        remaining_amount=0.016,
        has_active_order=False,
    ) is True
    assert partial_maker_exit_should_retry(
        submitted_amount=48.494,
        remaining_amount=48.494,
        has_active_order=False,
    ) is False
    assert partial_maker_exit_should_retry(
        submitted_amount=48.494,
        remaining_amount=0.016,
        has_active_order=True,
    ) is False


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


def test_losing_trade_exits_early_only_after_structural_invalidation():
    assert losing_trade_exit_reason(
        candidate_reason="defensive bid-liquidity collapse",
        current_gross_pct=-0.061,
        early_invalidation_stop_pct=0.06,
        max_stop_pct=0.10,
    ) == "early liquidity invalidation stop"
    assert losing_trade_exit_reason(
        candidate_reason=None,
        current_gross_pct=-0.061,
        early_invalidation_stop_pct=0.06,
        max_stop_pct=0.10,
    ) is None
    assert losing_trade_exit_reason(
        candidate_reason=None,
        current_gross_pct=-0.101,
        early_invalidation_stop_pct=0.06,
        max_stop_pct=0.10,
    ) == "defensive account hard-stop"


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
