from dashboard.shm_dash_app import StreamState, _ts_to_plot_datetime, _window_points, _window_trades


def test_stream_state_symbol_cache_only_changes_for_new_symbols() -> None:
    state = StreamState(window_sec=60.0, retention_sec=600.0)

    state.on_event(
        {
            "kind": "trade",
            "symbol": "BTCUSDT",
            "exchange_time": 1_000_000,
            "price": 100.0,
            "qty": 1.0,
            "seq": 1,
        }
    )
    snap = state.snapshot_symbol(None)
    assert snap["symbol_version"] == 1
    assert snap["symbols"] == ("BTCUSDT",)
    assert snap["symbol_options"] == ({"label": "BTCUSDT", "value": "BTCUSDT"},)

    state.on_event(
        {
            "kind": "trade",
            "symbol": "BTCUSDT",
            "exchange_time": 2_000_000,
            "price": 101.0,
            "qty": 2.0,
            "seq": 2,
        }
    )
    snap = state.snapshot_symbol("BTCUSDT")
    assert snap["symbol_version"] == 1
    assert snap["selected"] == "BTCUSDT"

    state.on_event(
        {
            "kind": "trade",
            "symbol": "ETHUSDT",
            "exchange_time": 3_000_000,
            "price": 102.0,
            "qty": 3.0,
            "seq": 3,
        }
    )
    snap = state.snapshot_symbol("BTCUSDT")
    assert snap["symbol_version"] == 2
    assert snap["symbols"] == ("BTCUSDT", "ETHUSDT")


def test_snapshot_controls_tracks_selection_without_copying_series() -> None:
    state = StreamState(window_sec=60.0, retention_sec=600.0)

    state.on_event(
        {
            "kind": "trade",
            "symbol": "ETHUSDT",
            "exchange_time": 1_000_000,
            "price": 100.0,
            "qty": 1.0,
            "seq": 1,
        }
    )
    state.on_event(
        {
            "kind": "trade",
            "symbol": "BTCUSDT",
            "exchange_time": 2_000_000,
            "price": 101.0,
            "qty": 2.0,
            "seq": 2,
        }
    )

    snap = state.snapshot_controls("ETHUSDT")
    assert snap["selected"] == "ETHUSDT"
    assert snap["symbols"] == ("BTCUSDT", "ETHUSDT")
    assert "series" not in snap

    snap = state.snapshot_controls("DOGEUSDT")
    assert snap["selected"] == "BTCUSDT"


def test_window_helpers_slice_tail_without_scanning_full_prefix() -> None:
    ts = [1.0, 2.0, 3.0, 4.0]
    px = [10.0, 20.0, 30.0, 40.0]
    qty = [100.0, 200.0, 300.0, 400.0]

    assert _window_points(ts, px, 2.5) == ([3.0, 4.0], [30.0, 40.0])
    assert _window_trades(ts, px, qty, 3.0) == ([3.0, 4.0], [30.0, 40.0], [300.0, 400.0])


def test_plot_timestamps_use_utc_across_midnight_boundary() -> None:
    before_midnight = _ts_to_plot_datetime(1_712_102_399.0)
    after_midnight = _ts_to_plot_datetime(1_712_102_401.0)

    assert before_midnight.isoformat() == "2024-04-02T23:59:59+00:00"
    assert after_midnight.isoformat() == "2024-04-03T00:00:01+00:00"
