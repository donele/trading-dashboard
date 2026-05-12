#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote

import dash
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html, no_update
from plotly.subplots import make_subplots

try:
    from trade_sim.app import SimData
except ImportError:  # pragma: no cover
    from app import SimData


ROOT_ORDER = ("dumpsim", "livesim", "tradesim")
ROOTS = [Path.home() / "workspace" / "sgt" / name for name in ROOT_ORDER]
DISCOVERY_WINDOW_HOURS: int | None = None
INDEX_SUMMARY_CACHE_PATH = Path("/tmp/trade_sim_index_summary_cache.json")
_INDEX_SUMMARY_CACHE: dict[str, dict[str, float]] | None = None
ALL_SYMBOLS_VALUE = "__all_symbols__"
ALL_DATES_VALUE = "__all_dates__"
ALL_HOURS_VALUE = "__all_hours__"


def normalize_head(head: str) -> Path | None:
    try:
        resolved = Path(head).expanduser().resolve(strict=True)
    except FileNotFoundError:
        return None
    for root in ROOTS:
        if not root.exists():
            continue
        try:
            resolved.relative_to(root.resolve())
            return resolved
        except ValueError:
            continue
    return None


def parse_state_filename(path: Path):
    parts = path.name.split(".")
    if len(parts) < 4 or parts[-1] != "parquet":
        return None
    date = parts[-2]
    if len(date) != 8 or not date.isdigit():
        return None
    symbol = ".".join(parts[:-3])
    if not symbol:
        return None
    return {"symbol": symbol, "date": date, "path": path}


def state_files_for_head(head: Path):
    state_dir = head / "log" / "state"
    if not state_dir.is_dir():
        return []
    rows = []
    for pq_path in state_dir.glob("*.parquet"):
        parsed = parse_state_filename(pq_path)
        if parsed is not None:
            rows.append(parsed)
    rows.sort(key=lambda x: (x["symbol"], x["date"]))
    return rows


def order_parquet_dates_for_head(head: Path) -> set[str]:
    log_dir = head / "log"
    if not log_dir.is_dir():
        return set()
    dates: set[str] = set()
    for path in log_dir.glob("order.????????.parquet"):
        parts = path.name.split(".")
        if len(parts) == 3 and parts[0] == "order" and parts[2] == "parquet":
            date = parts[1]
            if len(date) == 8 and date.isdigit():
                dates.add(date)
    return dates


def _head_last_update_ts(head: Path) -> float:
    log_dir = head / "log"
    mtimes: list[float] = []
    if log_dir.is_dir():
        for path in log_dir.glob("order.????????.parquet"):
            try:
                mtimes.append(path.stat().st_mtime)
            except FileNotFoundError:
                continue
        state_dir = log_dir / "state"
        if state_dir.is_dir():
            for path in state_dir.glob("*.parquet"):
                try:
                    mtimes.append(path.stat().st_mtime)
                except FileNotFoundError:
                    continue
    if mtimes:
        return max(mtimes)
    return head.stat().st_mtime


def discover_heads(window_hours: int | None = None) -> dict[str, list[Path]]:
    grouped: dict[str, list[Path]] = {name: [] for name in ROOT_ORDER}
    cutoff_ts: float | None = None
    if window_hours is not None:
        cutoff_ts = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).timestamp()
    for root_name, root in zip(ROOT_ORDER, ROOTS):
        if not root.exists():
            continue
        for log_dir in root.rglob("log"):
            if not log_dir.is_dir():
                continue
            state_dir = log_dir / "state"
            if not state_dir.is_dir():
                continue
            if not any(log_dir.glob("order.????????.parquet")):
                continue
            head = log_dir.parent
            if not state_files_for_head(head):
                continue
            if cutoff_ts is not None and _head_last_update_ts(head) < cutoff_ts:
                continue
            grouped[root_name].append(head)
        grouped[root_name].sort(
            key=lambda head: (
                max((row["date"] for row in state_files_for_head(head)), default=""),
                str(head),
            ),
            reverse=True,
        )
    return grouped


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(dtype=float)


def _head_symbols_for_nav(head: Path) -> list[str]:
    return sorted({row["symbol"] for row in state_files_for_head(head)})


def _head_dates_for_nav(head: Path, symbol: str | None = None) -> list[str]:
    rows = state_files_for_head(head)
    if symbol and symbol != ALL_SYMBOLS_VALUE:
        rows = [row for row in rows if row["symbol"] == symbol]
    return sorted({row["date"] for row in rows})


def _make_head_nav(
    head: Path,
    pathname: str,
    symbol: str | None,
    date: str | None,
    hour: str | None = None,
) -> html.Div:
    symbol_value = symbol if symbol else ALL_SYMBOLS_VALUE
    date_value = date if date else ALL_DATES_VALUE
    hour_value = hour if hour else ALL_HOURS_VALUE
    show_hour = pathname in ("/symbol", "/chart")
    symbol_options = [{"label": "All Symbols", "value": ALL_SYMBOLS_VALUE}]
    symbol_options.extend({"label": sym, "value": sym} for sym in _head_symbols_for_nav(head))
    date_options = [{"label": "All Dates", "value": ALL_DATES_VALUE}]
    date_options.extend({"label": d, "value": d} for d in _head_dates_for_nav(head, symbol))
    hour_options = [{"label": "All Hours", "value": ALL_HOURS_VALUE}]
    hour_options.extend({"label": f"{h:02d}", "value": f"{h:02d}"} for h in range(24))
    return html.Div(
        [
            html.Div(
                [
                    html.A("Home", href="/", style={"marginRight": "12px"}),
                    html.Span(str(head), style={"fontWeight": "600"}),
                ],
                style={"marginBottom": "8px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div("Symbol", style={"fontSize": "11px", "marginBottom": "2px"}),
                            dcc.Dropdown(
                                id="symbol-nav-dropdown",
                                options=symbol_options,
                                value=symbol_value,
                                clearable=False,
                                searchable=True,
                            ),
                        ],
                        style={"flex": "1 1 320px", "minWidth": "220px"},
                    ),
                    html.Div(
                        [
                            html.Div("Date", style={"fontSize": "11px", "marginBottom": "2px"}),
                            dcc.Dropdown(
                                id="date-nav-dropdown",
                                options=date_options,
                                value=date_value,
                                clearable=False,
                                searchable=True,
                            ),
                        ],
                        style={"flex": "1 1 320px", "minWidth": "220px"},
                    ),
                    html.Div(
                        [
                            html.Div("Hour", style={"fontSize": "11px", "marginBottom": "2px"}),
                            dcc.Dropdown(
                                id="hour-nav-dropdown",
                                options=hour_options,
                                value=hour_value,
                                clearable=False,
                                searchable=True,
                            ),
                        ],
                        style={
                            "flex": "0 1 220px",
                            "minWidth": "160px",
                            "display": "block" if show_hour else "none",
                        },
                    ),
                ],
                style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "alignItems": "flex-end"},
            ),
        ],
        style={"marginBottom": "12px"},
    )


def _nav_target(pathname: str | None, head: Path, symbol: str | None, date: str | None, hour: str | None = None) -> tuple[str, str]:
    head_q = quote(str(head))
    symbol_value = symbol if symbol else ALL_SYMBOLS_VALUE
    date_value = date if date else ALL_DATES_VALUE
    hour_value = hour if hour else ALL_HOURS_VALUE
    if date_value == ALL_DATES_VALUE:
        search = f"?head={head_q}"
        if symbol_value != ALL_SYMBOLS_VALUE:
            search += f"&symbol={quote(symbol_value)}"
        return "/stats", search
    if symbol_value == ALL_SYMBOLS_VALUE:
        return "/portfolio", f"?head={head_q}&date={quote(date_value)}"
    if pathname == "/chart":
        search = f"?head={head_q}&symbol={quote(symbol_value)}&date={quote(date_value)}"
        if hour_value != ALL_HOURS_VALUE:
            search += f"&hour={quote(hour_value)}"
        return "/chart", search
    if pathname == "/symbol":
        search = f"?head={head_q}&symbol={quote(symbol_value)}&date={quote(date_value)}"
        if hour_value != ALL_HOURS_VALUE:
            search += f"&hour={quote(hour_value)}"
        return "/symbol", search
    return "/symbol", f"?head={head_q}&symbol={quote(symbol_value)}&date={quote(date_value)}"


def _nav_href(pathname: str | None, head: Path, symbol: str | None, date: str | None, hour: str | None = None) -> str:
    path, search = _nav_target(pathname, head, symbol, date, hour)
    return f"{path}{search}"


def _format_num(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.{digits}f}"


def _format_usd(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"${float(value):,.{digits}f}"


def _format_pct(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def _format_bps(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 10000:.{digits}f} bps"


def _format_ratio(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    if value == float("inf"):
        return "inf"
    if value == float("-inf"):
        return "-inf"
    return f"{float(value):.{digits}f}"


def _return_series_stats(returns: pd.Series, periods_per_year: float = 1.0) -> dict[str, float]:
    clean = pd.to_numeric(returns, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna()
    if clean.empty:
        return {
            "volatility": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
        }
    mean = float(clean.mean())
    std = float(clean.std(ddof=0))
    downside = clean[clean < 0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    positive_sum = float(clean[clean > 0].sum())
    negative_sum = float(clean[clean < 0].sum())
    annualization = periods_per_year ** 0.5 if periods_per_year > 0 else 1.0
    return {
        "volatility": std,
        "sharpe": (mean / std) * annualization if std else 0.0,
        "sortino": (mean / downside_std) * annualization if downside_std else 0.0,
        "profit_factor": positive_sum / abs(negative_sum) if negative_sum < 0 else (float("inf") if positive_sum > 0 else 0.0),
        "win_rate": float((clean > 0).mean()),
    }


def _make_stats_table(rows: list[tuple[str, str]], *, compact: bool = False) -> html.Div:
    label_padding = "2px 4px 2px 0" if compact else "4px 6px 4px 0"
    value_padding = "2px 0 2px 4px" if compact else "4px 0 4px 6px"
    table_width = "100%" if compact else "min(560px, 100%)"
    return html.Div(
        html.Table(
            [
                html.Tbody(
                    [
                        html.Tr(
                            [
                                html.Th(label, style={"textAlign": "left", "padding": label_padding, "borderBottom": "1px solid #e5e7eb"}),
                                html.Td(value, style={"textAlign": "right", "padding": value_padding, "borderBottom": "1px solid #e5e7eb"}),
                            ]
                        )
                        for label, value in rows
                    ]
                )
            ],
            style={"width": "100%", "borderCollapse": "collapse"},
        ),
        style={"width": table_width, "marginBottom": "8px"},
    )


def _make_step_trace(
    x,
    y,
    name: str,
    color: str,
    showlegend: bool = False,
    legendgroup: str | None = None,
):
    return go.Scatter(
        x=x,
        y=y,
        mode="lines",
        name=name,
        line={"color": color, "width": 1.8},
        line_shape="hv",
        showlegend=showlegend,
        legendgroup=legendgroup,
        hovertemplate=f"{name}=%{{y}}<extra></extra>",
    )


def make_symbol_figure(simdata: SimData, symbol: str, sdate: str, freq: str = "5min") -> go.Figure:
    timeline = simdata.get_timeline(symbol, sdate, freq=freq)
    asymmetry = _buy_sell_notional_asymmetry(simdata, sdate, freq=freq, symbol=symbol, index=timeline.index)
    fig = make_subplots(
        rows=8,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=(
            "Size Traded",
            "Notional Traded",
            "Buy/Sell NTL Asymmetry",
            "Notional Position",
            "Mid",
            "PnL",
            "Fees",
            "Funding Rate",
        ),
    )
    series = [
        ("size_traded", "#0F766E"),
        ("notional_traded", "#2563EB"),
        ("buy_sell_ntl_asymmetry", "#111827"),
        ("notional_pos", "#C2410C"),
        ("mid", "#7C3AED"),
        ("pnl", "#1D4ED8"),
        ("fees", "#DC2626"),
        ("funding_rate", "#111827"),
    ]
    for row_idx, (col, color) in enumerate(series, start=1):
        if col == "buy_sell_ntl_asymmetry":
            if asymmetry.empty:
                continue
            fig.add_trace(_make_step_trace(asymmetry.index, asymmetry, col, color), row=row_idx, col=1)
            fig.update_yaxes(title=col, row=row_idx, col=1, range=[-1.05, 1.05], automargin=True)
            continue
        if timeline.empty or col not in timeline.columns:
            continue
        fig.add_trace(_make_step_trace(timeline.index, _safe_series(timeline, col), col, color), row=row_idx, col=1)
        fig.update_yaxes(title=col, row=row_idx, col=1, automargin=True)
    fig.update_xaxes(title="Time", row=8, col=1)
    fig.update_layout(
        template="plotly_white",
        height=1280,
        title=f"{symbol} | {sdate}",
        hovermode="x unified",
        margin={"l": 18, "r": 18, "t": 70, "b": 40},
    )
    return fig


def make_portfolio_figure(simdata: SimData, sdate: str, freq: str = "5min") -> go.Figure:
    timelines = simdata.get_timelines(sdate, freq=freq)
    timeline_index = _union_timeline_index(timelines.values())
    asymmetry = _buy_sell_notional_asymmetry(simdata, sdate, freq=freq, index=timeline_index)
    palette = [
        "#2563EB",
        "#0F766E",
        "#C2410C",
        "#7C3AED",
        "#0891B2",
        "#65A30D",
        "#DB2777",
        "#EA580C",
        "#4F46E5",
        "#6B7280",
    ]
    symbol_colors = {symbol: palette[idx % len(palette)] for idx, symbol in enumerate(sorted(timelines.keys()))}
    seen_symbols: set[str] = set()
    fig = make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=("Notional Traded", "Buy/Sell NTL Asymmetry", "Notional Position", "PnL", "Funding Rate"),
    )
    series = [
        ("notional_traded", True),
        ("buy_sell_ntl_asymmetry", False),
        ("notional_pos", True),
        ("pnl", True),
        ("funding_rate", False),
    ]
    for row_idx, (col, show_total) in enumerate(series, start=1):
        if col == "buy_sell_ntl_asymmetry":
            if not asymmetry.empty:
                fig.add_trace(
                    _make_step_trace(asymmetry.index, asymmetry, "total", "#111827", showlegend=False),
                    row=row_idx,
                    col=1,
                )
            fig.update_yaxes(title=col, row=row_idx, col=1, range=[-1.05, 1.05], automargin=True)
            continue
        total_series = []
        for symbol, tl in timelines.items():
            if tl.empty or col not in tl.columns:
                continue
            fig.add_trace(
                _make_step_trace(
                    tl.index,
                    _safe_series(tl, col),
                    symbol,
                    symbol_colors.get(symbol, "#2563EB"),
                    showlegend=symbol not in seen_symbols,
                    legendgroup=symbol,
                ),
                row=row_idx,
                col=1,
            )
            seen_symbols.add(symbol)
            total_series.append(_safe_series(tl, col))
        if show_total and total_series:
            total = pd.concat(total_series, axis=1).sum(axis=1)
            fig.add_trace(
                go.Scatter(
                    x=total.index,
                    y=total,
                    mode="lines",
                    name="total",
                    line={"color": "#111827", "width": 0.8},
                    line_shape="hv",
                    hovertemplate="total=%{y}<extra></extra>",
                    showlegend=False,
                ),
                row=row_idx,
                col=1,
            )
        fig.update_yaxes(title=col, row=row_idx, col=1, automargin=True)
    fig.update_xaxes(title="Time", row=5, col=1)
    fig.update_layout(
        template="plotly_white",
        height=1180,
        title=f"Portfolio | {sdate}",
        hovermode="x unified",
        margin={"l": 18, "r": 18, "t": 70, "b": 40},
    )
    return fig


def _union_timeline_index(timelines) -> pd.Index:
    indexes = [tl.index for tl in timelines if not tl.empty]
    if not indexes:
        return pd.Index([])
    values = indexes[0]
    for index in indexes[1:]:
        values = values.union(index)
    return values.sort_values()


def _empty_asymmetry(index: pd.Index | None = None) -> pd.Series:
    if index is None:
        return pd.Series(dtype=float, name="buy_sell_ntl_asymmetry")
    return pd.Series(0.0, index=index, name="buy_sell_ntl_asymmetry")


def _buy_sell_notional_asymmetry(
    simdata: SimData,
    sdate: str,
    *,
    freq: str = "5min",
    symbol: str | None = None,
    index: pd.Index | None = None,
) -> pd.Series:
    simdata.load_order(sdate)
    if simdata.dfo is None:
        return _empty_asymmetry(index)

    t1 = pd.to_datetime(sdate, format="%Y%m%d")
    t2 = t1 + pd.Timedelta(days=1)
    try:
        if symbol is None:
            orders = simdata.dfo.reset_index()
        else:
            orders = simdata.dfo.loc[symbol].reset_index()
            orders["symbol"] = symbol
    except KeyError:
        return _empty_asymmetry(index)

    required = {"filled_qty", "price", "side", "symbol"}
    if orders.empty or not required.issubset(orders.columns):
        return _empty_asymmetry(index)

    if "last_update_time" in orders.columns:
        fill_time = pd.to_datetime(pd.to_numeric(orders["last_update_time"], errors="coerce"), unit="us")
    elif "create_time" in orders.columns:
        fill_time = pd.to_datetime(pd.to_numeric(orders["create_time"], errors="coerce"), unit="us")
    elif "create_datetime" in orders.columns:
        fill_time = pd.to_datetime(orders["create_datetime"], errors="coerce")
    else:
        return _empty_asymmetry(index)

    orders = orders.copy()
    orders["fill_time"] = fill_time
    filled_qty = pd.to_numeric(orders["filled_qty"], errors="coerce").fillna(0.0)
    orders = orders[(filled_qty > 0) & (orders["fill_time"] >= t1) & (orders["fill_time"] < t2)]
    if orders.empty:
        return _empty_asymmetry(index)

    orders["side_bucket"] = orders["side"].map(_side_bucket)
    orders = orders[orders["side_bucket"].isin(["BUY", "SELL"])].copy()
    if orders.empty:
        return _empty_asymmetry(index)

    price = pd.to_numeric(orders["price"], errors="coerce")
    filled_qty = pd.to_numeric(orders["filled_qty"], errors="coerce")
    multiplier = pd.to_numeric(orders["symbol"].map(simdata.multiplier_map), errors="coerce")
    orders["filled_ntl"] = price * filled_qty * multiplier
    orders = orders.dropna(subset=["fill_time", "filled_ntl"]).set_index("fill_time").sort_index()
    if orders.empty:
        return _empty_asymmetry(index)

    buy = orders.loc[orders["side_bucket"] == "BUY", "filled_ntl"].resample(freq, label="right").sum()
    sell = orders.loc[orders["side_bucket"] == "SELL", "filled_ntl"].resample(freq, label="right").sum()
    by_side = pd.concat([buy.rename("buy"), sell.rename("sell")], axis=1).fillna(0.0)
    denominator = by_side["buy"] + by_side["sell"]
    asymmetry = ((by_side["buy"] - by_side["sell"]) / denominator.where(denominator != 0)).fillna(0.0)
    asymmetry.name = "buy_sell_ntl_asymmetry"
    if index is not None:
        asymmetry = asymmetry.reindex(index).fillna(0.0)
        asymmetry.name = "buy_sell_ntl_asymmetry"
    return asymmetry


def _side_bucket(raw: object) -> str | None:
    if raw is None:
        return None
    value = str(raw).upper()
    if value in {"BID", "BUY"}:
        return "BUY"
    if value in {"ASK", "SELL"}:
        return "SELL"
    return None


def _build_order_segment_frame(df: pd.DataFrame, *, side: str) -> pd.DataFrame:
    if df.empty or "create_time" not in df.columns or "last_update_time" not in df.columns or "price" not in df.columns:
        return pd.DataFrame(columns=["start_time", "end_time", "price", "order_ntl"])
    create_time = pd.to_numeric(df["create_time"], errors="coerce")
    last_update_time = pd.to_numeric(df["last_update_time"], errors="coerce")
    timepoints = pd.Index(pd.concat([create_time.dropna(), last_update_time.dropna()]).sort_values().unique())
    if len(timepoints) < 2:
        return pd.DataFrame(columns=["start_time", "end_time", "price", "order_ntl"])
    prices = pd.to_numeric(df["price"], errors="coerce")
    if "order_ntl" in df.columns:
        order_ntl = pd.to_numeric(df["order_ntl"], errors="coerce").fillna(0.0)
    else:
        order_ntl = pd.Series(0.0, index=df.index)
    segments = pd.DataFrame({"start_time": timepoints[:-1], "end_time": timepoints[1:]})

    def _segment_price(row) -> float | None:
        active = (create_time.reindex(df.index) <= row.start_time) & (last_update_time.reindex(df.index) >= row.end_time)
        active_prices = prices[active].dropna()
        if active_prices.empty:
            return None
        return float(active_prices.max() if side == "BUY" else active_prices.min())

    def _segment_ntl(row) -> float:
        active = (create_time.reindex(df.index) <= row.start_time) & (last_update_time.reindex(df.index) >= row.end_time)
        return float(order_ntl[active].sum())

    segments["price"] = segments.apply(_segment_price, axis=1)
    segments["order_ntl"] = segments.apply(_segment_ntl, axis=1)
    return segments.dropna(subset=["price"])


def _make_order_segment_trace(df: pd.DataFrame, *, side: str, color: str) -> go.Scatter:
    xs = []
    ys = []
    ntls = []
    if not df.empty:
        for row in df.itertuples(index=False):
            start_ts = getattr(row, "start_time", None)
            end_ts = getattr(row, "end_time", None)
            price = getattr(row, "price", None)
            order_ntl = getattr(row, "order_ntl", None)
            if start_ts is None or end_ts is None or price is None:
                continue
            try:
                x0 = pd.to_datetime(int(start_ts), unit="us")
                x1 = pd.to_datetime(int(end_ts), unit="us")
                y = float(price)
                ntl = float(order_ntl) if order_ntl is not None else float("nan")
            except (TypeError, ValueError):
                continue
            if x1 <= x0:
                x1 = x0 + timedelta(microseconds=1)
            xs.extend([x0, x1, None])
            ys.extend([y, y, None])
            ntls.extend([ntl, ntl, None])
    return go.Scatter(
        x=xs,
        y=ys,
        customdata=ntls,
        mode="lines",
        name=f"{side.lower()} order",
        line={"color": color, "width": 1.8},
        showlegend=True,
        hovertemplate=f"{side.lower()} order=%{{y}}<br>order_ntl=%{{customdata:,.2f}}<extra></extra>",
    )


def _raw_hour_state_frame(simdata: SimData, symbol: str, sdate: str, hour: int) -> pd.DataFrame:
    simdata.load_state(symbol, sdate)
    t1 = pd.to_datetime(sdate, format="%Y%m%d").replace(hour=hour)
    t2 = t1 + timedelta(hours=1)
    if simdata.dfs is None:
        return pd.DataFrame(columns=["bid", "ask", "notional_pos"])
    try:
        df_state = simdata.dfs.loc[symbol]
    except KeyError:
        return pd.DataFrame(columns=["bid", "ask", "notional_pos"])
    df_state = df_state[(df_state.index >= t1) & (df_state.index < t2)]
    return df_state


def _hour_state_series(df_state: pd.DataFrame, col: str, *, side: str | None = None) -> pd.Series:
    if col in df_state.columns:
        series = _safe_series(df_state, col)
        if col in {"min_net_bid", "net_bid", "max_net_ask", "net_ask"}:
            series = series.mask(series.abs() >= 1e8)
        return series
    if col == "max_buy_ntl":
        qty = _safe_series(df_state, "buy_top_qty")
        px = _safe_series(df_state, "buy_top_px")
        mult = _safe_series(df_state, "contract_multiplier")
        if mult.empty:
            mult = pd.Series(1.0, index=df_state.index)
        return qty * px * mult
    if col == "max_sell_ntl":
        qty = _safe_series(df_state, "sell_top_qty")
        px = _safe_series(df_state, "sell_top_px")
        mult = _safe_series(df_state, "contract_multiplier")
        if mult.empty:
            mult = pd.Series(1.0, index=df_state.index)
        return qty * px * mult
    if col in {"bid_ask_price", "bid_order_price", "ask_order_price"}:
        return _safe_series(df_state, col)
    return pd.Series(dtype=float, index=df_state.index)


def make_hour_figure(simdata: SimData, symbol: str, sdate: str, hour: int) -> go.Figure:
    dfo, dfbidask = simdata.get_orders_bid_ask(symbol, sdate, hour)
    df_state = _raw_hour_state_frame(simdata, symbol, sdate, hour)
    t1 = pd.to_datetime(sdate, format="%Y%m%d").replace(hour=hour)
    t2 = t1 + timedelta(hours=1)
    fig = make_subplots(
        rows=8,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[2.2, 1, 1, 1, 1, 1, 1, 1],
    )
    has_data = False

    if not dfbidask.empty:
        fig.add_trace(
            go.Scatter(
                x=dfbidask.index,
                y=_safe_series(dfbidask, "bid"),
                mode="lines",
                name="bid",
                line={"color": "#2563EB", "width": 1.2},
                opacity=0.35,
                hovertemplate="bid=%{y}<extra></extra>",
            ),
            row=1,
            col=1,
        )
        has_data = True
        fig.add_trace(
            go.Scatter(
                x=dfbidask.index,
                y=_safe_series(dfbidask, "ask"),
                mode="lines",
                name="ask",
                line={"color": "#DC2626", "width": 1.2},
                opacity=0.35,
                hovertemplate="ask=%{y}<extra></extra>",
            ),
            row=1,
            col=1,
        )
        has_data = True

    if not dfo.empty:
        order_df = dfo.reset_index()
        if "side" in order_df.columns:
            order_df["side_bucket"] = order_df["side"].map(_side_bucket)
        else:
            order_df["side_bucket"] = None
        buy_df = order_df[order_df["side_bucket"] == "BUY"]
        sell_df = order_df[order_df["side_bucket"] == "SELL"]
        buy_segments = _build_order_segment_frame(buy_df, side="BUY")
        sell_segments = _build_order_segment_frame(sell_df, side="SELL")
        if not buy_segments.empty:
            fig.add_trace(_make_order_segment_trace(buy_segments, side="BUY", color="#0F766E"), row=1, col=1)
            has_data = True
        if not sell_segments.empty:
            fig.add_trace(_make_order_segment_trace(sell_segments, side="SELL", color="#C2410C"), row=1, col=1)
            has_data = True

        if "filled_qty" in order_df.columns:
            filled_qty = pd.to_numeric(order_df["filled_qty"], errors="coerce").fillna(0)
            fills = order_df[filled_qty > 0].copy()
        else:
            fills = pd.DataFrame(columns=order_df.columns)
        if not fills.empty:
            if "last_update_time" in fills.columns:
                fills["fill_time"] = pd.to_datetime(pd.to_numeric(fills["last_update_time"], errors="coerce"), unit="us")
            elif "create_time" in fills.columns:
                fills["fill_time"] = pd.to_datetime(pd.to_numeric(fills["create_time"], errors="coerce"), unit="us")
            else:
                fills["fill_time"] = pd.NaT
            buy_fills = fills[fills["side_bucket"] == "BUY"]
            sell_fills = fills[fills["side_bucket"] == "SELL"]
            if not buy_fills.empty:
                fig.add_trace(
                    go.Scatter(
                        x=buy_fills["fill_time"],
                        y=pd.to_numeric(buy_fills["price"], errors="coerce"),
                        mode="markers",
                        name="buy fill",
                        marker={"symbol": "triangle-up", "color": "#1D4ED8", "size": 9},
                        hovertemplate="buy fill=%{y}<extra></extra>",
                    ),
                    row=1,
                    col=1,
                )
                has_data = True
            if not sell_fills.empty:
                fig.add_trace(
                    go.Scatter(
                        x=sell_fills["fill_time"],
                        y=pd.to_numeric(sell_fills["price"], errors="coerce"),
                        mode="markers",
                        name="sell fill",
                        marker={"symbol": "triangle-down", "color": "#DC2626", "size": 9},
                        hovertemplate="sell fill=%{y}<extra></extra>",
                    ),
                    row=1,
                    col=1,
                )
                has_data = True

    def _add_state_trace(col: str, row: int, color: str, label: str | None = None) -> None:
        nonlocal has_data
        if df_state.empty and col not in {"max_buy_ntl", "max_sell_ntl"}:
            return
        series = _hour_state_series(df_state, col)
        if series.empty:
            return
        fig.add_trace(
            go.Scatter(
                x=df_state.index,
                y=series,
                mode="lines",
                name=label or col,
                line={"color": color, "width": 1.4},
                line_shape="hv",
                hovertemplate=f"{label or col}=%{{y}}<extra></extra>",
            ),
            row=row,
            col=1,
        )
        has_data = True

    _add_state_trace("notional_pos", 2, "#6D28D9", "notional position")
    _add_state_trace("pnl", 3, "#7C3AED", "pnl")
    _add_state_trace("min_net_bid", 4, "#9CA3AF", "min net bid")
    _add_state_trace("net_bid", 4, "#2563EB", "net bid")
    _add_state_trace("bid_ask_price", 4, "#14B8A6", "bid/ask price")
    _add_state_trace("bid_order_price", 4, "#0EA5E9", "bid order price")
    _add_state_trace("max_net_ask", 5, "#F97316", "max net ask")
    _add_state_trace("net_ask", 5, "#DC2626", "net ask")
    _add_state_trace("ask_order_price", 5, "#A855F7", "ask order price")
    _add_state_trace("max_buy_ntl", 6, "#0F766E", "max buy ntl")
    _add_state_trace("buy_size", 6, "#22C55E", "buy size")
    _add_state_trace("bid_order_ntl", 6, "#1D4ED8", "bid order ntl")
    _add_state_trace("max_sell_ntl", 7, "#C2410C", "max sell ntl")
    _add_state_trace("sell_size", 7, "#F59E0B", "sell size")
    _add_state_trace("ask_order_ntl", 7, "#7C3AED", "ask order ntl")
    _add_state_trace("bid_order_size", 8, "#16A34A", "buy order size")
    _add_state_trace("ask_order_size", 8, "#DC2626", "ask order size")

    if not has_data:
        fig.add_annotation(
            text="No data for this hour",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font={"size": 14, "color": "#6B7280"},
        )

    fig.update_xaxes(range=[t1, t2], title="Time", row=8, col=1)
    fig.update_yaxes(title="Price", automargin=True, row=1, col=1)
    fig.update_yaxes(title="Notional Position", automargin=True, row=2, col=1)
    fig.update_yaxes(title="PnL", automargin=True, row=3, col=1)
    fig.update_yaxes(title="Net Bid", automargin=True, row=4, col=1)
    fig.update_yaxes(title="Net Ask", automargin=True, row=5, col=1)
    fig.update_yaxes(title="Buy NTL", automargin=True, row=6, col=1)
    fig.update_yaxes(title="Sell NTL", automargin=True, row=7, col=1)
    fig.update_yaxes(title="Order Size", automargin=True, row=8, col=1)
    fig.update_layout(
        template="plotly_white",
        height=2210,
        title=f"{symbol} | {sdate} | {hour:02d}:00",
        hovermode="x unified",
        margin={"l": 18, "r": 18, "t": 70, "b": 40},
    )
    return fig


def _page_shell(title: str, body) -> html.Div:
    return html.Div(
        [
            html.H2(title, style={"margin": "0 0 10px 0"}),
            body,
        ],
        style={"maxWidth": "1500px", "margin": "0 auto", "padding": "16px"},
    )


def _daily_symbol_summary(simdata: SimData, symbol: str) -> pd.DataFrame:
    rows = []
    for date in simdata.sdates:
        timeline = simdata.get_timeline(symbol, date)
        if timeline.empty:
            continue
        final = timeline.iloc[-1]
        rows.append(
            {
                "date": date,
                "pnl": float(pd.to_numeric(final.get("pnl"), errors="coerce") or 0.0),
                "notional_traded": float(pd.to_numeric(final.get("notional_traded"), errors="coerce") or 0.0),
                "notional_pos": float(pd.to_numeric(final.get("notional_pos"), errors="coerce") or 0.0),
                "fees": float(pd.to_numeric(final.get("fees"), errors="coerce") or 0.0),
                "size_traded": float(pd.to_numeric(final.get("size_traded"), errors="coerce") or 0.0),
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True) if rows else pd.DataFrame()


def _daily_portfolio_summary(simdata: SimData) -> pd.DataFrame:
    rows = []
    for date in simdata.sdates:
        timelines = simdata.get_timelines(date)
        if not timelines:
            continue
        totals: dict[str, float] = {"notional_traded": 0.0, "notional_pos": 0.0, "pnl": 0.0, "fees": 0.0, "size_traded": 0.0}
        for timeline in timelines.values():
            if timeline.empty:
                continue
            final = timeline.iloc[-1]
            for col in totals:
                value = pd.to_numeric(final.get(col), errors="coerce")
                if pd.notna(value):
                    totals[col] += float(value)
        rows.append({"date": date, **totals})
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True) if rows else pd.DataFrame()


def _load_index_summary_cache() -> dict[str, dict[str, float]]:
    global _INDEX_SUMMARY_CACHE
    if _INDEX_SUMMARY_CACHE is not None:
        return _INDEX_SUMMARY_CACHE
    try:
        raw = json.loads(INDEX_SUMMARY_CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            _INDEX_SUMMARY_CACHE = {
                str(k): v for k, v in raw.items() if isinstance(v, dict)
            }
        else:
            _INDEX_SUMMARY_CACHE = {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        _INDEX_SUMMARY_CACHE = {}
    return _INDEX_SUMMARY_CACHE


def _save_index_summary_cache() -> None:
    cache = _load_index_summary_cache()
    try:
        INDEX_SUMMARY_CACHE_PATH.write_text(json.dumps(cache), encoding="utf-8")
    except OSError:
        pass


def _head_index_summary(head: Path) -> tuple[float, float]:
    head_str = str(head)
    cache = _load_index_summary_cache()
    cache_row = cache.get(head_str)
    head_mtime = _head_last_update_ts(head)
    if cache_row is not None:
        cached_mtime = float(cache_row.get("last_update_ts", -1.0))
        if cached_mtime == head_mtime:
            return float(cache_row.get("total_notional", 0.0)), float(cache_row.get("pnl_bps", 0.0))
    simdata = SimData(head_str)
    summary_df = _daily_portfolio_summary(simdata)
    if summary_df.empty:
        total_notional, pnl_bps = 0.0, 0.0
    else:
        total_notional = float(pd.to_numeric(summary_df["notional_traded"], errors="coerce").fillna(0.0).sum())
        total_pnl = float(pd.to_numeric(summary_df["pnl"], errors="coerce").fillna(0.0).sum())
        pnl_bps = (total_pnl / total_notional) if total_notional else 0.0
    cache[head_str] = {
        "last_update_ts": head_mtime,
        "total_notional": total_notional,
        "pnl_bps": pnl_bps,
    }
    _save_index_summary_cache()
    return total_notional, pnl_bps


def _stats_bucket_minutes(simdata: SimData) -> int:
    return max(5, 5 * max(1, len(simdata.sdates)))


def _combined_stats_timeline(simdata: SimData, symbol: str | None, freq_minutes: int) -> pd.DataFrame:
    frames = []
    pnl_offset = 0.0
    if symbol:
        for date in simdata.sdates:
            timeline = simdata.get_timeline(symbol, date, freq="5min")
            if not timeline.empty:
                timeline = timeline.copy()
                if "pnl" in timeline.columns:
                    pnl_series = pd.to_numeric(timeline["pnl"], errors="coerce").fillna(0.0)
                    timeline["pnl"] = pnl_offset + pnl_series
                    pnl_offset = float(timeline["pnl"].iloc[-1])
                frames.append(timeline)
    else:
        for date in simdata.sdates:
            timelines = simdata.get_timelines(date, freq="5min")
            if not timelines:
                continue
            totals = []
            for timeline in timelines.values():
                if not timeline.empty:
                    totals.append(timeline)
            if totals:
                combined_day = pd.concat(totals).sort_index()
                if combined_day.index.has_duplicates:
                    combined_day = combined_day.groupby(level=0).sum(numeric_only=True)
                if "pnl" in combined_day.columns:
                    combined_day = combined_day.copy()
                    pnl_series = pd.to_numeric(combined_day["pnl"], errors="coerce").fillna(0.0)
                    combined_day["pnl"] = pnl_offset + pnl_series
                    pnl_offset = float(combined_day["pnl"].iloc[-1])
                frames.append(combined_day)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames).sort_index()
    if combined.index.has_duplicates:
        combined = combined.groupby(level=0).last()
    freq = f"{freq_minutes}min"
    # Use the first sample in each bucket so the timestamp labels reflect
    # bucket-start values on the step chart, especially across day resets.
    combined = combined.resample(freq).first().ffill()
    return combined


def _interval_pnl_series(simdata: SimData, symbol: str | None, freq_minutes: int) -> pd.Series:
    pnl_deltas = []
    freq = f"{freq_minutes}min"
    for date in simdata.sdates:
        if symbol:
            timeline = simdata.get_timeline(symbol, date, freq=freq)
        else:
            timelines = simdata.get_timelines(date, freq=freq)
            frames = [tl for tl in timelines.values() if not tl.empty]
            if not frames:
                continue
            timeline = pd.concat(frames).sort_index()
            if timeline.index.has_duplicates:
                timeline = timeline.groupby(level=0).sum(numeric_only=True)
        if timeline.empty:
            continue
        pnl = pd.to_numeric(timeline.get("pnl"), errors="coerce").fillna(0.0)
        pnl_delta = pnl.diff().fillna(pnl.iloc[0])
        pnl_deltas.append(pnl_delta)
    if not pnl_deltas:
        return pd.Series(dtype=float)
    return pd.concat(pnl_deltas)


def _make_stats_figure(timeline_df: pd.DataFrame, title: str) -> go.Figure:
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("PnL", "Notional Traded", "Notional Position"),
    )
    if not timeline_df.empty:
        x = timeline_df.index
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pd.to_numeric(timeline_df["pnl"], errors="coerce"),
                mode="lines",
                name="pnl",
                line={"color": "#2563EB", "width": 1.5},
                line_shape="hv",
                hovertemplate="pnl=%{y}<extra></extra>",
                showlegend=False,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pd.to_numeric(timeline_df["notional_traded"], errors="coerce"),
                mode="lines",
                name="notional traded",
                line={"color": "#C2410C", "width": 1.5},
                line_shape="hv",
                hovertemplate="notional_traded=%{y}<extra></extra>",
                showlegend=False,
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pd.to_numeric(timeline_df["notional_pos"], errors="coerce"),
                mode="lines",
                name="notional position",
                line={"color": "#0F766E", "width": 1.5},
                line_shape="hv",
                hovertemplate="notional_pos=%{y}<extra></extra>",
                showlegend=False,
            ),
            row=3,
            col=1,
        )
    fig.update_layout(
        template="plotly_white",
        height=780,
        title=title,
        hovermode="x unified",
        margin={"l": 18, "r": 18, "t": 70, "b": 40},
    )
    fig.update_xaxes(title="Date", row=3, col=1)
    for row_idx in (1, 2, 3):
        fig.update_yaxes(automargin=True, row=row_idx, col=1)
    return fig


def render_stats(search: str) -> html.Div:
    query = parse_qs(search.lstrip("?"))
    head, symbol, _ = _resolve_head_symbol_date(query, require_symbol=False)
    if head is None:
        return _page_shell("Invalid head", html.Pre(unquote(query.get("head", [""])[0])))
    simdata = SimData(str(head))
    if symbol:
        summary_df = _daily_symbol_summary(simdata, symbol)
        bucket_minutes = _stats_bucket_minutes(simdata)
        chart_df = _combined_stats_timeline(simdata, symbol, bucket_minutes)
        title = f"Quant Stats | {symbol} | {head}"
    else:
        summary_df = _daily_portfolio_summary(simdata)
        bucket_minutes = _stats_bucket_minutes(simdata)
        chart_df = _combined_stats_timeline(simdata, None, bucket_minutes)
        title = f"Portfolio Quant Stats | {head}"
    if summary_df.empty:
        return _page_shell("No data", html.Pre(symbol or str(head)))
    summary_df = summary_df.copy()
    summary_df["date_ts"] = pd.to_datetime(summary_df["date"], format="%Y%m%d", errors="coerce")
    summary_df["cum_pnl"] = pd.to_numeric(summary_df["pnl"], errors="coerce").fillna(0.0).cumsum()
    summary_df["drawdown"] = summary_df["cum_pnl"] - summary_df["cum_pnl"].cummax()
    summary_df["abs_pos"] = pd.to_numeric(summary_df["notional_pos"], errors="coerce").abs()
    fig = _make_stats_figure(chart_df, title)
    pnl_series = pd.to_numeric(summary_df["pnl"], errors="coerce").fillna(0.0)
    notional_series = pd.to_numeric(summary_df["notional_traded"], errors="coerce").fillna(0.0)
    fees_series = pd.to_numeric(summary_df["fees"], errors="coerce").fillna(0.0)
    interval_pnl = _interval_pnl_series(simdata, symbol, bucket_minutes)
    periods_per_year = (365.0 * 24.0 * 60.0) / float(bucket_minutes)
    return_stats = _return_series_stats(interval_pnl, periods_per_year=periods_per_year)
    total_pnl = float(pnl_series.sum())
    total_notional = float(notional_series.sum())
    total_fees = float(fees_series.sum())
    total_return = float(total_pnl / total_notional) if total_notional else 0.0
    total_fees_bps = float(total_fees / total_notional) if total_notional else 0.0
    avg_return = total_return
    max_drawdown = float(summary_df["drawdown"].min()) if not summary_df.empty else 0.0
    max_abs_position = float(summary_df["abs_pos"].max()) if not summary_df.empty else 0.0
    stats_table = _make_stats_table(
        [
            ("Days", str(len(summary_df))),
            ("Total Notional", _format_usd(total_notional)),
            ("Total PnL", _format_usd(total_pnl)),
            ("PnL", _format_bps(total_return)),
            ("Total Fees", _format_usd(total_fees)),
            ("Fees", _format_bps(total_fees_bps)),
            ("Sharpe (annualized)", _format_ratio(return_stats["sharpe"])),
            ("Sortino (annualized)", _format_ratio(return_stats["sortino"])),
            (f"Profit Factor ({bucket_minutes}m)", _format_ratio(return_stats["profit_factor"])),
            (f"Volatility ({bucket_minutes}m)", _format_num(return_stats["volatility"], digits=4)),
            (f"Win Rate ({bucket_minutes}m)", _format_pct(return_stats["win_rate"])),
            ("Avg Return", _format_bps(avg_return)),
            ("Max Drawdown", _format_usd(max_drawdown)),
            ("Max Abs Position", _format_usd(max_abs_position)),
        ],
        compact=True,
    )
    table_rows = []
    for row in summary_df.itertuples(index=False):
        table_rows.append(
            html.Tr(
                [
                    html.Td(row.date),
                    html.Td(f"{float(row.pnl):,.2f}"),
                    html.Td(f"{float(row.notional_traded):,.2f}"),
                    html.Td(f"{float(row.notional_pos):,.2f}"),
                    html.Td(f"{float(row.fees):,.2f}"),
                ]
            )
        )
    table = html.Table(
        [
            html.Thead(html.Tr([html.Th("Date"), html.Th("PnL"), html.Th("Notional Traded"), html.Th("Notional Pos"), html.Th("Fees")])),
            html.Tbody(table_rows),
        ],
        style={"width": "100%", "borderCollapse": "collapse", "marginTop": "8px", "fontSize": "11px"},
    )
    head_nav = _make_head_nav(head, "/stats", symbol if symbol else None, ALL_DATES_VALUE)
    sidebar = html.Div(
        [stats_table],
        style={
            "flex": "0 0 320px",
            "maxWidth": "320px",
            "minWidth": "280px",
            "fontSize": "11px",
            "lineHeight": "1.15",
        },
    )
    main_panel = html.Div(
        [dcc.Graph(figure=fig, style={"height": "calc(100vh - 140px)"})],
        style={"flex": "1 1 auto", "minWidth": "0"},
    )
    return _page_shell(
        title,
        html.Div(
            [
                head_nav,
                html.Div(
                    [sidebar, main_panel],
                    style={"display": "flex", "gap": "12px", "alignItems": "flex-start"},
                ),
                html.Div(table, style={"marginTop": "10px"}),
            ]
        ),
    )


def render_index() -> html.Div:
    grouped = discover_heads(window_hours=DISCOVERY_WINDOW_HOURS)
    all_heads = [head for heads in grouped.values() for head in heads]
    common_prefix: Path | None = None
    if all_heads:
        try:
            common_prefix = Path(os.path.commonpath([str(head) for head in all_heads]))
        except ValueError:
            common_prefix = None
    blocks = []
    for root_name, heads in grouped.items():
        if not heads:
            continue
        head_cards = []
        for head in heads:
            files = state_files_for_head(head)
            order_dates = order_parquet_dates_for_head(head)
            total_notional, pnl_bps = _head_index_summary(head)
            by_symbol: dict[str, list[str]] = defaultdict(list)
            for row in files:
                if row["date"] in order_dates:
                    by_symbol[row["symbol"]].append(row["date"])
            all_dates = sorted({row["date"] for row in files if row["date"] in order_dates})
            head_cards.append(
                html.Div(
                    [
                        html.Div(
                            [
                                html.H4(
                                    html.A(
                                        str(head.relative_to(common_prefix)) if common_prefix is not None and head.is_relative_to(common_prefix) else str(head),
                                        href=f"/stats?head={quote(str(head))}",
                                        style={"color": "#111827", "textDecoration": "none"},
                                    ),
                                    style={"margin": 0, "display": "inline-block", "marginRight": "12px"},
                                ),
                                html.Div(
                                    [
                                        html.Span(f"NTL: {_format_usd(total_notional)}", style={"marginRight": "12px"}),
                                        html.Span(f"PnL: {_format_bps(pnl_bps)}", style={"marginRight": "12px"}),
                                        *[
                                            html.A(
                                                d,
                                                href=f"/portfolio?head={quote(str(head))}&date={d}",
                                                style={"marginRight": "8px"},
                                            )
                                            for d in all_dates
                                        ],
                                    ],
                                    style={"display": "inline-block", "whiteSpace": "nowrap"},
                                ),
                            ],
                            style={"marginBottom": "4px"},
                        ),
                    ],
                    style={"marginBottom": "6px"},
                )
            )
        blocks.append(html.Div([html.H3(root_name), *head_cards]))
    return _page_shell("Sim Dashboard v2", html.Div(blocks))


def _resolve_head_symbol_date(query: dict[str, list[str]], require_symbol: bool = True):
    head_raw = unquote(query.get("head", [""])[0])
    symbol = unquote(query.get("symbol", [""])[0])
    date = query.get("date", [""])[0]
    head = normalize_head(head_raw)
    if head is None:
        return None, symbol, date
    if require_symbol and not symbol:
        return head, symbol, date
    return head, symbol, date


def render_portfolio(search: str) -> html.Div:
    query = parse_qs(search.lstrip("?"))
    head, _, date = _resolve_head_symbol_date(query, require_symbol=False)
    if head is None:
        return _page_shell("Invalid head", html.Pre(unquote(query.get("head", [""])[0])))
    simdata = SimData(str(head))
    if not date:
        if not simdata.sdates:
            return _page_shell("No dates", html.Pre(str(head)))
        date = simdata.sdates[-1]
    fig = make_portfolio_figure(simdata, date)
    head_nav = _make_head_nav(head, "/portfolio", None, date)
    return _page_shell(
        f"Portfolio | {head}",
        html.Div(
            [
                head_nav,
                dcc.Graph(figure=fig),
            ]
        ),
    )


def render_symbol(search: str) -> html.Div:
    query = parse_qs(search.lstrip("?"))
    head, symbol, date = _resolve_head_symbol_date(query)
    if head is None:
        return _page_shell("Invalid head", html.Pre(unquote(query.get("head", [""])[0])))
    if not symbol:
        return _page_shell("Missing symbol", html.Pre(search))
    state_entries = [row for row in state_files_for_head(head) if row["symbol"] == symbol]
    if not state_entries:
        return _page_shell("No dates found", html.Pre(symbol))
    available_dates = sorted({row["date"] for row in state_entries})
    if not date:
        date = available_dates[-1]
    simdata = SimData(str(head))
    hour = query.get("hour", [""])[0]
    if hour and hour.isdigit():
        hour_i = max(0, min(23, int(hour)))
    else:
        hour_i = None
    head_nav = _make_head_nav(head, "/symbol", symbol, date, hour=hour if hour else None)
    if hour_i is None:
        symbol_fig = make_symbol_figure(simdata, symbol, date)
        body = html.Div([head_nav, dcc.Graph(figure=symbol_fig, style={"height": "86vh"})])
        return _page_shell(f"{symbol} | {date} | {head}", body)
    hour_fig = make_hour_figure(simdata, symbol, date, hour_i)
    body = html.Div([head_nav, dcc.Graph(figure=hour_fig, style={"height": "86vh"})])
    return _page_shell(f"{symbol} | {date} | {hour_i:02d}:00 | {head}", body)


def render_chart(search: str) -> html.Div:
    query = parse_qs(search.lstrip("?"))
    head, symbol, date = _resolve_head_symbol_date(query)
    if head is None:
        return _page_shell("Invalid head", html.Pre(unquote(query.get("head", [""])[0])))
    if not symbol or not date:
        return _page_shell("Missing symbol/date", html.Pre(search))
    hour = query.get("hour", [""])[0]
    hour_i = int(hour) if hour.isdigit() else 0
    hour_i = max(0, min(23, hour_i))
    simdata = SimData(str(head))
    fig = make_hour_figure(simdata, symbol, date, hour_i)
    head_nav = _make_head_nav(head, "/chart", symbol, date, hour=hour)
    return _page_shell(
        f"{symbol} | {date} | {hour_i:02d}:00 | {head}",
        html.Div(
            [
                head_nav,
                dcc.Graph(figure=fig, style={"height": "78vh"}),
            ]
        ),
    )


app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Sim Dashboard v2"
app.layout = html.Div([dcc.Location(id="url"), html.Div(id="page-content")])


@app.callback(
    Output("url", "href"),
    Input("symbol-nav-dropdown", "value"),
    Input("date-nav-dropdown", "value"),
    Input("hour-nav-dropdown", "value"),
    State("url", "pathname"),
    State("url", "search"),
    prevent_initial_call=True,
)
def navigate_from_dropdowns(symbol_value, date_value, hour_value, pathname: str | None, search: str | None):
    search = search or ""
    if pathname in (None, "/", ""):
        return no_update
    query = parse_qs(search.lstrip("?"))
    head, _, _ = _resolve_head_symbol_date(query, require_symbol=False)
    if head is None:
        return no_update
    target_symbol = None if symbol_value in (None, ALL_SYMBOLS_VALUE) else str(symbol_value)
    target_date = None if date_value in (None, ALL_DATES_VALUE) else str(date_value)
    if hour_value in (None, ALL_HOURS_VALUE):
        target_hour = None
    else:
        target_hour = str(hour_value)
    target_href = _nav_href(pathname, head, target_symbol, target_date, target_hour)
    if target_href == f"{pathname}{search}":
        return no_update
    return target_href


@app.callback(Output("page-content", "children"), Input("url", "pathname"), Input("url", "search"))
def route(pathname: str | None, search: str | None):
    search = search or ""
    if pathname in (None, "/", ""):
        return render_index()
    if pathname == "/stats":
        return render_stats(search)
    if pathname == "/portfolio":
        return render_portfolio(search)
    if pathname == "/symbol":
        return render_symbol(search)
    if pathname == "/chart":
        return render_chart(search)
    return render_index()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade simulation dashboard")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d", action="store_true", help="Show heads updated in last 24 hours")
    group.add_argument("-w", action="store_true", help="Show heads updated in last 7 days")
    group.add_argument("-m", action="store_true", help="Show heads updated in last 30 days")
    return parser.parse_args()


def main() -> int:
    global DISCOVERY_WINDOW_HOURS
    args = parse_args()
    if args.d:
        DISCOVERY_WINDOW_HOURS = 24
    elif args.w:
        DISCOVERY_WINDOW_HOURS = 24 * 7
    elif args.m:
        DISCOVERY_WINDOW_HOURS = 24 * 30
    else:
        DISCOVERY_WINDOW_HOURS = None
    host = os.getenv("DASH2_HOST", "127.0.0.1")
    port = int(os.getenv("DASH2_PORT", "8050"))
    debug = os.getenv("DASH2_DEBUG", "1").lower() in ("1", "true", "yes", "on")
    app.run(debug=debug, host=host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
