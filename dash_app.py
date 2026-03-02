from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional

import dash
import pandas as pd
from dash import ALL, Dash, Input, Output, State, dcc, html
import plotly.graph_objs as go

from order_data import (
    DEFAULT_INTERVAL_MINUTES,
    available_dates_from_serialized,
    counts_for_date_from_serialized,
    cumulative_notional_points,
    daily_totals_from_serialized,
    get_serialized_day_counts,
    get_latest_day_counts,
    latest_date_from_serialized,
)

LOG_DIR = Path(os.getenv("ORDER_LOG_DIR", "/home/jdlee/workspace/sgt/livesim/binance_us/log"))
REFRESH_INTERVAL_MS = 60_000
FAST_REFRESH_INTERVAL_MS = 2_000

def _preferred_counts(
    day_counts: Dict[str, Dict[str, Dict[str, int]]],
    fast_counts: Dict[str, Dict[str, Dict[str, int]]],
) -> Dict[str, Dict[str, Dict[str, int]]]:
    if day_counts:
        return day_counts
    return fast_counts or {}

app = Dash(__name__)
app.layout = html.Div(
    [
        html.H1("New orders per 10-minute bucket", style={"marginBottom": "0.5rem"}),
        html.Div(
            id="date-calendar",
            style={
                "display": "flex",
                "flexWrap": "wrap",
                "gap": "0.25rem",
                "marginBottom": "0.75rem",
                "alignItems": "center",
            },
        ),
        html.Div(
            [
                html.Button("← Previous day", id="prev-day", n_clicks=0, style={"flex": "0 0 auto"}),
                html.Div(id="date-label", style={"flex": "1", "textAlign": "center", "fontSize": "0.9rem"}),
                html.Button("Next day →", id="next-day", n_clicks=0, style={"flex": "0 0 auto"}),
            ],
            style={"display": "flex", "gap": "0.5rem", "marginBottom": "0.75rem"},
        ),
        dcc.Graph(id="new-orders-graph"),
        dcc.Graph(id="cumulative-notional-graph", style={"marginTop": "1rem"}),
        dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
        dcc.Interval(id="fast-refresh-interval", interval=FAST_REFRESH_INTERVAL_MS, n_intervals=0),
        dcc.Store(id="day-counts-store"),
        dcc.Store(id="fast-day-store"),
        dcc.Store(id="selected-date-store"),
        html.Div(id="log-source", style={"fontSize": "0.8rem", "color": "#777", "marginTop": "0.5rem"}),
    ],
    style={"fontFamily": "Inter, sans-serif", "maxWidth": "960px", "margin": "0 auto", "padding": "1rem"},
)


@app.callback(
    Output("day-counts-store", "data"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_day_counts(_: int) -> Dict[str, Dict[str, Dict[str, int]]]:
    try:
        return get_serialized_day_counts(LOG_DIR, interval_minutes=DEFAULT_INTERVAL_MINUTES)
    except FileNotFoundError:
        return {}


@app.callback(
    Output("fast-day-store", "data"),
    Input("fast-refresh-interval", "n_intervals"),
)
def refresh_fast_day_counts(_: int) -> Dict[str, Dict[str, Dict[str, int]]]:
    try:
        return get_latest_day_counts(LOG_DIR, interval_minutes=DEFAULT_INTERVAL_MINUTES)
    except FileNotFoundError:
        return {}


@app.callback(
    Output("selected-date-store", "data"),
    Output("prev-day", "disabled"),
    Output("next-day", "disabled"),
    Input("prev-day", "n_clicks"),
    Input("next-day", "n_clicks"),
    Input({"type": "date-box", "index": ALL}, "n_clicks"),
    Input("day-counts-store", "data"),
    Input("fast-day-store", "data"),
    State("selected-date-store", "data"),
)
def navigate_date(
    prev_clicks: int,
    next_clicks: int,
    _date_clicks: list[int],
    day_counts: Dict[str, Dict[str, Dict[str, int]]],
    fast_counts: Dict[str, Dict[str, Dict[str, int]]],
    current: Optional[str],
) -> tuple[Optional[str], bool, bool]:
    data = _preferred_counts(day_counts, fast_counts)
    dates = available_dates_from_serialized(data)
    if not dates:
        return None, True, True

    ctx = dash.callback_context
    triggered_prop_ids = [t["prop_id"].split(".")[0] for t in ctx.triggered] if ctx.triggered else []
    triggered_id = None
    for prop in triggered_prop_ids:
        if prop and prop != "day-counts-store":
            triggered_id = prop
            break

    if not current:
        current = latest_date_from_serialized(data)
    elif current not in dates and triggered_id != "fast-day-store":
        current = latest_date_from_serialized(data)

    if triggered_id and triggered_id.startswith("{"):
        payload = json.loads(triggered_id)
        if payload.get("type") == "date-box":
            potential = payload.get("index")
            if potential in dates:
                current = potential
    elif triggered_id == "prev-day":
        idx = dates.index(current)
        new_idx = max(0, idx - 1)
        current = dates[new_idx]
    elif triggered_id == "next-day":
        idx = dates.index(current)
        new_idx = min(len(dates) - 1, idx + 1)
        current = dates[new_idx]

    idx = dates.index(current)
    prev_disabled = idx == 0
    next_disabled = idx == len(dates) - 1

    return current, prev_disabled, next_disabled


def _calendar_color(value: int, minimum: int, maximum: int) -> str:
    if maximum <= minimum:
        ratio = 0.0
    else:
        ratio = (value - minimum) / (maximum - minimum)
    lightness = 90 - ratio * 55
    lightness = max(30, min(90, lightness))
    return f"hsl(214, 76%, {lightness:.1f}%)"


@app.callback(
    Output("date-calendar", "children"),
    Input("day-counts-store", "data"),
    Input("fast-day-store", "data"),
    State("selected-date-store", "data"),
)
def render_calendar(
    day_counts: Dict[str, Dict[str, Dict[str, int]]],
    fast_counts: Dict[str, Dict[str, Dict[str, int]]],
    selected_date: Optional[str],
) -> list[html.Button]:
    data = _preferred_counts(day_counts, fast_counts)
    dates = available_dates_from_serialized(data)
    if not dates:
        return [html.Div("No data yet", style={"fontSize": "0.9rem", "color": "#555"})]

    totals = daily_totals_from_serialized(data)
    values = [totals.get(date, 0) for date in dates]
    minimum = min(values)
    maximum = max(values)
    boxes: list[html.Button] = []
    for date in dates:
        count = totals.get(date, 0)
        color = _calendar_color(count, minimum, maximum)
        is_selected = date == selected_date
        label = pd.Timestamp(date).strftime("%b %d")
        boxes.append(
            html.Button(
                label,
                id={"type": "date-box", "index": date},
                n_clicks=0,
                title=f"{count} unique new orders",
                style={
                    "width": "65px",
                    "height": "40px",
                    "borderRadius": "6px",
                    "border": "2px solid #222" if is_selected else "1px solid #ccc",
                    "backgroundColor": color,
                    "color": "#111",
                    "fontSize": "0.85rem",
                    "fontWeight": "600" if is_selected else "400",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.15)" if is_selected else "none",
                },
            )
        )
    return boxes


@app.callback(
    Output("new-orders-graph", "figure"),
    Output("cumulative-notional-graph", "figure"),
    Output("log-source", "children"),
    Output("date-label", "children"),
    Input("day-counts-store", "data"),
    Input("fast-day-store", "data"),
    Input("selected-date-store", "data"),
)
def update_graph(
    day_counts: Dict[str, Dict[str, Dict[str, int]]],
    fast_counts: Dict[str, Dict[str, Dict[str, int]]],
    selected_date: Optional[str],
) -> tuple[go.Figure, go.Figure, str, str]:
    data = _preferred_counts(day_counts, fast_counts)
    if not data or not selected_date:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Waiting for data",
            xaxis_title="UTC time",
            yaxis_title="New orders",
        )
        empty_cum = go.Figure()
        empty_cum.update_layout(
            title="Cumulative filled notional",
            xaxis_title="First NEW timestamp (UTC)",
            yaxis_title="Cumulative notional",
        )
        return empty_fig, empty_cum, "No data available", "No date selected"

    new_counts = counts_for_date_from_serialized(
        data, selected_date, interval_minutes=DEFAULT_INTERVAL_MINUTES, metric="new"
    )
    fill_counts = counts_for_date_from_serialized(
        data, selected_date, interval_minutes=DEFAULT_INTERVAL_MINUTES, metric="fills"
    )
    start = new_counts.index[0]

    end = new_counts.index[-1] + pd.Timedelta(minutes=DEFAULT_INTERVAL_MINUTES)
    fig = go.Figure(
        data=[
            go.Scatter(x=new_counts.index, y=new_counts.values, mode="lines+markers", name="New orders"),
            go.Scatter(
                x=fill_counts.index,
                y=fill_counts.values,
                mode="lines",
                name="Fills",
                line=dict(dash="dash", color="#ef553b"),
            ),
        ]
    )
    fig.update_layout(
        title=f"New orders ({pd.Timestamp(selected_date).date().isoformat()})",
        xaxis_title="UTC time",
        yaxis_title="New orders",
        hovermode="x unified",
        margin=dict(t=60, b=40, l=40, r=20),
    )
    fig.update_xaxes(
        range=[start, end],
        rangeslider=dict(visible=True),
        showspikes=True,
        spikemode="across",
        tickformat="%H:%M",
    )
    total_new = int(new_counts.sum())
    total_fills = int(fill_counts.sum())
    caption = f"{total_new} unique new orders · {total_fills} fills on {pd.Timestamp(selected_date).date().isoformat()}"
    cum_points = cumulative_notional_points(data, selected_date)
    cum_fig = go.Figure()
    if cum_points:
        cum_x = [pd.Timestamp(point["bucket_iso"]) for point in cum_points]
        cum_y = [point["cumulative"] for point in cum_points]
        customdata = [
            [point["bucket_iso"], point["bucket_notional"], point["cumulative"]] for point in cum_points
        ]
        cum_fig.add_trace(
            go.Scatter(
                x=cum_x,
                y=cum_y,
                mode="lines+markers",
                name="Cumulative notional",
                line=dict(color="#00cc96"),
                customdata=customdata,
                hovertemplate=(
                    "Bucket %{customdata[0]}<br>"
                    "Bucket notional %{customdata[1]:,.2f}<br>"
                    "Cumulative %{customdata[2]:,.2f}<extra></extra>"
                ),
            )
        )
    cum_fig.update_layout(
        title="Cumulative filled notional",
        xaxis_title="First NEW timestamp (UTC)",
        yaxis_title="Cumulative notional",
        margin=dict(t=50, b=40, l=40, r=20),
    )
    label = pd.Timestamp(selected_date).strftime("%A, %B %d, %Y")
    return fig, cum_fig, caption, label


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
