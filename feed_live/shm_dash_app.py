#!/usr/bin/env python3
"""Live Dash app for SGT SHM market-data monitoring."""

from __future__ import annotations

import argparse
import atexit
import logging
from bisect import bisect_left
import json
import math
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

from dash import Dash, Input, Output, State, dcc, html, no_update
import plotly.graph_objects as go
from plotly.subplots import make_subplots


@dataclass
class SymbolSeries:
    bid_ts: Deque[float] = field(default_factory=deque)
    bid_px: Deque[float] = field(default_factory=deque)
    ask_ts: Deque[float] = field(default_factory=deque)
    ask_px: Deque[float] = field(default_factory=deque)
    trade_ts: Deque[float] = field(default_factory=deque)
    trade_px: Deque[float] = field(default_factory=deque)
    trade_qty: Deque[float] = field(default_factory=deque)
    last_kind: str = ""
    last_px: float = float("nan")
    last_seq: int = -1


class StreamState:
    def __init__(self, window_sec: float, retention_sec: float) -> None:
        self._lock = threading.Lock()
        self.window_sec = window_sec
        self.retention_sec = retention_sec
        self.symbol_to_series: Dict[str, SymbolSeries] = {}
        self.symbols_sorted: Tuple[str, ...] = ()
        self.symbol_options: Tuple[dict, ...] = ()
        self._symbol_cache_dirty = False
        self.symbol_version = 0
        self.last_event_time = 0.0
        self.events_total = 0

    def _refresh_symbol_cache_locked(self) -> None:
        if not self._symbol_cache_dirty:
            return
        self.symbols_sorted = tuple(sorted(self.symbol_to_series.keys()))
        self.symbol_options = tuple({"label": sym, "value": sym} for sym in self.symbols_sorted)
        self._symbol_cache_dirty = False

    def _get(self, symbol: str) -> SymbolSeries:
        if symbol not in self.symbol_to_series:
            self.symbol_to_series[symbol] = SymbolSeries()
            self._symbol_cache_dirty = True
            self.symbol_version += 1
        return self.symbol_to_series[symbol]

    def _selected_symbol_locked(self, selected: Optional[str]) -> Optional[str]:
        self._refresh_symbol_cache_locked()
        if selected in self.symbol_to_series:
            return selected
        if self.symbols_sorted:
            return self.symbols_sorted[0]
        return None

    @staticmethod
    def _copy_series(series: SymbolSeries) -> dict:
        return {
            "bid_ts": list(series.bid_ts),
            "bid_px": list(series.bid_px),
            "ask_ts": list(series.ask_ts),
            "ask_px": list(series.ask_px),
            "trade_ts": list(series.trade_ts),
            "trade_px": list(series.trade_px),
            "trade_qty": list(series.trade_qty),
            "last_kind": series.last_kind,
            "last_px": series.last_px,
            "last_seq": series.last_seq,
        }

    @staticmethod
    def _prune(ts: Deque[float], ys: Deque[float], cutoff: float) -> None:
        while ts and ts[0] < cutoff:
            ts.popleft()
            ys.popleft()

    def on_event(self, evt: dict) -> None:
        symbol = str(evt.get("symbol", evt.get("symbol_id", "UNKNOWN")))
        exch_us = int(evt.get("exchange_time", 0))
        ts = exch_us / 1_000_000.0 if exch_us else time.time()
        kind = str(evt.get("kind", ""))
        prune_cutoff = ts - self.retention_sec

        with self._lock:
            series = self._get(symbol)
            self.events_total += 1
            self.last_event_time = time.time()
            series.last_kind = kind
            series.last_seq = int(evt.get("seq", -1))

            if kind == "book_ticker":
                bid = float(evt.get("bid", float("nan")))
                ask = float(evt.get("ask", float("nan")))
                series.bid_ts.append(ts)
                series.bid_px.append(bid)
                series.ask_ts.append(ts)
                series.ask_px.append(ask)
                series.last_px = (bid + ask) * 0.5
            elif kind == "trade":
                px = float(evt.get("price", float("nan")))
                qty = float(evt.get("qty", 0.0))
                series.trade_ts.append(ts)
                series.trade_px.append(px)
                series.trade_qty.append(qty)
                series.last_px = px
            elif kind == "incremental":
                series.last_px = float(evt.get("price", float("nan")))

            self._prune(series.bid_ts, series.bid_px, prune_cutoff)
            self._prune(series.ask_ts, series.ask_px, prune_cutoff)
            self._prune(series.trade_ts, series.trade_px, prune_cutoff)
            self._prune(series.trade_ts, series.trade_qty, prune_cutoff)

    def snapshot(self) -> dict:
        with self._lock:
            self._refresh_symbol_cache_locked()
            out = {
                "events_total": self.events_total,
                "last_event_age_sec": (time.time() - self.last_event_time) if self.last_event_time else float("inf"),
                "symbols": self.symbols_sorted,
                "symbol_options": self.symbol_options,
                "symbol_version": self.symbol_version,
                "series": {},
            }
            for sym, s in self.symbol_to_series.items():
                out["series"][sym] = self._copy_series(s)
            return out

    def snapshot_controls(self, selected: Optional[str]) -> dict:
        with self._lock:
            selected_sym = self._selected_symbol_locked(selected)
            return {
                "events_total": self.events_total,
                "last_event_age_sec": (time.time() - self.last_event_time) if self.last_event_time else float("inf"),
                "symbols": self.symbols_sorted,
                "symbol_options": self.symbol_options,
                "symbol_version": self.symbol_version,
                "selected": selected_sym,
            }

    def snapshot_symbol(self, selected: Optional[str]) -> dict:
        with self._lock:
            selected_sym = self._selected_symbol_locked(selected)
            series = self._copy_series(self.symbol_to_series[selected_sym]) if selected_sym is not None else None

            return {
                "events_total": self.events_total,
                "last_event_age_sec": (time.time() - self.last_event_time) if self.last_event_time else float("inf"),
                "symbols": self.symbols_sorted,
                "symbol_options": self.symbol_options,
                "symbol_version": self.symbol_version,
                "selected": selected_sym,
                "series": series,
            }


class ReaderProcess:
    def __init__(
        self,
        reader_script: Path,
        pathname: str,
        refdata: Optional[str],
        sleep_ms: float,
        state: StreamState,
    ) -> None:
        self.reader_script = reader_script
        self.pathname = pathname
        self.refdata = refdata
        self.sleep_ms = sleep_ms
        self.state = state
        self.proc: Optional[subprocess.Popen[str]] = None
        self.thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        cmd: List[str] = [
            sys.executable,
            str(self.reader_script),
            "--pathname",
            self.pathname,
            "--sleep-ms",
            str(self.sleep_ms),
        ]
        if self.refdata:
            cmd += ["--refdata", self.refdata]

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Fail fast on startup errors (e.g. missing SHM segment) instead of
        # running a dashboard with a dead reader process.
        time.sleep(0.2)
        rc = self.proc.poll()
        if rc is not None:
            _stdout, stderr_text = self.proc.communicate(timeout=1)
            err = (stderr_text or "").strip()
            if err:
                raise RuntimeError(f"reader process exited during startup (rc={rc}): {err}")
            raise RuntimeError(f"reader process exited during startup (rc={rc})")

        self.thread = threading.Thread(target=self._consume_stdout, daemon=True)
        self.thread.start()
        threading.Thread(target=self._consume_stderr, daemon=True).start()

    def _consume_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            if self._stop.is_set():
                break
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            self.state.on_event(evt)

    def _consume_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        for line in self.proc.stderr:
            if self._stop.is_set():
                break
            sys.stderr.write(f"[reader] {line}")

    def stop(self) -> None:
        self._stop.set()
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def _window_points(ts: List[float], px: List[float], cutoff: float) -> Tuple[List[float], List[float]]:
    start = bisect_left(ts, cutoff)
    return ts[start:], px[start:]


def _window_trades(ts: List[float], px: List[float], qty: List[float], cutoff: float) -> Tuple[List[float], List[float], List[float]]:
    start = bisect_left(ts, cutoff)
    return ts[start:], px[start:], qty[start:]


def _aggregate_trade_notional_1s(trade_ts: List[float], trade_px: List[float], trade_qty: List[float]) -> Tuple[List[datetime], List[float]]:
    buckets: Dict[int, float] = {}
    for t, p, q in zip(trade_ts, trade_px, trade_qty):
        sec = int(t)
        buckets[sec] = buckets.get(sec, 0.0) + (p * q)
    secs = sorted(buckets.keys())
    x = [_ts_to_plot_datetime(sec) for sec in secs]
    y = [buckets[sec] for sec in secs]
    return x, y


def _aggregate_trade_notional_bucket(trade_ts: List[float], trade_px: List[float], trade_qty: List[float], bucket_sec: int) -> Tuple[List[datetime], List[float]]:
    buckets: Dict[int, float] = {}
    for t, p, q in zip(trade_ts, trade_px, trade_qty):
        sec = int(t)
        bucket = (sec // bucket_sec) * bucket_sec
        buckets[bucket] = buckets.get(bucket, 0.0) + (p * q)
    secs = sorted(buckets.keys())
    x = [_ts_to_plot_datetime(sec) for sec in secs]
    y = [buckets[sec] for sec in secs]
    return x, y


def _ts_to_plot_datetime(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def build_app(state: StreamState, title: str) -> Dash:
    app = Dash(__name__)
    app.title = title
    app.index_string = """<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
          html, body {
            margin: 0;
            padding: 0;
            background: #0B1220;
            color: #E5E7EB;
          }
          .dash-dropdown-value,
          .dash-dropdown-value > * {
            color: #102A43 !important;
          }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>"""

    app.layout = html.Div(
        style={"fontFamily": "'Segoe UI', 'IBM Plex Sans', Helvetica, Arial, sans-serif", "margin": "14px", "padding": "12px", "background": "linear-gradient(180deg, #0B1220 0%, #111A2B 100%)", "minHeight": "100vh"},
        children=[
            html.H3(title, style={"margin": 0, "color": "#E2E8F0", "letterSpacing": "0.2px"}),
            html.Div(id="status", style={"marginTop": "10px", "marginBottom": "10px", "padding": "8px 12px", "background": "#121A2B", "border": "1px solid #27334A", "borderRadius": "12px", "color": "#D7E2F0", "boxShadow": "0 8px 22px rgba(2, 6, 23, 0.45)"}),
            html.Div(
                style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "10px"},
                children=[
                    dcc.Dropdown(
                        id="window-select",
                        options=[
                            {"label": "1m", "value": "1m"},
                            {"label": "10m", "value": "10m"},
                        ],
                        value="1m",
                        clearable=False,
                        style={"width": "110px", "borderRadius": "10px"},
                    ),
                    dcc.Dropdown(id="symbol", options=[], value=None, clearable=False, style={"width": "420px", "borderRadius": "10px"}),
                ],
            ),
            dcc.Store(id="symbol-version", data=-1),
            dcc.Graph(id="price-graph", style={"height": "72vh", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}),
            dcc.Graph(id="overview-graph", style={"height": "72vh", "display": "none", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}),
            dcc.Interval(id="tick", interval=500, n_intervals=0),
        ],
    )

    @app.callback(
        Output("symbol", "options"),
        Output("symbol", "value"),
        Output("symbol-version", "data"),
        Input("tick", "n_intervals"),
        Input("symbol", "value"),
        State("symbol-version", "data"),
    )
    def refresh_symbol_controls(_n: int, selected: Optional[str], current_version: int):
        snap = state.snapshot_controls(selected)
        options_out = no_update
        version_out = no_update
        if snap["symbol_version"] != current_version:
            options_out = list(snap["symbol_options"])
            version_out = snap["symbol_version"]

        value_out = snap["selected"] if snap["selected"] != selected else no_update
        return options_out, value_out, version_out

    @app.callback(
        Output("status", "children"),
        Output("price-graph", "figure"),
        Output("overview-graph", "figure"),
        Output("price-graph", "style"),
        Output("overview-graph", "style"),
        Input("tick", "n_intervals"),
        Input("symbol", "value"),
        Input("window-select", "value"),
    )
    def refresh_charts(_n: int, selected: Optional[str], window_sel: str):
        snap = state.snapshot_symbol(selected)
        symbols = snap["symbols"]

        if not symbols:
            fig = go.Figure()
            fig.update_layout(template="plotly_white", title="Waiting for events from SHM...", paper_bgcolor="#0F172A", plot_bgcolor="#111827", font={"color": "#D7E2F0"})
            status = "events=0 | waiting for first packet"
            if window_sel == "10m":
                return status, no_update, fig, {"height": "72vh", "display": "none", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}, {"height": "72vh", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}
            return status, fig, no_update, {"height": "72vh", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}, {"height": "72vh", "display": "none", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}

        s = snap["series"]

        latest_candidates: List[float] = []
        if s["bid_ts"]:
            latest_candidates.append(s["bid_ts"][-1])
        if s["ask_ts"]:
            latest_candidates.append(s["ask_ts"][-1])
        if s["trade_ts"]:
            latest_candidates.append(s["trade_ts"][-1])
        latest_ts = max(latest_candidates) if latest_candidates else time.time()

        latest_dt = _ts_to_plot_datetime(latest_ts)
        overview_window_sec = 600.0
        overview_bucket_sec = 10
        overview_cutoff = latest_ts - overview_window_sec
        overview_cutoff_dt = _ts_to_plot_datetime(overview_cutoff)

        obid_t, obid_p = _window_points(s["bid_ts"], s["bid_px"], overview_cutoff)
        oask_t, oask_p = _window_points(s["ask_ts"], s["ask_px"], overview_cutoff)
        otrd_t, otrd_p, otrd_q = _window_trades(s["trade_ts"], s["trade_px"], s["trade_qty"], overview_cutoff)

        n_mid = min(len(obid_t), len(oask_t))
        mid_t = obid_t[:n_mid]
        mid_p = [(obid_p[i] + oask_p[i]) * 0.5 for i in range(n_mid)]
        mid_x = [_ts_to_plot_datetime(t) for t in mid_t]

        vol_x, vol_y = _aggregate_trade_notional_bucket(otrd_t, otrd_p, otrd_q, overview_bucket_sec)

        overview_fig = no_update
        if window_sel == "10m":
            overview_fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.70, 0.30],
            )

            if mid_x:
                overview_fig.add_trace(go.Scatter(x=mid_x, y=mid_p, mode="lines", name="mid", line={"width": 2.0, "shape": "hv", "color": "#93C5FD"}), row=1, col=1)
            if vol_x:
                overview_fig.add_trace(go.Bar(x=vol_x, y=vol_y, name="notional USD / 10s", width=overview_bucket_sec * 1000, marker={"color": "#60A5FA", "opacity": 0.85}), row=2, col=1)

            overview_fig.update_layout(
                template="plotly_white",
                paper_bgcolor="#0F172A",
                plot_bgcolor="#111827",
                font={"color": "#D7E2F0"},
                margin={"l": 40, "r": 10, "t": 30, "b": 40},
                legend={"orientation": "h", "y": 1.03, "x": 0.0, "bgcolor": "rgba(17,24,39,0.75)"},
                hovermode="x unified",
                xaxis={"range": [overview_cutoff_dt, latest_dt], "tickformat": "%H:%M:%S", "hoverformat": "%Y-%m-%d %H:%M:%S UTC"},
                xaxis2={"range": [overview_cutoff_dt, latest_dt], "tickformat": "%H:%M:%S", "hoverformat": "%Y-%m-%d %H:%M:%S UTC", "title": "time (UTC)"},
            )
            overview_fig.update_yaxes(title_text="mid price", row=1, col=1, gridcolor="#243042", zeroline=False)
            overview_fig.update_yaxes(title_text="notional USD", row=2, col=1, gridcolor="#243042", zeroline=False)
            overview_fig.update_xaxes(showgrid=True, gridcolor="#1F2A3D")

        # Last known bid/ask spread in bps.
        spread_bps = float("nan")
        if s["bid_px"] and s["ask_px"]:
            last_bid = s["bid_px"][-1]
            last_ask = s["ask_px"][-1]
            mid0 = 0.5 * (last_bid + last_ask)
            if mid0 > 0:
                spread_bps = (last_ask - last_bid) / mid0 * 10000.0

        # Day-ized volatility from 10m mid series (used in both 1m and 10m modes).
        vol_ts = mid_t
        vol_mid = mid_p

        sum_r2 = 0.0
        sum_dt = 0.0
        if len(vol_mid) >= 2:
            for i in range(1, len(vol_mid)):
                m0 = vol_mid[i - 1]
                m1 = vol_mid[i]
                dt = vol_ts[i] - vol_ts[i - 1]
                if m0 <= 0 or m1 <= 0 or dt <= 0:
                    continue
                r = math.log(m1 / m0)
                sum_r2 += r * r
                sum_dt += dt

        vol_bps = float("nan")
        if sum_dt > 0:
            sigma_day = math.sqrt((sum_r2 / sum_dt) * 86400.0)
            vol_bps = sigma_day * 10000.0

        total_notional_10m = sum(vol_y)
        coverage_sec = 0.0
        if len(mid_t) >= 2:
            coverage_sec = max(0.0, min(600.0, mid_t[-1] - mid_t[0]))

        est_24h_notional = float("nan")
        if coverage_sec > 0.0:
            est_24h_notional = total_notional_10m * (86400.0 / coverage_sec)

        spread_txt = "na" if math.isnan(spread_bps) else f"{spread_bps:.2f}bps"
        vol_pct = vol_bps / 100.0
        vol_txt = "na" if math.isnan(vol_bps) else f"{vol_pct:.1f}%"
        est_24h_txt = "na" if math.isnan(est_24h_notional) else f"{est_24h_notional:,.0f}"
        last_update_txt = latest_dt.strftime("%Y-%m-%d %H:%M")

        age = snap["last_event_age_sec"]
        age_txt = "inf" if age == float("inf") else f"{age * 1000.0:.1f}ms"
        status = (
            f"last_px={s['last_px']:.8g} | "
            f"estimated_24hr_volume={est_24h_txt} | "
            f"bid_ask_spread={spread_txt} | "
            f"volatility={vol_txt} | "
            f"last_seq={s['last_seq']} | "
            f"last_update={last_update_txt} | "
            f"last_event_age={age_txt}"
        )

        if window_sel == "10m":
            return status, no_update, overview_fig, {"height": "72vh", "display": "none", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}, {"height": "72vh", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}

        main_window_sec = 60.0
        cutoff = latest_ts - main_window_sec
        cutoff_dt = _ts_to_plot_datetime(cutoff)

        bid_t, bid_p = _window_points(s["bid_ts"], s["bid_px"], cutoff)
        ask_t, ask_p = _window_points(s["ask_ts"], s["ask_px"], cutoff)
        trd_t, trd_p, trd_q = _window_trades(s["trade_ts"], s["trade_px"], s["trade_qty"], cutoff)

        bid_x = [_ts_to_plot_datetime(t) for t in bid_t]
        ask_x = [_ts_to_plot_datetime(t) for t in ask_t]
        trd_x = [_ts_to_plot_datetime(t) for t in trd_t]
        bar_x, bar_y = _aggregate_trade_notional_1s(trd_t, trd_p, trd_q)

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.76, 0.24],
        )

        if bid_t:
            fig.add_trace(go.Scatter(x=bid_x, y=bid_p, mode="lines", name="bid", line={"width": 2.0, "shape": "hv", "color": "#2DD4BF"}), row=1, col=1)
        if ask_t:
            fig.add_trace(go.Scatter(x=ask_x, y=ask_p, mode="lines", name="ask", line={"width": 2.0, "shape": "hv", "color": "#FB7185"}), row=1, col=1)
        if trd_t:
            fig.add_trace(go.Scatter(x=trd_x, y=trd_p, mode="markers", name="trades", marker={"size": 5, "color": "#60A5FA", "opacity": 0.8}), row=1, col=1)

        if bar_x:
            fig.add_trace(go.Bar(x=bar_x, y=bar_y, name="trade notional USD / 1s", width=1000, marker={"color": "#60A5FA", "opacity": 0.85}), row=2, col=1)

        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="#0F172A",
            plot_bgcolor="#111827",
            font={"color": "#D7E2F0"},
            margin={"l": 40, "r": 10, "t": 30, "b": 40},
            legend={"orientation": "h", "y": 1.03, "x": 0.0, "bgcolor": "rgba(17,24,39,0.75)"},
            hovermode="x unified",
            xaxis={"range": [cutoff_dt, latest_dt], "tickformat": "%H:%M:%S", "hoverformat": "%Y-%m-%d %H:%M:%S UTC"},
            xaxis2={"range": [cutoff_dt, latest_dt], "tickformat": "%H:%M:%S", "hoverformat": "%Y-%m-%d %H:%M:%S UTC", "title": "time (UTC)"},
        )
        fig.update_yaxes(title_text="price", row=1, col=1, gridcolor="#243042", zeroline=False)
        fig.update_yaxes(title_text="notional USD", row=2, col=1, gridcolor="#243042", zeroline=False)
        fig.update_xaxes(showgrid=True, gridcolor="#1F2A3D")

        return status, fig, no_update, {"height": "72vh", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}, {"height": "72vh", "display": "none", "borderRadius": "16px", "boxShadow": "0 12px 30px rgba(2, 6, 23, 0.55)", "background": "#121A2B"}

    return app

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Dash app for live SHM prices")
    ap.add_argument("--pathname", default="/var/lib/sgt/shm/datashm")
    ap.add_argument("--refdata", default="/var/lib/sgt/refdata/refdata.json")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8010)
    ap.add_argument("--sleep-ms", type=float, default=1.0, help="reader polling sleep")
    ap.add_argument("--window-sec", type=float, default=60.0, help="rolling display window in seconds")
    ap.add_argument("--retention-sec", type=float, default=600.0, help="in-memory retention for pruning")
    ap.add_argument("--title", default="Price Monitor")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    reader_script = Path(__file__).with_name("shm_direct_price_reader.py")
    if not reader_script.exists():
        raise FileNotFoundError(f"missing reader script: {reader_script}")

    window_sec = max(args.window_sec, 1.0)
    retention_sec = max(args.retention_sec, window_sec * 2.0, 600.0)

    state = StreamState(window_sec=window_sec, retention_sec=retention_sec)
    reader = ReaderProcess(
        reader_script=reader_script,
        pathname=args.pathname,
        refdata=args.refdata,
        sleep_ms=args.sleep_ms,
        state=state,
    )
    reader.start()

    def _shutdown(*_a):
        reader.stop()

    atexit.register(_shutdown)

    def _on_sigterm(_sig, _frame):
        _shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_sigterm)

    app = build_app(state, args.title)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app.run(host=args.host, port=args.port, debug=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
