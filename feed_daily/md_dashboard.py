#!/usr/bin/env python3
"""Build a browsable dashboard for shm dump conversion stats.

The dashboard consumes the daily stats emitted by shm_dump_to_parquet:

  <stats-root>/<YYYY>/<MMDD>/
    summary.json
    summary.csv
    datatype.csv
    exchange.csv
    symbol.csv

The output is a static HTML site with:
  - an index page across all days
  - a per-day page
  - a per-symbol page

The generated pages use inline SVG so no web framework is required.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import math
import shutil
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_STATS_ROOT = "/mnt/bigdata2/Ferris/stats"
DEFAULT_OUTPUT_ROOT = "./md_dashboard_site"


@dataclass(frozen=True)
class DaySummary:
    day: str
    total_symbols: int
    total_rows: int
    book_l2: int
    book_ticker: int
    trades: int
    fundingrate: int
    trade_notional: float


@dataclass(frozen=True)
class DatatypeRow:
    day: str
    datatype: str
    rows: int


@dataclass(frozen=True)
class ExchangeRow:
    day: str
    exchange: str
    total_symbols: int
    total_rows: int
    book_l2: int
    book_ticker: int
    trades: int
    fundingrate: int


@dataclass(frozen=True)
class SymbolRow:
    day: str
    symbol: str
    venue: str
    total_rows: int
    book_l2: int
    book_ticker: int
    trades: int
    fundingrate: int
    snapshot_packets: int
    snapshot_packet_interval_mean_s: float
    snapshot_packet_interval_median_s: float
    snapshot_packet_interval_min_s: float
    snapshot_packet_interval_max_s: float
    snapshot_packet_depth_mean: float
    snapshot_packet_depth_min: int
    snapshot_packet_depth_max: int
    trade_notional: float


@dataclass
class DayData:
    summary: DaySummary
    datatypes: list[DatatypeRow]
    exchanges: list[ExchangeRow]
    symbols: list[SymbolRow]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a static dashboard for shm dump stats.")
    parser.add_argument(
        "--stats-root",
        default=DEFAULT_STATS_ROOT,
        help="Root directory containing YYYY/MMDD stats tables",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where the HTML dashboard will be written",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve the generated dashboard with a local http.server after building",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind when using --serve",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind when using --serve",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove the output directory before rebuilding",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def parse_int(value: str | None, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(float(value))


def parse_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def load_day(day_dir: Path) -> DayData | None:
    summary_rows = read_csv_rows(day_dir / "summary.csv")
    if not summary_rows:
        return None
    summary_row = summary_rows[0]
    day = summary_row["day"]
    summary = DaySummary(
        day=day,
        total_symbols=parse_int(summary_row.get("total_symbols")),
        total_rows=parse_int(summary_row.get("total_rows")),
        book_l2=parse_int(summary_row.get("book_l2")),
        book_ticker=parse_int(summary_row.get("book_ticker")),
        trades=parse_int(summary_row.get("trades")),
        fundingrate=parse_int(summary_row.get("fundingrate")),
        trade_notional=parse_float(summary_row.get("trade_notional")),
    )

    datatypes = [
        DatatypeRow(day=row["day"], datatype=row["datatype"], rows=parse_int(row.get("rows")))
        for row in read_csv_rows(day_dir / "datatype.csv")
    ]
    exchanges = [
        ExchangeRow(
            day=row["day"],
            exchange=row["exchange"],
            total_symbols=parse_int(row.get("total_symbols")),
            total_rows=parse_int(row.get("total_rows")),
            book_l2=parse_int(row.get("book_l2")),
            book_ticker=parse_int(row.get("book_ticker")),
            trades=parse_int(row.get("trades")),
            fundingrate=parse_int(row.get("fundingrate")),
        )
        for row in read_csv_rows(day_dir / "exchange.csv")
    ]
    symbols = [
        SymbolRow(
            day=row["day"],
            symbol=row["symbol"],
            venue=row["venue"],
            total_rows=parse_int(row.get("total_rows")),
            book_l2=parse_int(row.get("book_l2")),
            book_ticker=parse_int(row.get("book_ticker")),
            trades=parse_int(row.get("trades")),
            fundingrate=parse_int(row.get("fundingrate")),
            snapshot_packets=parse_int(row.get("snapshot_packets", row.get("snapshots"))),
            snapshot_packet_interval_mean_s=parse_float(row.get("snapshot_packet_interval_mean_s", row.get("snapshot_interval_mean_s"))),
            snapshot_packet_interval_median_s=parse_float(row.get("snapshot_packet_interval_median_s", row.get("snapshot_interval_median_s"))),
            snapshot_packet_interval_min_s=parse_float(row.get("snapshot_packet_interval_min_s", row.get("snapshot_interval_min_s"))),
            snapshot_packet_interval_max_s=parse_float(row.get("snapshot_packet_interval_max_s", row.get("snapshot_interval_max_s"))),
            snapshot_packet_depth_mean=parse_float(row.get("snapshot_packet_depth_mean", row.get("snapshot_depth_mean"))),
            snapshot_packet_depth_min=parse_int(row.get("snapshot_packet_depth_min", row.get("snapshot_depth_min"))),
            snapshot_packet_depth_max=parse_int(row.get("snapshot_packet_depth_max", row.get("snapshot_depth_max"))),
            trade_notional=parse_float(row.get("trade_notional")),
        )
        for row in read_csv_rows(day_dir / "symbol.csv")
    ]
    return DayData(summary=summary, datatypes=datatypes, exchanges=exchanges, symbols=symbols)


def load_stats(stats_root: Path) -> list[DayData]:
    days: list[DayData] = []
    roots = []
    for subdir in ("daily_stats", "job_stats"):
        candidate = stats_root / subdir
        if candidate.exists():
            roots.append(candidate)
    if not roots:
        roots = [stats_root]

    seen_days: set[str] = set()
    for root in roots:
        for summary_path in sorted(root.glob("[0-9][0-9][0-9][0-9]/*/summary.csv")):
            day_dir = summary_path.parent
            day_data = load_day(day_dir)
            if day_data is None:
                continue
            if day_data.summary.day in seen_days:
                continue
            days.append(day_data)
            seen_days.add(day_data.summary.day)
    days.sort(key=lambda day: day.summary.day)
    return days


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}"


def fmt_float(value: float, digits: int = 3) -> str:
    if math.isfinite(value):
        return f"{value:,.{digits}f}"
    return "n/a"


def fmt_money(value: float) -> str:
    return f"{value:,.2f}"


def slugify_symbol(symbol: str) -> str:
    return urllib.parse.quote(symbol, safe="")


def day_to_dir(day: str) -> tuple[str, str]:
    return day[:4], day[4:]


def relpath(from_path: Path, to_path: Path) -> str:
    return Path(to_path).relative_to(from_path).as_posix() if to_path.is_relative_to(from_path) else str(to_path)


def page(title: str, body: str, nav: str = "") -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7fb;
      --panel: #ffffff;
      --text: #18212f;
      --muted: #5b6472;
      --border: #d8dde6;
      --accent: #265b9b;
      --accent2: #157a6e;
      --warn: #8a4f08;
    }}
    html, body {{
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
    .wrap {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 20px;
    }}
    .nav {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }}
    .nav a {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 12px;
    }}
    h1, h2, h3 {{
      margin: 0 0 12px 0;
      line-height: 1.15;
    }}
    h1 {{ font-size: 28px; }}
    h2 {{ font-size: 20px; margin-top: 20px; }}
    h3 {{ font-size: 16px; margin-top: 16px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin: 12px 0 20px 0;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
    }}
    .metric-label {{
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.02em;
    }}
    .metric-value {{
      font-size: 24px;
      font-weight: 700;
      margin-top: 4px;
    }}
    .metric-sub {{
      color: var(--muted);
      margin-top: 4px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }}
    th, td {{
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{
      text-align: left;
    }}
    thead th {{
      background: #eef2f7;
      font-weight: 600;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    tbody tr:hover {{
      background: #fbfcfe;
    }}
    .chart {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px;
      overflow-x: auto;
      margin-bottom: 16px;
    }}
    .section {{
      margin-bottom: 20px;
    }}
    .muted {{
      color: var(--muted);
    }}
    .small {{
      font-size: 12px;
    }}
    .status {{
      padding: 8px 10px;
      border-radius: 8px;
      background: #fff7e8;
      color: var(--warn);
      border: 1px solid #f0d7a2;
      margin-bottom: 12px;
    }}
    svg text {{
      font-family: inherit;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    {nav}
    {body}
  </div>
</body>
</html>
"""


def nav_links(parts: list[tuple[str, str]]) -> str:
    if not parts:
        return ""
    links = " ".join(f'<a href="{esc(href)}">{esc(label)}</a>' for label, href in parts)
    return f'<div class="nav">{links}</div>'


def bar_chart(title: str, items: list[tuple[str, float]], width: int = 1100, row_height: int = 26) -> str:
    if not items:
        return f'<div class="card"><h3>{esc(title)}</h3><div class="muted">No data</div></div>'
    label_width = max(min(max(len(label) for label, _ in items) * 7 + 10, 360), 120)
    value_width = 120
    chart_width = width - label_width - value_width - 40
    chart_width = max(chart_width, 160)
    max_value = max(value for _, value in items) or 1.0
    height = row_height * len(items) + 50
    lines = [
        f'<div class="chart"><h3>{esc(title)}</h3>',
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
    ]
    y = 28
    for label, value in items:
        bar_len = int((value / max_value) * chart_width) if max_value else 0
        lines.append(f'<text x="0" y="{y + 8}" font-size="12" fill="#18212f">{esc(label)}</text>')
        lines.append(
            f'<rect x="{label_width}" y="{y - 2}" width="{max(bar_len, 1)}" height="16" rx="4" fill="#265b9b"></rect>'
        )
        lines.append(
            f'<text x="{label_width + chart_width + 12}" y="{y + 8}" font-size="12" fill="#18212f">{esc(fmt_float(value, 2) if not float(value).is_integer() else fmt_int(value))}</text>'
        )
        y += row_height
    lines.append("</svg></div>")
    return "".join(lines)


def line_chart(title: str, series: list[tuple[str, float]], width: int = 1100, height: int = 260) -> str:
    if not series:
        return f'<div class="card"><h3>{esc(title)}</h3><div class="muted">No data</div></div>'
    values = [value for _, value in series]
    labels = [label for label, _ in series]
    min_value = min(values)
    max_value = max(values)
    if math.isclose(min_value, max_value):
        max_value += 1.0
        min_value -= 1.0
    left = 60
    top = 20
    right = 30
    bottom = 45
    plot_w = width - left - right
    plot_h = height - top - bottom
    points = []
    for idx, value in enumerate(values):
        x = left + (plot_w * idx / max(len(values) - 1, 1))
        y = top + plot_h - ((value - min_value) / (max_value - min_value)) * plot_h
        points.append(f"{x:.1f},{y:.1f}")
    grid_lines = []
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_h - frac * plot_h
        value = min_value + frac * (max_value - min_value)
        grid_lines.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" stroke="#d8dde6" stroke-width="1"></line>'
        )
        grid_lines.append(
            f'<text x="6" y="{y + 4:.1f}" font-size="11" fill="#5b6472">{esc(fmt_float(value, 2))}</text>'
        )
    tick_labels = []
    step = max(1, len(labels) // 6)
    for idx, label in enumerate(labels):
        if idx != len(labels) - 1 and idx % step != 0:
            continue
        x = left + (plot_w * idx / max(len(labels) - 1, 1))
        tick_labels.append(
            f'<text x="{x:.1f}" y="{top + plot_h + 22}" font-size="11" text-anchor="middle" fill="#5b6472">{esc(label)}</text>'
        )
    return (
        f'<div class="chart"><h3>{esc(title)}</h3>'
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">'
        f'{"".join(grid_lines)}'
        f'<polyline fill="none" stroke="#157a6e" stroke-width="2.5" points="{" ".join(points)}"></polyline>'
        f'{"".join(tick_labels)}'
        f"</svg></div>"
    )


def make_table(headers: list[str], rows: Iterable[Iterable[object]]) -> str:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def day_page(day: DayData, out_root: Path, prev_day: DayData | None, next_day: DayData | None) -> None:
    year, mmdd = day_to_dir(day.summary.day)
    day_dir = out_root / year / mmdd
    symbol_dir = day_dir / "symbols"
    symbol_dir.mkdir(parents=True, exist_ok=True)

    nav_items = [("Index", os.path.relpath(out_root / "index.html", day_dir))]
    if prev_day:
        prev_year, prev_mmdd = day_to_dir(prev_day.summary.day)
        nav_items.append(("Previous day", os.path.relpath(out_root / prev_year / prev_mmdd / "index.html", day_dir)))
    if next_day:
        next_year, next_mmdd = day_to_dir(next_day.summary.day)
        nav_items.append(("Next day", os.path.relpath(out_root / next_year / next_mmdd / "index.html", day_dir)))
    nav = nav_links(nav_items)

    metrics = [
        ("Symbols", fmt_int(day.summary.total_symbols)),
        ("Rows", fmt_int(day.summary.total_rows)),
        ("Book L2", fmt_int(day.summary.book_l2)),
        ("Book Ticker", fmt_int(day.summary.book_ticker)),
        ("Trades", fmt_int(day.summary.trades)),
        ("Funding Rate", fmt_int(day.summary.fundingrate)),
        ("Trade Notional", fmt_money(day.summary.trade_notional)),
    ]
    metric_html = "".join(
        f'<div class="card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>'
        for label, value in metrics
    )

    by_datatype = [(row.datatype, float(row.rows)) for row in day.datatypes]
    by_exchange = [(row.exchange, float(row.total_rows)) for row in day.exchanges]

    symbol_rows = []
    for row in sorted(day.symbols, key=lambda r: (r.venue, r.symbol)):
        symbol_href = f"symbols/{slugify_symbol(row.symbol)}.html"
        symbol_rows.append([
            f'<a href="{esc(symbol_href)}">{esc(row.symbol)}</a>',
            esc(row.venue),
            fmt_int(row.total_rows),
            fmt_int(row.book_l2),
            fmt_int(row.book_ticker),
            fmt_int(row.trades),
            fmt_int(row.fundingrate),
            fmt_int(row.snapshot_packets),
            fmt_float(row.snapshot_packet_interval_mean_s, 3),
            fmt_float(row.snapshot_packet_depth_mean, 2),
            fmt_money(row.trade_notional),
        ])

    body = "".join([
        f"<h1>{esc(day.summary.day)} · Daily Stats</h1>",
        '<div class="grid">' + metric_html + "</div>",
        bar_chart("Rows by datatype", by_datatype),
        bar_chart("Rows by exchange", by_exchange),
        '<div class="section"><h2>Per-symbol summary</h2>' + make_table(
            [
                "Symbol",
                "Venue",
                "Rows",
                "Book L2",
                "Book Ticker",
                "Trades",
                "Funding",
                "Snapshot packets",
                "Packet interval mean (s)",
                "Packet depth mean",
                "Trade notional",
            ],
            symbol_rows,
        ) + "</div>",
    ])
    (day_dir / "index.html").write_text(page(f"Stats {day.summary.day}", body, nav), encoding="utf-8")

    for row in sorted(day.symbols, key=lambda r: (r.venue, r.symbol)):
        symbol_page(row, day_dir, day)


def symbol_page(row: SymbolRow, day_dir: Path, day: DayData) -> None:
    symbol_dir = day_dir / "symbols"
    path = symbol_dir / f"{slugify_symbol(row.symbol)}.html"
    nav = nav_links([
        ("Index", "../../../index.html"),
        (f"Day {day.summary.day}", "../index.html"),
    ])
    metrics = [
        ("Rows", fmt_int(row.total_rows)),
        ("Book L2", fmt_int(row.book_l2)),
        ("Book Ticker", fmt_int(row.book_ticker)),
        ("Trades", fmt_int(row.trades)),
        ("Funding Rate", fmt_int(row.fundingrate)),
        ("Snapshot packets", fmt_int(row.snapshot_packets)),
        ("Trade Notional", fmt_money(row.trade_notional)),
    ]
    metric_html = "".join(
        f'<div class="card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>'
        for label, value in metrics
    )

    body = "".join([
        f"<h1>{esc(row.symbol)}</h1>",
        f'<div class="muted">Venue: {esc(row.venue)} · Day: {esc(row.day)}</div>',
        '<div class="grid">' + metric_html + "</div>",
        bar_chart(
            "Rows by datatype",
            [
                ("book_l2", float(row.book_l2)),
                ("book_ticker", float(row.book_ticker)),
                ("trades", float(row.trades)),
                ("fundingrate", float(row.fundingrate)),
            ],
        ),
        '<div class="section"><h2>Packet stats</h2>' + make_table(
            ["Metric", "Value"],
            [
                ("Snapshot packets", fmt_int(row.snapshot_packets)),
                ("Packet interval mean (s)", fmt_float(row.snapshot_packet_interval_mean_s, 6)),
                ("Packet interval median (s)", fmt_float(row.snapshot_packet_interval_median_s, 6)),
                ("Packet interval min (s)", fmt_float(row.snapshot_packet_interval_min_s, 6)),
                ("Packet interval max (s)", fmt_float(row.snapshot_packet_interval_max_s, 6)),
                ("Packet depth mean", fmt_float(row.snapshot_packet_depth_mean, 3)),
                ("Packet depth min", fmt_int(row.snapshot_packet_depth_min)),
                ("Packet depth max", fmt_int(row.snapshot_packet_depth_max)),
            ],
        ) + "</div>",
    ])
    path.write_text(page(f"{row.symbol} {row.day}", body, nav), encoding="utf-8")


def index_page(days: list[DayData], out_root: Path) -> None:
    nav = nav_links([])
    metrics = [
        ("Days", fmt_int(len(days))),
        ("Symbols", fmt_int(sum(day.summary.total_symbols for day in days))),
        ("Rows", fmt_int(sum(day.summary.total_rows for day in days))),
        ("Trades", fmt_int(sum(day.summary.trades for day in days))),
        ("Trade Notional", fmt_money(sum(day.summary.trade_notional for day in days))),
    ]
    metric_html = "".join(
        f'<div class="card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>'
        for label, value in metrics
    )

    rows_by_day = [(day.summary.day, float(day.summary.total_rows)) for day in days]
    trades_by_day = [(day.summary.day, float(day.summary.trades)) for day in days]
    notional_by_day = [(day.summary.day, float(day.summary.trade_notional)) for day in days]

    table_rows = []
    for day in days:
        year, mmdd = day_to_dir(day.summary.day)
        href = f"{year}/{mmdd}/index.html"
        table_rows.append([
            f'<a href="{esc(href)}">{esc(day.summary.day)}</a>',
            fmt_int(day.summary.total_symbols),
            fmt_int(day.summary.total_rows),
            fmt_int(day.summary.book_l2),
            fmt_int(day.summary.book_ticker),
            fmt_int(day.summary.trades),
            fmt_int(day.summary.fundingrate),
            fmt_money(day.summary.trade_notional),
        ])

    body = "".join([
        "<h1>Market Data Dashboard</h1>",
        '<div class="muted">Daily stats generated from shm dump conversion output.</div>',
        '<div class="grid">' + metric_html + "</div>",
        line_chart("Daily rows", rows_by_day),
        line_chart("Daily trades", trades_by_day),
        line_chart("Daily trade notional", notional_by_day),
        '<div class="section"><h2>Days</h2>' + make_table(
            ["Day", "Symbols", "Rows", "Book L2", "Book Ticker", "Trades", "Funding", "Trade Notional"],
            table_rows,
        ) + "</div>",
    ])
    (out_root / "index.html").write_text(page("Market Data Dashboard", body, nav), encoding="utf-8")


def build_dashboard(stats_root: Path, output_root: Path, clean: bool) -> list[DayData]:
    if clean and output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    days = load_stats(stats_root)
    if not days:
        raise RuntimeError(f"no stats found under {stats_root}")

    for idx, day in enumerate(days):
        prev_day = days[idx - 1] if idx > 0 else None
        next_day = days[idx + 1] if idx + 1 < len(days) else None
        day_page(day, output_root, prev_day, next_day)
    index_page(days, output_root)
    return days


def serve(output_root: Path, host: str, port: int) -> None:
    from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
    import os

    os.chdir(output_root)
    server = ThreadingHTTPServer((host, port), SimpleHTTPRequestHandler)
    print(f"Serving {output_root} on http://{host}:{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def main() -> int:
    args = parse_args()
    stats_root = Path(args.stats_root)
    output_root = Path(args.output_root)
    build_dashboard(stats_root, output_root, clean=not args.no_clean)
    if args.serve:
        serve(output_root, args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
