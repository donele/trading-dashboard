from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import re
import threading
from typing import Dict, Optional

import pandas as pd

ORDER_FILE_PATTERN = re.compile(r"^orders?\.(\d{8})\.log$")
DEFAULT_INTERVAL_MINUTES = 10

_FILE_COUNTS: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
_FILE_MTIMES: Dict[str, float] = {}
_FILE_PARTIAL: Dict[str, bool] = {}
_BACKGROUND_LOCK = threading.Lock()
_BACKGROUND_RUNNING = False


def _list_order_files(log_dir: Path) -> list[Path]:
    if not log_dir.exists():
        return []
    return sorted(path for path in log_dir.iterdir() if path.is_file() and ORDER_FILE_PATTERN.match(path.name))


def _extract_timestamp(line: str) -> Optional[datetime]:
    parts = line.split(" ", 2)
    if len(parts) < 2:
        return None
    ts_str = f"{parts[0]} {parts[1]}"
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None


def _parse_payload(line: str) -> dict:
    start = line.find("{")
    end = line.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        return json.loads(line[start : end + 1])
    except json.JSONDecodeError:
        return {}


def _make_key(strategy_id: int, client_order_id: int) -> str:
    return f"{strategy_id}:{client_order_id}"


def _build_day_index(log_date: pd.Timestamp, interval_minutes: int) -> pd.DatetimeIndex:
    start = log_date.normalize()
    periods = 24 * 60 // interval_minutes
    return pd.date_range(start=start, periods=periods, freq=f"{interval_minutes}min")


def _parse_lines_to_day_counts(lines: list[str], *, interval_minutes: int, day_filter: Optional[str] = None) -> Dict[str, Dict[str, Dict[str, float]]]:
    day_new_clients: Dict[str, Dict[str, pd.Timestamp]] = {}
    day_fill_buckets: Dict[str, Dict[str, int]] = {}
    day_notional_buckets: Dict[str, Dict[str, float]] = {}
    day_keys: Dict[str, Dict[str, Dict[str, object]]] = {}

    for line in lines:
        ts = _extract_timestamp(line)
        if ts is None:
            continue
        day_iso = pd.Timestamp(ts.date()).isoformat()
        if day_filter is not None and day_iso != day_filter:
            continue

        payload = _parse_payload(line)
        if not payload:
            continue

        bucket_iso = pd.Timestamp(ts).floor(f"{interval_minutes}min").isoformat()

        action = payload.get("order_action")
        client_id = payload.get("client_order_id")
        strategy = payload.get("strategy_id")
        parent = payload.get("parent_order_id")
        symbol = payload.get("symbol")

        if action == "NEW" and client_id is not None and strategy is not None:
            key = _make_key(int(strategy), int(client_id))
            clients = day_new_clients.setdefault(day_iso, {})
            current = clients.get(key)
            if current is None or ts < current:
                clients[key] = ts

            keys = day_keys.setdefault(day_iso, {})
            entry = keys.setdefault(
                key,
                {
                    "parent_order_id": int(parent) if parent is not None else None,
                    "strategy_id": int(strategy),
                    "client_order_id": int(client_id),
                    "symbol": symbol,
                    "first_new_ts": ts,
                    "first_new_iso": ts.isoformat(),
                    "notional": 0.0,
                },
            )
            if ts < entry["first_new_ts"]:
                entry["first_new_ts"] = ts
                entry["first_new_iso"] = ts.isoformat()
                if symbol:
                    entry["symbol"] = symbol

        status = payload.get("order_status")
        if status in {"PARTIAL_FILL", "FILLED"}:
            fills = day_fill_buckets.setdefault(day_iso, {})
            fills[bucket_iso] = fills.get(bucket_iso, 0) + 1

            executed_price = payload.get("executed_price")
            filled_qty = payload.get("filled_qty")
            if executed_price is None or filled_qty is None:
                continue

            notional = float(executed_price) * float(filled_qty)
            notional_map = day_notional_buckets.setdefault(day_iso, {})
            notional_map[bucket_iso] = notional_map.get(bucket_iso, 0.0) + notional

            if client_id is None or strategy is None:
                continue
            key = _make_key(int(strategy), int(client_id))
            keys = day_keys.setdefault(day_iso, {})
            entry = keys.setdefault(
                key,
                {
                    "parent_order_id": int(parent) if parent is not None else None,
                    "strategy_id": int(strategy),
                    "client_order_id": int(client_id),
                    "symbol": symbol,
                    "first_new_ts": ts,
                    "first_new_iso": ts.isoformat(),
                    "notional": 0.0,
                },
            )
            if ts < entry["first_new_ts"]:
                entry["first_new_ts"] = ts
                entry["first_new_iso"] = ts.isoformat()
            entry["notional"] = float(entry["notional"]) + notional

    day_counts: Dict[str, Dict[str, Dict[str, float]]] = {}
    all_days = set(day_new_clients) | set(day_fill_buckets) | set(day_notional_buckets) | set(day_keys)
    for day in all_days:
        new_counts: Dict[str, int] = {}
        for ts in day_new_clients.get(day, {}).values():
            bucket_iso = pd.Timestamp(ts).floor(f"{interval_minutes}min").isoformat()
            new_counts[bucket_iso] = new_counts.get(bucket_iso, 0) + 1

        key_map: Dict[str, Dict[str, float]] = {}
        for key, entry in day_keys.get(day, {}).items():
            key_map[key] = {
                "parent_order_id": entry["parent_order_id"],
                "strategy_id": entry["strategy_id"],
                "client_order_id": entry["client_order_id"],
                "symbol": entry.get("symbol"),
                "first_new_iso": entry["first_new_iso"],
                "notional": float(entry["notional"]),
            }

        day_counts[day] = {
            "new": new_counts,
            "fills": day_fill_buckets.get(day, {}),
            "notional": day_notional_buckets.get(day, {}),
            "keys": key_map,
        }

    return day_counts


def _parse_order_file(path: Path, *, interval_minutes: int) -> Dict[str, Dict[str, Dict[str, float]]]:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        lines = [line.rstrip("\n") for line in handle]
    return _parse_lines_to_day_counts(lines, interval_minutes=interval_minutes)


def _iter_lines_reverse(path: Path, chunk_size: int = 1024 * 1024):
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        buffer = b""
        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            data = handle.read(read_size)
            buffer = data + buffer
            lines = buffer.split(b"\n")
            buffer = lines[0]
            for raw in reversed(lines[1:]):
                yield raw.decode("utf-8", errors="ignore")
        if buffer:
            yield buffer.decode("utf-8", errors="ignore")


def _parse_latest_day_from_file(path: Path, *, interval_minutes: int) -> Dict[str, Dict[str, Dict[str, float]]]:
    latest_day: Optional[str] = None
    collected: list[str] = []

    for line in _iter_lines_reverse(path):
        ts = _extract_timestamp(line)
        if ts is None:
            continue
        day_iso = pd.Timestamp(ts.date()).isoformat()
        if latest_day is None:
            latest_day = day_iso
        if day_iso == latest_day:
            collected.append(line)
        elif day_iso < latest_day:
            break

    if not collected or latest_day is None:
        return {}

    collected.reverse()
    return _parse_lines_to_day_counts(collected, interval_minutes=interval_minutes, day_filter=latest_day)


def _ensure_file_cached(path: Path, *, interval_minutes: int, fast_only_latest_day: bool = False) -> None:
    path_str = str(path)
    mtime = path.stat().st_mtime
    unchanged = _FILE_MTIMES.get(path_str) == mtime and path_str in _FILE_COUNTS
    if unchanged and not _FILE_PARTIAL.get(path_str, False):
        return
    if unchanged and fast_only_latest_day:
        return

    if fast_only_latest_day and path_str not in _FILE_COUNTS:
        _FILE_COUNTS[path_str] = _parse_latest_day_from_file(path, interval_minutes=interval_minutes)
        _FILE_MTIMES[path_str] = mtime
        _FILE_PARTIAL[path_str] = True
        return

    _FILE_COUNTS[path_str] = _parse_order_file(path, interval_minutes=interval_minutes)
    _FILE_MTIMES[path_str] = mtime
    _FILE_PARTIAL[path_str] = False


def _rebuild_aggregated_counts() -> Dict[str, Dict[str, Dict[str, float]]]:
    aggregated: Dict[str, Dict[str, Dict[str, float]]] = {}
    aggregated_keys: Dict[str, Dict[str, Dict[str, object]]] = {}

    for file_counts in _FILE_COUNTS.values():
        for day, metrics in file_counts.items():
            entry = aggregated.setdefault(day, {"new": {}, "fills": {}, "notional": {}})
            for metric in ("new", "fills", "notional"):
                for bucket, value in metrics.get(metric, {}).items():
                    current = entry[metric].get(bucket, 0)
                    entry[metric][bucket] = current + value

            key_entry = aggregated_keys.setdefault(day, {})
            for key, data in metrics.get("keys", {}).items():
                existing = key_entry.get(key)
                if existing is None:
                    key_entry[key] = data.copy()
                    continue
                existing["notional"] = float(existing["notional"]) + float(data["notional"])
                if pd.Timestamp(data["first_new_iso"]) < pd.Timestamp(existing["first_new_iso"]):
                    existing["first_new_iso"] = data["first_new_iso"]
                    if data.get("symbol"):
                        existing["symbol"] = data["symbol"]
                    existing["parent_order_id"] = data.get("parent_order_id")
                    existing["strategy_id"] = data["strategy_id"]
                    existing["client_order_id"] = data["client_order_id"]

    merged: Dict[str, Dict[str, Dict[str, float]]] = {}
    for day, metrics in aggregated.items():
        merged[day] = {
            "new": metrics["new"],
            "fills": metrics["fills"],
            "notional": metrics["notional"],
            "keys": aggregated_keys.get(day, {}),
        }
    return merged


def _background_worker(log_dir: Path, *, interval_minutes: int) -> None:
    global _BACKGROUND_RUNNING
    try:
        files = sorted(_list_order_files(log_dir), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in files:
            _ensure_file_cached(path, interval_minutes=interval_minutes, fast_only_latest_day=False)
    finally:
        with _BACKGROUND_LOCK:
            _BACKGROUND_RUNNING = False


def _start_background_processing(log_dir: Path, *, interval_minutes: int) -> None:
    global _BACKGROUND_RUNNING
    with _BACKGROUND_LOCK:
        if _BACKGROUND_RUNNING:
            return
        _BACKGROUND_RUNNING = True
    thread = threading.Thread(
        target=_background_worker,
        args=(log_dir,),
        kwargs={"interval_minutes": interval_minutes},
        daemon=True,
    )
    thread.start()


def _latest_log_file(log_dir: Path) -> Optional[Path]:
    files = _list_order_files(log_dir)
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def get_latest_day_counts(log_dir: Path, *, interval_minutes: int = DEFAULT_INTERVAL_MINUTES) -> Dict[str, Dict[str, Dict[str, float]]]:
    latest = _latest_log_file(log_dir)
    if latest is None:
        return {}

    _ensure_file_cached(latest, interval_minutes=interval_minutes, fast_only_latest_day=True)
    _start_background_processing(log_dir, interval_minutes=interval_minutes)
    return _rebuild_aggregated_counts()


def get_serialized_day_counts(log_dir: Path, *, interval_minutes: int = DEFAULT_INTERVAL_MINUTES) -> Dict[str, Dict[str, Dict[str, float]]]:
    current_files = {str(path): path for path in _list_order_files(log_dir)}
    for path in current_files.values():
        _ensure_file_cached(path, interval_minutes=interval_minutes, fast_only_latest_day=False)

    removed = [key for key in _FILE_COUNTS if key not in current_files]
    for key in removed:
        _FILE_COUNTS.pop(key, None)
        _FILE_MTIMES.pop(key, None)
        _FILE_PARTIAL.pop(key, None)

    return _rebuild_aggregated_counts()


def available_dates_from_serialized(day_counts: Dict[str, Dict[str, Dict[str, float]]]) -> list[str]:
    return sorted(day_counts.keys(), key=pd.Timestamp)


def counts_for_date_from_serialized(
    day_counts: Dict[str, Dict[str, Dict[str, float]]],
    date_iso: str,
    *,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    metric: str = "new",
) -> pd.Series:
    target = pd.Timestamp(date_iso)
    index = _build_day_index(target, interval_minutes)
    if date_iso not in day_counts or metric not in day_counts[date_iso]:
        return pd.Series(0, index=index, dtype=float)
    buckets = {pd.Timestamp(k): v for k, v in day_counts[date_iso][metric].items()}
    return pd.Series(buckets, dtype=float).reindex(index, fill_value=0)


def latest_date_from_serialized(day_counts: Dict[str, Dict[str, Dict[str, float]]]) -> Optional[str]:
    dates = available_dates_from_serialized(day_counts)
    if not dates:
        return None
    return dates[-1]


def daily_totals_from_serialized(day_counts: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for date, metrics in day_counts.items():
        totals[date] = int(sum(metrics.get("new", {}).values()))
    return totals


def cumulative_notional_points(
    day_counts: Dict[str, Dict[str, Dict[str, float]]], date_iso: str
) -> list[Dict[str, float | str]]:
    if date_iso not in day_counts:
        return []
    buckets = day_counts[date_iso].get("notional", {})
    if not buckets:
        return []

    series = pd.Series({pd.Timestamp(k): v for k, v in buckets.items()}, dtype=float).sort_index()
    cumulative = series.cumsum()

    result: list[Dict[str, float | str]] = []
    for timestamp, bucket_value in series.items():
        result.append(
            {
                "bucket_iso": timestamp.isoformat(),
                "bucket_notional": float(bucket_value),
                "cumulative": float(cumulative.loc[timestamp]),
            }
        )
    return result
