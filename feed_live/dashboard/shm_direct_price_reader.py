#!/usr/bin/env python3
"""
Direct Python SysV SHM reader for SGT market-data batches.

This is a monitoring/demo utility. It mirrors the current C++ ABI/layout in:
- core/ipc/shm_context.h
- core/types/{packet_header,packet_boundary,book_ticker,trade,incremental}.h
"""

from __future__ import annotations

import argparse
import ctypes
import json
import signal
import struct
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


# Verified on this repo/build environment with a layout probe.
HEADER_SIZE = 256
CONSUMER_SEQUENCE_SIZE = 64
METADATA_SIZE = 24

OFF_SEQUENCE_NUM = 0
OFF_LEDGER_SIZE = 224
OFF_STORAGE_SIZE = 232
OFF_CONSUMERS = 240
OFF_INITIALIZED = 248

# Packet/message sizes.
SZ_PACKET_HEADER = 22
SZ_PACKET_BEGIN = 19
SZ_PACKET_END = 19
SZ_INCREMENTAL = 55
SZ_BOOK_TICKER = 82
SZ_TRADE = 64

# msg_type constants from core/types/message_types.h
MSG_PACKET_HEADER = 0
MSG_PACKET_BEGIN = 1
MSG_PACKET_END = 2
MSG_INCREMENTAL = 4
MSG_BOOK_TICKER = 5
MSG_TRADE = 6

# struct formats (little-endian, no implicit alignment)
FMT_PACKET_HEADER = "<HIQQ"
FMT_PACKET_BEGIN = "<HQQ?"
FMT_PACKET_END = "<HQQ?"
FMT_INCREMENTAL = "<HQqdqdQBI"
FMT_BOOK_TICKER = "<HQqdqdqdqdQ"
FMT_TRADE = "<H6xQqdqdQBIB2x"


libc = ctypes.CDLL(None, use_errno=True)
libc.ftok.argtypes = [ctypes.c_char_p, ctypes.c_int]
libc.ftok.restype = ctypes.c_int

libc.shmget.argtypes = [ctypes.c_int, ctypes.c_size_t, ctypes.c_int]
libc.shmget.restype = ctypes.c_int

libc.shmat.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
libc.shmat.restype = ctypes.c_void_p

libc.shmdt.argtypes = [ctypes.c_void_p]
libc.shmdt.restype = ctypes.c_int


@dataclass
class SegmentLayout:
    base: int
    ledger_size: int
    storage_size: int
    consumers: int
    consumer_base: int
    ledger_base: int
    storage_base: int


def _u16(addr: int) -> int:
    return struct.unpack("<H", ctypes.string_at(addr, 2))[0]


def _i64(addr: int) -> int:
    return struct.unpack("<q", ctypes.string_at(addr, 8))[0]


def _u8(addr: int) -> int:
    return ctypes.string_at(addr, 1)[0]


def attach_segment(pathname: str) -> SegmentLayout:
    path = Path(pathname)
    if not path.exists():
        raise FileNotFoundError(
            f"pathname '{pathname}' does not exist; ftok() requires an existing file"
        )

    key = libc.ftok(str(path).encode(), 8)
    if key == -1:
        err = ctypes.get_errno()
        raise OSError(err, f"ftok failed for {pathname}")

    shmid = libc.shmget(key, 0, 0)
    if shmid == -1:
        err = ctypes.get_errno()
        raise OSError(err, f"shmget failed for key={key}")

    ptr = libc.shmat(shmid, None, 0)
    if ptr == ctypes.c_void_p(-1).value:
        err = ctypes.get_errno()
        raise OSError(err, f"shmat failed for shmid={shmid}")

    base = int(ptr)

    # Wait for producer initialization handshake.
    while _u8(base + OFF_INITIALIZED) == 0:
        time.sleep(0.001)

    ledger_size = _i64(base + OFF_LEDGER_SIZE)
    storage_size = _i64(base + OFF_STORAGE_SIZE)
    consumers = _i64(base + OFF_CONSUMERS)

    if ledger_size <= 0 or storage_size <= 0 or consumers <= 0:
        libc.shmdt(ctypes.c_void_p(base))
        raise RuntimeError(
            "invalid SHM header values "
            f"ledger_size={ledger_size} storage_size={storage_size} consumers={consumers}"
        )

    consumer_base = base + HEADER_SIZE
    ledger_base = consumer_base + consumers * CONSUMER_SEQUENCE_SIZE
    storage_base = ledger_base + ledger_size * METADATA_SIZE

    return SegmentLayout(
        base=base,
        ledger_size=ledger_size,
        storage_size=storage_size,
        consumers=consumers,
        consumer_base=consumer_base,
        ledger_base=ledger_base,
        storage_base=storage_base,
    )


def detach_segment(layout: SegmentLayout) -> None:
    if libc.shmdt(ctypes.c_void_p(layout.base)) != 0:
        err = ctypes.get_errno()
        raise OSError(err, "shmdt failed")


def _refdata_multiplier(row: dict) -> float:
    for key in ("contract_multiplier", "contract_size", "multiplier"):
        raw = row.get(key)
        if raw is None:
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if val > 0.0:
            return val
    return 1.0


def load_symbol_map(refdata_path: Optional[str]) -> Dict[int, Tuple[str, float]]:
    if not refdata_path:
        return {}

    data = json.loads(Path(refdata_path).read_text())
    out: Dict[int, Tuple[str, float]] = {}
    if not isinstance(data, list):
        return out

    for row in data:
        if not isinstance(row, dict):
            continue
        sid = row.get("global_instance_id")
        if sid is None:
            sid = row.get("symbol_id")
        if sid is None:
            continue
        name = row.get("flat_id") or row.get("native_id") or row.get("name")
        out[int(sid)] = (
            str(name) if name is not None else str(sid),
            _refdata_multiplier(row),
        )
    return out


def iter_events(
    layout: SegmentLayout,
    symbol_meta_map: Dict[int, Tuple[str, float]],
    sleep_sec: float,
    symbol_filter: Optional[set[int]],
) -> Iterable[dict]:
    consumer_seq = _i64(layout.base + OFF_SEQUENCE_NUM) + 1
    current_symbol_id: Optional[int] = None

    while True:
        cursor = _i64(layout.base + OFF_SEQUENCE_NUM)
        if cursor < consumer_seq:
            time.sleep(sleep_sec)
            continue

        idx = consumer_seq & (layout.ledger_size - 1)
        meta = layout.ledger_base + idx * METADATA_SIZE
        offset = _i64(meta + 0)
        size = _i64(meta + 8)
        topic = _u16(meta + 16)

        consumer_seq += 1

        if size <= 0:
            continue

        storage_idx = offset & (layout.storage_size - 1)
        batch_ptr = layout.storage_base + storage_idx
        batch = ctypes.string_at(batch_ptr, int(size))
        if len(batch) < SZ_PACKET_HEADER:
            continue

        msg_type, batch_size, global_seq_no, sequencer_ts = struct.unpack_from(
            FMT_PACKET_HEADER, batch, 0
        )
        if msg_type != MSG_PACKET_HEADER:
            continue
        if batch_size > len(batch):
            continue

        pos = SZ_PACKET_HEADER
        while pos < batch_size:
            if pos + 2 > batch_size:
                break
            mtype = struct.unpack_from("<H", batch, pos)[0]

            if mtype == MSG_PACKET_BEGIN:
                if pos + SZ_PACKET_BEGIN > batch_size:
                    break
                _, symbol_id, _exchange_time, _snapshot = struct.unpack_from(
                    FMT_PACKET_BEGIN, batch, pos
                )
                current_symbol_id = symbol_id
                pos += SZ_PACKET_BEGIN
                continue

            if mtype == MSG_PACKET_END:
                if pos + SZ_PACKET_END > batch_size:
                    break
                _, symbol_id, _exchange_time, _snapshot = struct.unpack_from(
                    FMT_PACKET_END, batch, pos
                )
                current_symbol_id = symbol_id
                pos += SZ_PACKET_END
                continue

            if mtype == MSG_BOOK_TICKER:
                if pos + SZ_BOOK_TICKER > batch_size:
                    break
                (
                    _,
                    exchange_time,
                    bid_v,
                    bid_incr,
                    bid_qty_v,
                    bid_qty_incr,
                    ask_v,
                    ask_incr,
                    ask_qty_v,
                    ask_qty_incr,
                    update_id,
                ) = struct.unpack_from(FMT_BOOK_TICKER, batch, pos)
                pos += SZ_BOOK_TICKER

                symbol_id = current_symbol_id if current_symbol_id is not None else -1
                if symbol_filter and symbol_id not in symbol_filter:
                    continue

                symbol_name, contract_multiplier = symbol_meta_map.get(symbol_id, (str(symbol_id), 1.0))

                bid = bid_v * bid_incr
                ask = ask_v * ask_incr
                evt = {
                    "kind": "book_ticker",
                    "symbol_id": symbol_id,
                    "symbol": symbol_name,
                    "exchange_time": exchange_time,
                    "sequencer_time_ns": sequencer_ts,
                    "seq": global_seq_no,
                    "topic": topic,
                    "bid": bid,
                    "ask": ask,
                    "mid": (bid + ask) * 0.5,
                    "bid_qty": bid_qty_v * bid_qty_incr * contract_multiplier,
                    "ask_qty": ask_qty_v * ask_qty_incr * contract_multiplier,
                    "contract_multiplier": contract_multiplier,
                    "update_id": update_id,
                }
                yield evt
                continue

            if mtype == MSG_TRADE:
                if pos + SZ_TRADE > batch_size:
                    break
                (
                    _,
                    exchange_time,
                    px_v,
                    px_incr,
                    qty_v,
                    qty_incr,
                    trade_id,
                    side,
                    _flags,
                    is_mm,
                ) = struct.unpack_from(FMT_TRADE, batch, pos)
                pos += SZ_TRADE

                symbol_id = current_symbol_id if current_symbol_id is not None else -1
                if symbol_filter and symbol_id not in symbol_filter:
                    continue

                symbol_name, contract_multiplier = symbol_meta_map.get(symbol_id, (str(symbol_id), 1.0))

                evt = {
                    "kind": "trade",
                    "symbol_id": symbol_id,
                    "symbol": symbol_name,
                    "exchange_time": exchange_time,
                    "sequencer_time_ns": sequencer_ts,
                    "seq": global_seq_no,
                    "topic": topic,
                    "price": px_v * px_incr,
                    "qty": qty_v * qty_incr * contract_multiplier,
                    "contract_multiplier": contract_multiplier,
                    "trade_id": trade_id,
                    "side": int(side),
                    "is_market_maker": bool(is_mm),
                }
                yield evt
                continue

            if mtype == MSG_INCREMENTAL:
                if pos + SZ_INCREMENTAL > batch_size:
                    break
                (
                    _,
                    exchange_time,
                    px_v,
                    px_incr,
                    qty_v,
                    qty_incr,
                    num_orders,
                    side,
                    _flags,
                ) = struct.unpack_from(FMT_INCREMENTAL, batch, pos)
                pos += SZ_INCREMENTAL

                symbol_id = current_symbol_id if current_symbol_id is not None else -1
                if symbol_filter and symbol_id not in symbol_filter:
                    continue

                symbol_name, contract_multiplier = symbol_meta_map.get(symbol_id, (str(symbol_id), 1.0))

                evt = {
                    "kind": "incremental",
                    "symbol_id": symbol_id,
                    "symbol": symbol_name,
                    "exchange_time": exchange_time,
                    "sequencer_time_ns": sequencer_ts,
                    "seq": global_seq_no,
                    "topic": topic,
                    "price": px_v * px_incr,
                    "qty": qty_v * qty_incr * contract_multiplier,
                    "contract_multiplier": contract_multiplier,
                    "num_orders": num_orders,
                    "side": int(side),
                }
                yield evt
                continue

            # Unknown type for this monitor utility: stop this batch to avoid desync.
            break


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Direct Python SHM price reader")
    ap.add_argument("--pathname", default="/node_data/shm/datashm", help="SHM pathname used by ftok")
    ap.add_argument("--refdata", default=None, help="Optional refdata JSON to map symbol_id -> name")
    ap.add_argument(
        "--symbol-id",
        action="append",
        type=int,
        default=None,
        help="Optional symbol_id filter (repeatable)",
    )
    ap.add_argument("--sleep-ms", type=float, default=1.0, help="poll sleep when no data")
    ap.add_argument("--max-events", type=int, default=0, help="exit after N events (0 means run forever)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    symbol_meta_map = load_symbol_map(args.refdata)
    symbol_filter = set(args.symbol_id) if args.symbol_id else None

    stop = {"flag": False}

    def _stop_handler(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    layout = attach_segment(args.pathname)
    count = 0
    try:
        for evt in iter_events(
            layout=layout,
            symbol_meta_map=symbol_meta_map,
            sleep_sec=max(args.sleep_ms, 0.0) / 1000.0,
            symbol_filter=symbol_filter,
        ):
            print(json.dumps(evt, separators=(",", ":")), flush=True)
            count += 1
            if stop["flag"]:
                break
            if args.max_events > 0 and count >= args.max_events:
                break
    finally:
        detach_segment(layout)

    return 0


if __name__ == "__main__":
    sys.exit(main())
