# trade_livesim

Dash dashboard for live-feed trading logs with simulated execution.

## Run

```bash
cd /home/jdlee/repos/trading-ui
python3 -m pip install -e .
python3 -m trade_livesim
```

Open `http://127.0.0.1:8060/`.

## Configuration

- `ORDER_LOG_DIR`: order log directory.
- `STATE_CSV_DIR`: state CSV directory (default: `<ORDER_LOG_DIR>/state`).

Example:

```bash
ORDER_LOG_DIR=/path/to/log STATE_CSV_DIR=/path/to/state python3 -m trade_livesim
```

## Data Expectations

- Order logs match `order*.YYYYMMDD.log`.
- State files are symbol/date CSV snapshots in the state directory.
- Timestamps are treated as UTC.
