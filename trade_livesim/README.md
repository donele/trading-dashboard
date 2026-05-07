# trade_livesim

Dash dashboard for live-feed trading logs with simulated execution.

## Run

```bash
cd /home/jdlee/repos/trading-ui/trade_livesim
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 dash_app.py
```

Open `http://127.0.0.1:8050/`.

## Configuration

- `ORDER_LOG_DIR`: order log directory.
- `STATE_CSV_DIR`: state CSV directory (default: `<ORDER_LOG_DIR>/state`).

Example:

```bash
ORDER_LOG_DIR=/path/to/log STATE_CSV_DIR=/path/to/state python3 dash_app.py
```

## Data Expectations

- Order logs match `order*.YYYYMMDD.log`.
- State files are symbol/date CSV snapshots in the state directory.
- Timestamps are treated as UTC.
