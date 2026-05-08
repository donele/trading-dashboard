# trade_sim

Dash dashboard for simulation runs and order/state analysis.

## Run

```bash
cd /home/jdlee/repos/trading-dashboard
python3 -m pip install -e .
python3 -m trade_sim
```

Open `http://127.0.0.1:8050/`.

## Configuration

- `DASH2_PORT`: override default port `8050`.
- `-d`: show only heads updated in the last 24 hours.
- `-w`: show only heads updated in the last 7 days.
- `-m`: show only heads updated in the last 30 days.

## Data Roots

The dashboard scans:

- `~/workspace/sgt/dumpsim`
- `~/workspace/sgt/livesim`
- `~/workspace/sgt/tradesim`

Each head directory should contain:

- `log/state/`
- one or more `log/order.????????.{log,parquet}` files.
