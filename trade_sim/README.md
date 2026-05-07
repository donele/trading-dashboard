# trade_sim

Dash dashboard for simulation runs and order/state analysis.

## Run

```bash
cd /home/jdlee/repos/trading-ui
python3 -m pip install -e .
python3 -m trade_sim
```

Open `http://127.0.0.1:8050/`.

## Configuration

- `DASH2_PORT`: override default port `8050`.

## Data Roots

The dashboard scans:

- `~/workspace/sgt/dumpsim`
- `~/workspace/sgt/livesim`
- `~/workspace/sgt/tradesim`

Each head directory should contain:

- `log/state/`
- one or more `log/order.????????.{log,parquet}` files.
