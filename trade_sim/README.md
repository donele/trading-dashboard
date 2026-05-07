# trade_sim

Dash dashboard for simulation runs and order/state analysis.

## Run

```bash
cd /home/jdlee/repos/trading-ui/trade_sim
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app_dash.py
```

Open `http://127.0.0.1:8051/`.

## Configuration

- `DASH2_PORT`: override default port `8051`.

## Data Roots

The dashboard scans:

- `~/workspace/sgt/dumpsim`
- `~/workspace/sgt/livesim`
- `~/workspace/sgt/tradesim`

Each head directory should contain:

- `log/state/`
- one or more `log/order.????????.{log,parquet}` files.
