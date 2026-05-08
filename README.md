# trading-ui

Collection of independent dashboards for feed monitoring and trading simulation.

## Dashboards

- `feed_daily`: Daily static market-data stats dashboard.
- `feed_live`: Live feed monitor from shared memory.
- `trade_sim`: Simulation dashboard for dump/livesim/tradesim heads.
- `trade_livesim`: Trade activity dashboard using live-feed strategy logs with simulated execution.
- `algo_docs`: Builds MkDocs from `~/repos/sgt` into `~/repos/sgt/site` and serves it.

## Quick Start

```bash
cd /home/jdlee/repos/trading-ui
python3 -m pip install -e .
```

Start dashboards from the repository root:

```bash
python3 -m feed_daily
python3 -m feed_live
python3 -m trade_sim
python3 -m trade_livesim
python3 -m algo_docs
```

- [feed_daily/README.md](/home/jdlee/repos/trading-ui/feed_daily/README.md)
- [feed_live/README.md](/home/jdlee/repos/trading-ui/feed_live/README.md)
- [trade_sim/README.md](/home/jdlee/repos/trading-ui/trade_sim/README.md)
- [trade_livesim/README.md](/home/jdlee/repos/trading-ui/trade_livesim/README.md)
