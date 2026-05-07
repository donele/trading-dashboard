# trading-ui

Monorepo for UI dashboards used in trading workflows.

## Repository Layout

- `feed_daily/`: Daily feed dashboard.
- `feed_live/`: Live feed monitor dashboard.
- `trade_sim/`: Trading simulation dashboard.
- `trade_livesim/`: Trading dashboard using live feed with simulated execution.

## Getting Started

Each dashboard is managed independently and has its own dependencies and run instructions:

1. Read [feed_live/README.md](/home/jdlee/repos/trading-ui/feed_live/README.md)
2. Read [trade_sim/README.md](/home/jdlee/repos/trading-ui/trade_sim/README.md)

## Notes

- Install dependencies from each subproject's `requirements.txt`.
- Run each app from its own directory unless its README states otherwise.
