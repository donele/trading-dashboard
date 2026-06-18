# ops_console

Operations console for the local trading environment.

It provides:

- start, stop, and restart controls for configured services
- a single page with links to known HTTP endpoints
- recent logs for market-data services, erigon, and local dashboard processes
- support for both `systemd` units and subprocess-managed local Python servers

## Run

```bash
cd /home/jdlee/repos/trading-dashboard
python3 -m pip install -e .
python3 -m ops_console
```

Open `http://127.0.0.1:8000/`.

## Configuration

The app reads config from the first existing path in this order:

1. `--config /path/to/config.json`
2. `OPS_CONSOLE_CONFIG`
3. `ops_console/config.json`
4. `ops_console/config.example.json`

The bundled example config tracks:

- `sgt-md-okx-futures.service`
- `sgt-md-gate-futures.service`
- `sgt-md-bybit-futures.service`
- `erigon.service`
- local dashboard processes under `~/repos/trading-dashboard`

## Notes

- `systemd` actions call `systemctl` directly, so the effective permissions are whatever the web app process has.
- `systemd` services use `journalctl` unless a `log_file` is configured; the bundled market-data services read their process log files under `/var/lib/sgt/logs/`.
- local process-managed services write logs to `~/.local/state/trading-dashboard/ops-console/logs/`.
