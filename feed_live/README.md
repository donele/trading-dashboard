# feed_live

Dash dashboard for live market feed monitoring from SGT shared memory.

## Run

```bash
cd /home/jdlee/repos/trading-dashboard
python3 -m pip install -e .
python3 -m feed_live \
  --pathname /var/lib/sgt/shm/datashm \
  --refdata /var/lib/sgt/refdata/refdata.json \
  --host 127.0.0.1 \
  --port 8020 \
  --title "Feed Live Monitor"
```

Open `http://127.0.0.1:8020/`.

## CLI Tools

- `python3 -m feed_live`: live dashboard app.
- `python3 -m feed_live.shm_direct_price_reader`: direct shared-memory stream reader.

## Test

```bash
cd /home/jdlee/repos/trading-dashboard/feed_live
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```
