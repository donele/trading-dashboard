# feed_live

Dash dashboard for live market feed monitoring from SGT shared memory.

## Run

```bash
cd /home/jdlee/repos/trading-ui
python3 -m pip install -e .
python3 -m feed_live \
  --pathname <shm-pathname> \
  --refdata <path-to-refdata>/refdata.latest.json \
  --host 127.0.0.1 \
  --port 8060 \
  --title "Feed Live Monitor"
```

Open `http://127.0.0.1:8060/`.

## CLI Tools

- `python3 -m feed_live`: live dashboard app.
- `python3 -m feed_live.shm_direct_price_reader`: direct shared-memory stream reader.

## Test

```bash
cd /home/jdlee/repos/trading-ui/feed_live
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```
