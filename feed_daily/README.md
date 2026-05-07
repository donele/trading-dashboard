# MD Dashboard

Static dashboard generator for shm dump conversion stats.

Snapshot metrics in the generated stats are packet-level snapshot counts derived
from dump files. The dump format does not retain the SHM topic metadata, so the
dashboard does not attempt to label them as rolling timer snapshots.

## Input

The dashboard reads stats tables written by `shm_dump_to_parquet`:

```text
<stats-root>/<YYYY>/<MMDD>/
  summary.json
  summary.csv
  datatype.csv
  exchange.csv
  symbol.csv
```

## Build

```bash
python3 tools/md_dashboard/md_dashboard.py \
  --stats-root /mnt/bigdata2/Ferris/stats \
  --output-root /tmp/md_dashboard
```

## Serve

```bash
python3 tools/md_dashboard/md_dashboard.py \
  --stats-root /mnt/bigdata2/Ferris/stats \
  --output-root /tmp/md_dashboard \
  --serve \
  --host 127.0.0.1 \
  --port 8000
```

Open:

```text
http://127.0.0.1:8000/
```
