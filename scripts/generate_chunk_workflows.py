import os
import glob
from datetime import datetime, timezone, timedelta

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)

# 获取当前本地时间（东八区）
def get_local_iso_timestamp():
    tz = timezone(timedelta(hours=8))
    now = datetime.now(tz)
    return now.isoformat()

template = """\
# Generated at: {timestamp}
name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '0 20 * * *'  # 每天 UTC 20:00 触发，相当于东八区凌晨4点
  workflow_dispatch:

jobs:
  deep_chunk_{n}:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Run final scan on chunk {n}
        run: |
          python scripts/4.3final_scan.py --input {chunk_file}
"""

chunk_files = sorted(glob.glob(os.path.join(CHUNK_DIR, "*.csv")))
timestamp = get_local_iso_timestamp()

for i, f in enumerate(chunk_files, start=1):
    wf_path = os.path.join(WORKFLOW_DIR, f"deep_chunk_{i}.yml")
    with open(wf_path, "w", encoding="utf-8") as w:
        w.write(template.format(n=i, chunk_file=f, timestamp=timestamp))
    print(f"✅ Created: {wf_path}")
