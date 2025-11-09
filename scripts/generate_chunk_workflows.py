import os
import glob
from datetime import datetime, timezone, timedelta

WORKFLOW_DIR = ".github/workflows"  # workflow 生成目录，GitHub Actions 默认识别这里
CHUNK_DIR = "output/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)

def get_local_iso_timestamp():
    tz = timezone(timedelta(hours=8))
    now = datetime.now(tz)
    return now.isoformat()

template = """\
# Generated at: {timestamp}
name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '0 20 * * *'  # 每天 UTC 20:00 触发，东八区凌晨4点
  workflow_dispatch:

permissions:
  contents: write  # 允许修改仓库文件，删除 workflow 需要

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

      - name: Self delete workflow file
        env:
          REPO_TOKEN: ${{{{ secrets.PERSONAL_ACCESS_TOKEN }}}}
          WORKFLOW_FILE: ".github/workflows/deep_chunk_{n}.yml"
          GITHUB_REPOSITORY: ${{{{ github.repository }}}}
          GITHUB_REF: ${{{{ github.ref }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git rm "$WORKFLOW_FILE"
          git commit -m "ci: self delete workflow deep_chunk_{n}.yml after run"
          git push https://x-access-token:${{REPO_TOKEN}}@github.com/${{GITHUB_REPOSITORY}} HEAD:${{GITHUB_REF}}
"""

chunk_files = sorted(glob.glob(os.path.join(CHUNK_DIR, "*.csv")))
timestamp = get_local_iso_timestamp()

for i, f in enumerate(chunk_files, start=1):
    wf_path = os.path.join(WORKFLOW_DIR, f"deep_chunk_{i}.yml")
    with open(wf_path, "w", encoding="utf-8") as w:
        w.write(template.format(n=i, chunk_file=f, timestamp=timestamp))
    print(f"✅ Created: {wf_path}")
