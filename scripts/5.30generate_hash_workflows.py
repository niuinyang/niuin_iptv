#!/usr/bin/env python3
import os
import json
from datetime import datetime

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_DIR = "output/cache/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

TIME_POINTS = {
    "0811": "08:11",
    "1612": "16:12",
    "2113": "21:13"
}

# 用于记录当天手动触发次数文件
MANUAL_TRIGGER_RECORD = "output/manual_trigger_record.json"

def get_chunks():
    files = os.listdir(CHUNK_DIR)
    chunks = [f for f in files if f.startswith("chunk-") and f.endswith(".csv")]
    return sorted(chunks)

def load_manual_record():
    if os.path.exists(MANUAL_TRIGGER_RECORD):
        with open(MANUAL_TRIGGER_RECORD, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}

def save_manual_record(data):
    os.makedirs(os.path.dirname(MANUAL_TRIGGER_RECORD), exist_ok=True)
    with open(MANUAL_TRIGGER_RECORD, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def generate_workflow(chunk_name, time_key, time_str, manual_count):
    # workflow 文件名，例如 hash-chunk-1-0811.yml
    base = os.path.splitext(chunk_name)[0]  # chunk-1
    workflow_name = f"hash-{base}-{time_key}"
    filename = f"{workflow_name}.yml"
    filepath = os.path.join(WORKFLOW_DIR, filename)

    chunk_id = base.split("-")[1]  # 1

    cron_time = time_str.split(":")
    hour = int(cron_time[0])
    minute = int(cron_time[1])

    # workflow内容
    content = f"""name: {workflow_name}

on:
  schedule:
    - cron: '{minute} {hour} * * *'  # UTC时间，需换算为UTC时间（假设北京时间+8小时）
  workflow_dispatch:
    inputs:
      manual_count:
        description: 'Manual trigger count'
        required: false
        default: '{manual_count}'

permissions:
  contents: write

jobs:
  run-cache:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: pip install --quiet pillow tqdm

      - name: Run cache script
        run: |
          python scripts/5.31cache.py --input {CHUNK_DIR}/{chunk_name} --timepoint {time_key} --chunk_id {chunk_id}

      - name: Push cache changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add output/cache/chunk/{datetime.now().strftime('%Y%m%d')}/
          git commit -m "Update cache for {chunk_name} at {time_key}" || echo "No changes to commit"
          n=0
          until [ $n -ge 3 ]
          do
            git push origin HEAD:main && break
            n=$((n+1))
            echo "Push failed, retry $n..."
            sleep 5
          done
        env:
          GITHUB_TOKEN: ${{{{ secrets.PUSH_TOKEN1 }}}}

      - name: Self-delete workflow file
        run: |
          git rm .github/workflows/{filename}
          git commit -m "Self delete {filename}"
          n=0
          until [ $n -ge 3 ]
          do
            git push origin HEAD:main && break
            n=$((n+1))
            echo "Push failed, retry $n..."
            sleep 5
          done
        env:
          GITHUB_TOKEN: ${{{{ secrets.PUSH_TOKEN1 }}}}
"""

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Generated workflow: {filename}")

def main():
    chunks = get_chunks()
    today = datetime.now().strftime("%Y%m%d")
    manual_record = load_manual_record()
    manual_count = manual_record.get(today, 0)  # 今天已触发次数

    # 根据触发次数决定当前时间点
    # 0->0811, 1->1612, 2->2113, 3->0811循环
    keys = list(TIME_POINTS.keys())
    time_key = keys[manual_count % len(keys)]
    time_str = TIME_POINTS[time_key]

    print(f"Manual trigger count today: {manual_count}, will generate workflows for time point {time_key} ({time_str})")

    for chunk in chunks:
        generate_workflow(chunk, time_key, time_str, manual_count)

    # 更新记录
    manual_record[today] = manual_count + 1
    save_manual_record(manual_record)

if __name__ == "__main__":
    main()