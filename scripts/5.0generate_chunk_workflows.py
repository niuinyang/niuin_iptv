#!/usr/bin/env python3
# scripts/5.0generate_chunk_workflows.py
import os
import re
import json
import time

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)

# 🧩 模板（已修正版，统一北京时间 08:15 执行）
TEMPLATE = """name: Scan_{n}

on:
  schedule:
    - cron: '{cron}'  # 每天 UTC {utc_hour}:{utc_min:02d} 触发（北京时间 07:10）
  workflow_dispatch:

permissions:
  contents: write

jobs:
  scan_{n}:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install ffmpeg
        run: sudo apt-get update && sudo apt-get install -y ffmpeg

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run fast scan for {n}
        run: |
          mkdir -p output/middle/fast/ok output/middle/fast/not
          python scripts/5.1fast_scan.py \
            --input output/middle/chunk/{n}.csv \
            --output output/middle/fast/ok/fast_{n}.csv \
            --invalid output/middle/fast/not/fast_{n}-invalid.csv
            
      - name: Run deep scan for {n}
        run: |
          mkdir -p output/middle/deep/ok output/middle/deep/not
          python scripts/5.2deep_scan.py \
            --input output/middle/fast/ok/fast_{n}.csv \
            --output output/middle/deep/ok/deep_{n}.csv \
            --invalid output/middle/deep/not/deep_{n}-invalid.csv

      - name: Run final scan for {n}
        run: |
          mkdir -p output/middle/final/ok output/middle/final/not
          python scripts/5.3final_scan.py \
            --input output/middle/deep/ok/deep_{n}.csv \
            --output output/middle/final/ok/final_{n}.csv \
            --invalid output/middle/final/not/final_{n}-invalid.csv \
            --chunk_id {n} \
            --cache_dir output/cache
"""

print("🧹 清理旧的 workflow 文件...")
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_chunk_.+\.yml", f):
        os.remove(os.path.join(WORKFLOW_DIR, f))

if os.path.exists(CACHE_FILE):
    os.remove(CACHE_FILE)

# 固定每天北京时间 07:10 执行（UTC 23:10）
start_hour = 0  # UTC 时区小时
start_minute = 20  # UTC 分钟

chunks = sorted([f for f in os.listdir(CHUNK_DIR) if re.match(r"chunk\d+-\d+\.csv", f)])
cache_data = {}

for chunk_file in chunks:
    utc_hour = start_hour
    utc_min = start_minute
    cron = f"{utc_min} {utc_hour} * * *"

    chunk_id = os.path.splitext(chunk_file)[0]

    workflow_filename = f"scan_{chunk_id}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=chunk_id, cron=cron, utc_hour=utc_hour, utc_min=utc_min))

    print(f"✅ 已生成 workflow: {workflow_filename} 触发时间: {cron}")
    cache_data[chunk_id] = {"cron": cron, "file": workflow_filename}

# 写入缓存文件
with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

print("✅ 生成完毕，脚本结束。")
