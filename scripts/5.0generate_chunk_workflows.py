#!/usr/bin/env python3
# scripts/5.0generate_chunk_workflows.py
import os
import re
import json

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)

# 模板，新增 env 传入 COMMIT_SHA
TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["2预处理🚀 IPTV全流程（下载→合并→分割→生成）"]
    types:
      - completed
  workflow_dispatch:

env:
  COMMIT_SHA: ${{{{ github.sha }}}}

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

      - name: Commit and Push Outputs
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add output/cache \\
                  output/middle/fast \\
                  output/middle/deep \\
                  output/middle/final

          if git diff --cached --quiet; then
            echo "No output updates."
            exit 0
          fi

          git commit -m "Update scan outputs for {n} [skip ci]"

          MAX_RETRIES=5
          COUNT=1

          until git push --quiet; do
            echo "Push failed (attempt $COUNT/$MAX_RETRIES), retrying..."

            git stash push -m "auto-stash" || true
            git pull --rebase --quiet || true
            git stash pop || true

            COUNT=$((COUNT+1))
            if [ $COUNT -gt $MAX_RETRIES ]; then
              echo "🔥 Push failed after $MAX_RETRIES attempts."
              exit 1
            fi

            sleep 2
          done

          echo "Push outputs succeeded."
"""

print("🧹 清理旧的 workflow 文件...")
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_.+\.yml", f):
        os.remove(os.path.join(WORKFLOW_DIR, f))

if os.path.exists(CACHE_FILE):
    os.remove(CACHE_FILE)

chunks = sorted([f for f in os.listdir(CHUNK_DIR) if re.match(r"chunk-?\d+\.csv", f)])
cache_data = {}

for chunk_file in chunks:
    chunk_id = os.path.splitext(chunk_file)[0]  # 去掉扩展名

    workflow_filename = f"scan_{chunk_id}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=chunk_id))

    print(f"✅ 已生成 workflow: {workflow_filename}")

    cache_data[chunk_id] = {"file": workflow_filename}

with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

print("\n🌀 生成 workflow 和缓存文件完成。请提交并推送。")