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

# 收集所有 chunk 文件的时间点 ID，比如 chunk-0811.csv 提取 0811
chunk_files = sorted([f for f in os.listdir(CHUNK_DIR) if re.match(r"chunk-?\d+\.csv", f)])
chunk_ids = []
for f in chunk_files:
    # 去掉 chunk- 和 .csv，保留纯数字/字符部分作为时间点ID
    m = re.match(r"chunk-?(\d+)\.csv", f)
    if m:
        chunk_ids.append(m.group(1))

# 用逗号连接所有 chunk id 字符串，传给 final_scan
chunk_ids_str = ",".join(chunk_ids)

# 🧩 模板（改为监听 2pre-process.yml 完成，取消 schedule）
TEMPLATE = f"""name: Scan_all_chunks

on:
  workflow_run:
    workflows: ["2预处理🚀 IPTV全流程（下载→合并→分割→生成）"]
    types:
      - completed
  workflow_dispatch:

permissions:
  contents: write

jobs:
  scan_all:
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

      # 依旧针对单个 chunk 文件执行 fast_scan 和 deep_scan，单独处理每个 chunk 文件
"""

# 追加每个 chunk 的 fast_scan 和 deep_scan 步骤，同时最后统一做一次 final_scan 多 chunk_ids 扫描
for chunk_id in chunk_ids:
    TEMPLATE += f"""
      - name: Run fast scan for {chunk_id}
        run: |
          mkdir -p output/middle/fast/ok output/middle/fast/not
          python scripts/5.1fast_scan.py \\
            --input output/middle/chunk/chunk-{chunk_id}.csv \\
            --output output/middle/fast/ok/fast_{chunk_id}.csv \\
            --invalid output/middle/fast/not/fast_{chunk_id}-invalid.csv
            
      - name: Run deep scan for {chunk_id}
        run: |
          mkdir -p output/middle/deep/ok output/middle/deep/not
          python scripts/5.2deep_scan.py \\
            --input output/middle/fast/ok/fast_{chunk_id}.csv \\
            --output output/middle/deep/ok/deep_{chunk_id}.csv \\
            --invalid output/middle/deep/not/deep_{chunk_id}-invalid.csv
"""

# 最后一次性运行 final_scan，传入所有 chunk_ids
TEMPLATE += f"""
      - name: Run final scan for all chunks
        run: |
          mkdir -p output/middle/final/ok output/middle/final/not
          python scripts/5.3final_scan.py \\
            --input output/middle/deep/ok/deep_{{chunk_id}}.csv \\
            --output output/middle/final/ok/final_all.csv \\
            --invalid output/middle/final/not/final_all-invalid.csv \\
            --chunk_ids {chunk_ids_str} \\
            --cache_dir output/cache \\
            --threshold 0.95 \\
            --concurrency 6 \\
            --timeout 20

      - name: Commit and Push Outputs
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # 添加所有扫描阶段生成的文件，排除缓存文件夹
          git add output/middle/fast \\
                  output/middle/deep \\
                  output/middle/final

          if git diff --cached --quiet; then
            echo "No output updates."
            exit 0
          fi

          git commit -m "Update scan outputs for all chunks [skip ci]"

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

# 写入 workflow 文件
workflow_filename = "scan_all_chunks.yml"
workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

with open(workflow_path, "w", encoding="utf-8") as f:
    f.write(TEMPLATE)

print(f"✅ 已生成 workflow: {workflow_filename}")

# 写入缓存文件（chunk id 映射）
cache_data = {chunk_id: {"file": workflow_filename} for chunk_id in chunk_ids}

with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

print("\n🌀 提交生成的 workflow 和缓存文件到 GitHub...\n")
print("✅ 生成完毕，脚本结束。")