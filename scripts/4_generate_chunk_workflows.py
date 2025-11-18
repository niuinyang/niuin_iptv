#!/usr/bin/env python3
import os
import re
import json

WORKFLOW_DIR = ".github/workflows"        # GitHub Actions 工作流文件存放目录
CHUNK_DIR = "output/middle/chunk"         # 存放分片 CSV 文件的目录
CACHE_FILE = "output/cache_workflow.json" # 生成的缓存文件路径，记录所有 workflow 文件信息

# 确保工作流目录和缓存目录存在，避免写文件时出错
os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)

# GitHub Actions workflow 模板字符串，使用 {n} 占位符替换分片编号
# 包含三阶段扫描脚本依次执行的步骤，最后会提交并推送结果文件
# 其中 env 里设置 COMMIT_SHA 变量，方便追踪代码版本
TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["4生成chunk_workflows"]
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
          python scripts/5.1fast_scan.py \\
            --input output/middle/chunk/{n}.csv \\
            --output output/middle/fast/ok/fast_{n}.csv \\
            --invalid output/middle/fast/not/fast_{n}-invalid.csv
            
      - name: Run deep scan for {n}
        run: |
          mkdir -p output/middle/deep/ok output/middle/deep/not
          python scripts/5.2deep_scan.py \\
            --input output/middle/fast/ok/fast_{n}.csv \\
            --output output/middle/deep/ok/deep_{n}.csv \\
            --invalid output/middle/deep/not/deep_{n}-invalid.csv

      - name: Run final scan for {n}
        run: |
          mkdir -p output/middle/final/ok output/middle/final/not
          python scripts/5.3final_scan.py \\
            --input output/middle/deep/ok/deep_{n}.csv \\
            --output output/middle/final/ok/final_{n}.csv \\
            --invalid output/middle/final/not/final_{n}-invalid.csv \\
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

# 打印清理提示信息
print("🧹 清理旧的 workflow 文件...")

# 遍历 workflow 目录下所有文件，删除符合 scan_*.yml 命名规则的旧 workflow 文件
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_.+\.yml", f):
        os.remove(os.path.join(WORKFLOW_DIR, f))

# 如果存在缓存文件，删除它，准备重新生成
if os.path.exists(CACHE_FILE):
    os.remove(CACHE_FILE)

# 获取 chunk 目录下所有符合 chunk-数字.csv 格式的文件，排序，方便依次生成 workflow
chunks = sorted([f for f in os.listdir(CHUNK_DIR) if re.match(r"chunk-?\d+\.csv", f)])

cache_data = {}  # 用于缓存所有生成的 workflow 信息，便于后续使用和管理

for chunk_file in chunks:
    chunk_id = os.path.splitext(chunk_file)[0]  # 去除扩展名，只保留文件名部分，如 chunk-22

    workflow_filename = f"scan_{chunk_id}.yml"  # 生成 workflow 文件名
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)  # workflow 文件完整路径

    # 将模板中的占位符 {n} 替换成当前 chunk_id，写入对应的 workflow 文件
    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=chunk_id))

    # 打印提示，告知已生成对应的 workflow 文件
    print(f"✅ 已生成 workflow: {workflow_filename}")

    # 把当前 workflow 文件名存入缓存字典，后续写入缓存文件
    cache_data[chunk_id] = {"file": workflow_filename}

# 将所有生成的 workflow 信息写入缓存 JSON 文件
with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

# 打印完成提示，提醒用户提交并推送
print("\n🌀 生成 workflow 和缓存文件完成。请提交并推送。")