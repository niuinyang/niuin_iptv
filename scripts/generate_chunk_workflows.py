#!/usr/bin/env python3
# scripts/generate_chunk_workflows.py
import os
import re
import json
import time
from datetime import datetime, timedelta
import subprocess

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)

# 🧩 模板（已更新：带 git stash 防护、拉取合并和重试 push）
TEMPLATE = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '{cron}'  # 每天 UTC {utc_hour}:{utc_min:02d} 触发
  workflow_dispatch:

permissions:
  contents: write

jobs:
  deep_validate_chunk_{n}:
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

      - name: Run deep validation for chunk {n}
        run: |
          python scripts/4.3final_scan.py --input output/chunk/chunk_{n}.csv --chunk_id {n} --cache_dir output/cache

      - name: Commit and push changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add output/chunk_final_scan/working_chunk_{n}.csv output/chunk_final_scan/final_chunk_{n}.csv output/chunk_final_scan/final_invalid_chunk_{n}.csv output/cache/chunk/cache_hashes_chunk_{n}.json || echo "No files to add"
          git commit -m "ci: add final scan results and cache chunk {n}" || echo "No changes to commit"

          # 🔹 安全推送逻辑，带 stash 防护未暂存改动，重试推送
          for i in 1 2 3; do
            echo "推送尝试第 $i 次"
            if git push https://github-actions:${{ secrets.GITHUB_TOKEN }}@github.com/niuinyang/niuin_iptv.git main; then
              echo "推送成功"
              break
            else
              echo "推送失败，尝试拉取远程合并"
              git stash push -m "ci: stash before pull"
              if git pull --rebase; then
                echo "拉取合并成功，准备重试推送"
                git stash pop || echo "无 stash 可弹出"
              else
                echo "拉取合并失败，等待 30 秒后重试"
                git rebase --abort || true
                git stash pop || echo "无 stash 可弹出"
                sleep 30
              fi
            fi
          done
"""

# 🧹 清理旧 workflow 文件
print("🧹 清理旧的 workflow 文件...")
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"deep_chunk_\d+\.yml", f):
        os.remove(os.path.join(WORKFLOW_DIR, f))

if os.path.exists(CACHE_FILE):
    os.remove(CACHE_FILE)

# 🕒 按时间间隔分配 cron
start_hour = 19  # UTC 基准小时
start_minute = 30
interval = 10  # 每个 chunk 相隔 10 分钟
chunks = sorted([f for f in os.listdir(CHUNK_DIR) if f.startswith("chunk_") and f.endswith(".csv")])
total_chunks = len(chunks)

cache_data = {}

for i, chunk_file in enumerate(chunks, start=1):
    utc_hour = start_hour + ((start_minute + (i - 1) * interval) // 60)
    utc_min = (start_minute + (i - 1) * interval) % 60
    if utc_hour >= 24:
        utc_hour -= 24
    cron = f"{utc_min} {utc_hour} * * *"

    workflow_filename = f"deep_chunk_{i}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=i, cron=cron, utc_hour=utc_hour, utc_min=utc_min))

    print(f"✅ 已生成 workflow: {workflow_filename} 触发时间: {cron}")
    cache_data[f"chunk_{i}"] = {"cron": cron, "file": workflow_filename}

# 写入缓存文件
with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

print("\n🌀 提交生成的 workflow 和缓存文件到 GitHub...\n")

# 🧠 自动提交并推送
subprocess.run(["git", "add", "-A"], check=False)
subprocess.run(["git", "status"], check=False)
commit_msg = "ci: auto-generate deep validation workflows"
result = subprocess.run(["git", "commit", "-m", commit_msg], text=True)
if result.returncode == 0:
    print("✅ 已提交更改，准备推送...")
else:
    print("ℹ️ 无更改，跳过提交")

# 多次推送重试（防止偶发冲突）
for attempt in range(1, 4):
    print(f"尝试推送，第 {attempt} 次...")
    code = subprocess.run(["git", "push"], text=True).returncode
    if code == 0:
        print("🚀 推送成功")
        break
    else:
        print("⚠️ 推送失败，等待 30 秒后重试...")
        time.sleep(30)