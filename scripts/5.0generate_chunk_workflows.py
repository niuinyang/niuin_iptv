#!/usr/bin/env python3
# scripts/5.0generate_chunk_workflows.py
import os
import re
import json
import time
import subprocess  # 新增

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)

# 🧩 模板（改为监听 2pre-process.yml 完成，取消 schedule）
TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["2预处理🚀 IPTV全流程（下载→合并→分割→生成）"]
    types:
      - completed
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

chunks = sorted([f for f in os.listdir(CHUNK_DIR) if re.match(r"chunk\d+-\d+\.csv", f)])
cache_data = {}

for chunk_file in chunks:
    chunk_id = os.path.splitext(chunk_file)[0]

    workflow_filename = f"scan_{chunk_id}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=chunk_id))

    print(f"✅ 已生成 workflow: {workflow_filename}")

    cache_data[chunk_id] = {"file": workflow_filename}

with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache_data, f, indent=2, ensure_ascii=False)

print("\n🌀 提交生成的 workflow 和缓存文件到 GitHub...\n")

# === 新增：自动提交和推送代码，带自动stash避免冲突 ===

subprocess.run(["git", "add", "-A"], check=False)
status_result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
if status_result.stdout.strip() == "":
    print("ℹ️ 无更改，跳过提交和推送")
else:
    commit_msg = "ci: auto-generate scan chunk workflows"
    commit_result = subprocess.run(["git", "commit", "-m", commit_msg], text=True)
    if commit_result.returncode != 0:
        print("⚠️ 提交失败，跳过推送")
    else:
        for attempt in range(1, 4):
            print(f"尝试推送，第 {attempt} 次...")
            push_result = subprocess.run(["git", "push"], text=True)
            if push_result.returncode == 0:
                print("🚀 推送成功")
                break
            else:
                print("⚠️ 推送失败，尝试自动stash并拉取远程合并重试...")
                subprocess.run(["git", "stash", "push", "-m", "ci: stash before pull"], text=True)
                pull_result = subprocess.run(["git", "pull", "--rebase"], text=True)
                if pull_result.returncode != 0:
                    print("❌ 拉取失败，终止重试")
                    subprocess.run(["git", "rebase", "--abort"], text=True)
                    subprocess.run(["git", "stash", "pop"], text=True)
                    break
                subprocess.run(["git", "stash", "pop"], text=True)
                print("⏳ 等待30秒后重试推送")
                time.sleep(30)
        else:
            print("❌ 达到最大重试次数，推送失败，请手动处理冲突")
            exit(1)

print("✅ 生成完毕，脚本结束。")
