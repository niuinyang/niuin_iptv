#!/usr/bin/env python3
# scripts/generate_chunk_workflows.py
"""
为 output/middle/chunk/ 下的 chunk{main}-{sub}.csv 自动生成独立的 GitHub Actions workflow
每个 workflow 间隔 5 分钟，按顺序运行:
  1) scripts/4.1fast_scan.py  -> output/middle/fast/fast_chunk{main}-{sub}.csv
  2) scripts/4.2deep_scan.py  -> output/middle/deep/deep_chunk{main}-{sub}.csv
  3) scripts/4.3final_scan.py  -> output/middle/final/final_chunk{main}-{sub}.csv
推送逻辑沿用原脚本（带 GITHUB_TOKEN 安全推送 + 重试/拉取合并）。
"""

import os
import re
import json
import time
import subprocess

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_FILE = "output/cache_workflow.json"

# 创建需要的目录
os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output/cache", exist_ok=True)
os.makedirs("output/middle/fast", exist_ok=True)
os.makedirs("output/middle/deep", exist_ok=True)
os.makedirs("output/middle/final", exist_ok=True)

# 🧩 模板：注意对 GITHUB_TOKEN 的转义（在 format 中保留 GitHub Actions 的花括号）
TEMPLATE = """name: Scan Chunk {file_base}

on:
  schedule:
    - cron: '{cron}'  # 每天 UTC {utc_hour}:{utc_min:02d} 触发
  workflow_dispatch:

permissions:
  contents: write

jobs:
  scan_chunk_{job_name}:
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

      - name: Run fast scan
        run: |
          set -e
          mkdir -p output/middle/fast
          python scripts/4.1fast_scan.py --input {chunk_path} --output output/middle/fast/fast_{file_base}.csv

      - name: Run deep scan
        run: |
          set -e
          mkdir -p output/middle/deep
          python scripts/4.2deep_scan.py --input output/middle/fast/fast_{file_base}.csv --output output/middle/deep/deep_{file_base}.csv

      - name: Run final scan
        run: |
          set -e
          mkdir -p output/middle/final
          python scripts/4.3final_scan.py --input output/middle/deep/deep_{file_base}.csv --output output/middle/final/final_{file_base}.csv --cache_dir output/cache

      - name: Commit and push changes
        env:
          GITHUB_TOKEN: ${{{{ secrets.GITHUB_TOKEN }}}}
        run: |
          set -e || true
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # 只添加相关输出文件和缓存（如果存在）
          git add output/middle/final/final_{file_base}.csv output/cache || true
          git commit -m "ci: add scan results for {file_base}" || echo "No changes to commit"

          # 设置远程并带安全推送重试机制
          git remote set-url origin https://x-access-token:${{GITHUB_TOKEN}}@github.com/niuinyang/niuin_iptv.git

          for i in 1 2 3; do
            echo "推送尝试第 $i 次"
            if git push origin HEAD:main; then
              echo "推送成功 ✅"
              break
            else
              echo "推送失败，尝试拉取远程合并 🔄"
              git stash push -m "ci: stash before pull" || echo "stash 失败或无变更"
              if git pull --rebase origin main; then
                echo "拉取成功，准备重试推送"
                git stash pop || echo "无 stash 可弹出或 pop 失败"
              else
                echo "拉取失败，等待 30 秒后重试"
                git rebase --abort || true
                git stash pop || echo "无 stash 可弹出"
                sleep 30
              fi
            fi
          done
"""

# 🧹 清理旧的 scan_chunk_*.yml workflow 文件
print("🧹 清理旧的 workflow 文件（scan_chunk_*.yml）...")
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_chunk_[\d\-]+\.yml", f):
        try:
            os.remove(os.path.join(WORKFLOW_DIR, f))
        except Exception as e:
            print(f"删除 {f} 失败: {e}")

# 如果存在旧缓存文件，先删除（重新生成）
if os.path.exists(CACHE_FILE):
    try:
        os.remove(CACHE_FILE)
    except Exception as e:
        print(f"删除旧缓存文件失败: {e}")

# 🕒 定时分配: 起始时间（UTC）
start_hour = 19  # UTC 基准小时（保留原逻辑）
start_minute = 30
interval = 5  # 每个 chunk 相隔 5 分钟

# 收集 chunk 文件（只匹配 chunk{main}-{sub}.csv）
chunks = []
pattern = re.compile(r'^chunk(\d+)-(\d+)\.csv$', re.IGNORECASE)
if os.path.isdir(CHUNK_DIR):
    for f in os.listdir(CHUNK_DIR):
        m = pattern.match(f)
        if m:
            main = int(m.group(1))
            sub = int(m.group(2))
            chunks.append((main, sub, f))
else:
    print(f"⚠️ 找不到目录 {CHUNK_DIR}，请确认路径是否正确。")
    chunks = []

# 按主编号、子编号排序（保证顺序）
chunks.sort(key=lambda x: (x[0], x[1]))
total_chunks = len(chunks)
print(f"找到 {total_chunks} 个 chunk 文件，开始生成 workflows...")

cache_data = {}

for idx, (main, sub, filename) in enumerate(chunks, start=1):
    # 计算 cron 时间
    total_minutes = start_minute + (idx - 1) * interval
    utc_hour = start_hour + (total_minutes // 60)
    utc_min = total_minutes % 60
    # 如果跨天，模 24
    if utc_hour >= 24:
        utc_hour = utc_hour % 24
    cron = f"{utc_min} {utc_hour} * * *"

    # 文件与名字
    file_base = f"chunk{main}-{sub}"
    job_name = f"{main}_{sub}"
    workflow_filename = f"scan_chunk_{main}-{sub}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)
    chunk_path = os.path.join(CHUNK_DIR, filename).replace("\\", "/")  # 保证路径格式在 windows 下也 ok

    # 写入 workflow 文件
    try:
        with open(workflow_path, "w", encoding="utf-8") as f:
            f.write(TEMPLATE.format(
                file_base=file_base,
                cron=cron,
                utc_hour=utc_hour,
                utc_min=utc_min,
                job_name=job_name,
                chunk_path=chunk_path
            ))
        print(f"✅ 已生成 workflow: {workflow_filename}  触发时间: {cron}")
    except Exception as e:
        print(f"✖️ 写入 {workflow_filename} 失败: {e}")
        continue

    cache_data[file_base] = {"cron": cron, "workflow": workflow_filename, "source": chunk_path}

# 写入缓存文件
try:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)
    print(f"\n📦 已写入缓存文件: {CACHE_FILE}")
except Exception as e:
    print(f"写入缓存文件失败: {e}")

# 🌀 提交生成的 workflow 和缓存文件到 GitHub（本地执行 git 操作）
print("\n🌀 尝试提交并推送生成的 workflow 和缓存文件到 GitHub...\n")

subprocess.run(["git", "add", "-A"], check=False)
subprocess.run(["git", "status"], check=False)
commit_msg = "ci: auto-generate scan chunk workflows"
result = subprocess.run(["git", "commit", "-m", commit_msg], text=True)
if result.returncode == 0:
    print("✅ 已提交更改，准备推送...")
else:
    print("ℹ️ 无更改或提交失败，跳过提交")

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

print("\n🎯 完成。")
