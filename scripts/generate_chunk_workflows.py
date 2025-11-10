#!/usr/bin/env python3
# scripts/generate_chunk_workflows.py
import os
import re
import argparse
import json
import subprocess
import time

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)

TEMPLATE = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '{cron_min} {cron_hour} * * *'  # 触发时间，UTC时间
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
        run: |
          pip install -r requirements.txt

      - name: Run deep validation for chunk {n}
        run: |
          python scripts/4.3final_scan.py --input {chunk_file} --chunk_id {n} --cache_dir output/cache
"""

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def generate_workflows():
    cache = load_cache()

    # 统计所有 chunk 文件，排序
    chunk_files = []
    for filename in os.listdir(CHUNK_DIR):
        match = re.match(r"chunk_(\d+)\.csv$", filename)
        if not match:
            print(f"跳过不匹配的文件: {filename}")
            continue
        chunk_files.append((int(match.group(1)), filename))
    chunk_files.sort(key=lambda x: x[0])

    # 计算触发时间，起点 UTC 19:30，对应东八区凌晨3:30，间隔10分钟
    start_hour = 19
    start_minute = 30
    interval_min = 10

    for idx, (n_int, filename) in enumerate(chunk_files):
        n = str(n_int)
        workflow_filename = f"deep_chunk_{n}.yml"
        workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)
        chunk_file_path = os.path.join(CHUNK_DIR, filename)

        # 计算cron时间
        total_minutes = start_minute + idx * interval_min
        cron_hour = start_hour + total_minutes // 60
        cron_min = total_minutes % 60
        if cron_hour >= 24:
            cron_hour = cron_hour % 24

        cache_key = f"chunk_{n}"
        if cache.get(cache_key) == workflow_filename and os.path.exists(workflow_path):
            print(f"已存在且缓存一致: {workflow_filename} 触发时间: {cron_min} {cron_hour} * * *")
            continue

        with open(workflow_path, "w", encoding="utf-8") as wf:
            wf.write(TEMPLATE.format(n=n, chunk_file=chunk_file_path, cron_hour=cron_hour, cron_min=cron_min))
        cache[cache_key] = workflow_filename
        print(f"✅ 已生成 workflow: {workflow_filename} 触发时间: {cron_min} {cron_hour} * * *")

    save_cache(cache)

def git_commit_push(max_retries=3, wait_seconds=5):
    print("\n🌀 提交生成的 workflow 到 GitHub...")

    try:
        # 先强制清理本地改动，确保pull不报错
        subprocess.run(["git", "reset", "--hard"], check=True)
        subprocess.run(["git", "clean", "-fd"], check=True)

        subprocess.run(["git", "pull", "--rebase"], check=True)

        subprocess.run(["git", "add", ".github/workflows"], check=True)
        subprocess.run(["git", "add", "output/cache"], check=True)  # 添加缓存目录
        subprocess.run(["git", "commit", "-m", "ci: auto-generate deep validation workflows"], check=False)
    except subprocess.CalledProcessError as e:
        print("⚠️ Git 预处理失败:", e)
        return

    for attempt in range(1, max_retries + 1):
        try:
            subprocess.run(["git", "push"], check=True)
            print("✅ 已成功推送到远程仓库")
            break
        except subprocess.CalledProcessError as e:
            print(f"⚠️ 第 {attempt} 次推送失败:", e)
            if attempt < max_retries:
                print(f"⏳ 等待 {wait_seconds} 秒后重试推送...")
                try:
                    subprocess.run(["git", "pull", "--rebase"], check=True)
                except subprocess.CalledProcessError as pull_err:
                    print("⚠️ 自动拉取远程最新失败，跳过重试:", pull_err)
                    break
                time.sleep(wait_seconds)
            else:
                print("❌ 达到最大重试次数，推送失败，请手动检查冲突。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-push", action="store_true", help="仅生成，不执行 git push")
    args = parser.parse_args()

    generate_workflows()

    if not args.no_push:
        git_commit_push()