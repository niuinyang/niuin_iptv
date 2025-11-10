#!/usr/bin/env python3
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
os.makedirs("output/cache", exist_ok=True)

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
        with:
          fetch-depth: 0
          persist-credentials: false

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
          python scripts/4.3final_scan.py --input {chunk_file} --chunk_id {n} --cache_dir output/cache

      - name: Add, commit and push scan results and cache files
        env:
          PUSH_TOKEN1: ${{{{ secrets.PUSH_TOKEN1 }}}}
          REPO: ${{{{ github.repository }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git remote set-url origin https://x-access-token:${{ secrets.PUSH_TOKEN1 }}@github.com/${{ github.repository }}.git

          max_retries=3
          wait_seconds=5
          attempt=1

          while [ $attempt -le $max_retries ]; do
            echo "尝试拉取远程合并，尝试次数: $attempt"
            git fetch origin main
            if git merge --ff-only origin/main; then
              echo "拉取合并成功"
              break
            else
              echo "合并失败，等待 $wait_seconds 秒后重试..."
              sleep $wait_seconds
              attempt=$((attempt + 1))
            fi
          done

          if [ $attempt -gt $max_retries ]; then
            echo "⚠️ 达到最大重试次数，合并失败，跳过推送"
            exit 0
          fi

          git add output/chunk_final_scan/working_chunk_{n}.csv output/chunk_final_scan/final_chunk_{n}.csv output/chunk_final_scan/final_invalid_chunk_{n}.csv output/cache/chunk/cache_hashes_chunk_{n}.json || echo "No scan result or cache files to add"
          git commit -m "ci: add final scan results and cache chunk {n}" || echo "No changes in scan results or cache"
          git push || echo "Push skipped"
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

    chunk_files = []
    for filename in os.listdir(CHUNK_DIR):
        match = re.match(r"chunk_(\d+)\.csv$", filename)
        if not match:
            print(f"跳过不匹配的文件: {filename}")
            continue
        chunk_files.append((int(match.group(1)), filename))
    chunk_files.sort(key=lambda x: x[0])

    start_hour = 19  # UTC时间 19:30 对应东八区凌晨3:30
    start_minute = 30
    interval_min = 10

    for idx, (n_int, filename) in enumerate(chunk_files):
        n = str(n_int)
        workflow_filename = f"deep_chunk_{n}.yml"
        workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)
        chunk_file_path = os.path.join(CHUNK_DIR, filename)

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
    print("\n🌀 提交生成的 workflow 和缓存文件 到 GitHub...")

    try:
        subprocess.run(["git", "reset", "--hard"], check=True)
        subprocess.run(["git", "clean", "-fd"], check=True)

        # 配置 git 用户身份
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)

        subprocess.run(["git", "pull", "--rebase"], check=True)
        subprocess.run(["git", "add", WORKFLOW_DIR], check=True)
        subprocess.run(["git", "add", "output/cache"], check=True)
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