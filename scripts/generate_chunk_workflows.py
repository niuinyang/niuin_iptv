#!/usr/bin/env python3
# scripts/generate_chunk_workflows.py
import os
import re
import argparse
import json
import subprocess

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)

def generate_cron_times(n):
    """
    生成 n 个触发时间点，每个间隔 10 分钟，从 UTC 19:30 开始
    """
    start_hour = 19  # UTC 时间，东八区凌晨3点对应小时
    start_minute = 30
    times = []
    for i in range(n):
        total_minutes = start_hour * 60 + start_minute + i * 10
        hour = total_minutes // 60
        minute = total_minutes % 60
        times.append((hour, minute))
    return times

TEMPLATE = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '{cron}'
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

      - name: Delete self workflow file
        env:
          PUSH_TOKEN: ${{{{ secrets.PUSH_TOKEN }}}}
          REPO: ${{{{ github.repository }}}}
          FILE: .github/workflows/deep_chunk_{n}.yml
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git rm "$FILE"
          git commit -m "ci: remove workflow $FILE"
          git remote set-url origin https://x-access-token:${{{{ env.PUSH_TOKEN }}}}@github.com/${{{{ env.REPO }}}}.git
          git push || echo "Push failed, possibly no changes"
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
    files = sorted(os.listdir(CHUNK_DIR))
    cron_times = generate_cron_times(len(files))

    for i, filename in enumerate(files):
        match = re.match(r"chunk_(\d+)\.csv$", filename)
        if not match:
            print(f"跳过不匹配的文件: {filename}")
            continue

        n = match.group(1)
        workflow_filename = f"deep_chunk_{n}.yml"
        workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)
        chunk_file_path = os.path.join(CHUNK_DIR, filename)

        cache_key = f"chunk_{n}"
        if cache.get(cache_key) == workflow_filename and os.path.exists(workflow_path):
            print(f"已存在且缓存一致: {workflow_filename}")
            continue

        hour, minute = cron_times[i]
        cron = f"{minute} {hour} * * *"

        content = TEMPLATE.format(n=n, chunk_file=chunk_file_path, cron=cron)

        with open(workflow_path, "w", encoding="utf-8") as wf:
            wf.write(content)
        cache[cache_key] = workflow_filename
        print(f"✅ 已生成 workflow: {workflow_filename} 触发时间: {cron}")

    save_cache(cache)

def git_commit_push():
    print("\n🌀 提交生成的 workflow 到 GitHub...")
    try:
        # 先清理本地未暂存改动，避免 pull --rebase 失败
        subprocess.run(["git", "reset", "--hard"], check=True)
        subprocess.run(["git", "clean", "-fd"], check=True)

        subprocess.run(["git", "pull", "--rebase"], check=True)
        subprocess.run(["git", "add", ".github/workflows"], check=True)
        subprocess.run(["git", "commit", "-m", "ci: auto-generate deep validation workflows"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✅ 已推送到远程仓库")
    except subprocess.CalledProcessError as e:
        print("⚠️ Git 操作失败:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-push", action="store_true", help="仅生成，不执行 git push")
    args = parser.parse_args()

    generate_workflows()

    if not args.no_push:
        git_commit_push()