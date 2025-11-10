#!/usr/bin/env python3
# scripts/generate_chunk_workflows.py
import os
import re
import argparse
from datetime import datetime
import subprocess
import json

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"
CACHE_FILE = "output/cache_workflow.json"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)

TEMPLATE = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '0 20 * * *'  # 每天 UTC 20:00（东八区 04:00）
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

      - name: Install dependencies
        run: |
          pip install -r requirements.txt

      - name: Run deep validation for chunk {n}
        run: |
          python scripts/4.3final_scan.py --input {chunk_file}

      - name: Commit and push results
        env:
          PUSH_TOKEN: ${{{{ secrets.PUSH_TOKEN }}}}
          REPO: ${{{{ github.repository }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git pull --rebase
          git add output/chunk_final_scan/
          git commit -m "ci: add final scan results chunk {n}" || echo "No changes"
          git remote set-url origin https://x-access-token:${{{{ env.PUSH_TOKEN }}}}@github.com/${{{{ env.REPO }}}}.git
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

def generate_workflows(add_timestamp=False):
    cache = load_cache()
    for filename in sorted(os.listdir(CHUNK_DIR)):
        match = re.match(r"chunk_(\d+)\.csv$", filename)
        if not match:
            print(f"跳过不匹配的文件: {filename}")
            continue

        n = match.group(1)
        # 修改这里，改成 deep_chunk_{n}.yml
        workflow_filename = f"deep_chunk_{n}.yml"

        workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)
        chunk_file_path = os.path.join(CHUNK_DIR, filename)

        cache_key = f"chunk_{n}"
        if cache.get(cache_key) == workflow_filename and os.path.exists(workflow_path):
            print(f"已存在且缓存一致: {workflow_filename}")
            continue

        with open(workflow_path, "w", encoding="utf-8") as wf:
            wf.write(TEMPLATE.format(n=n, chunk_file=chunk_file_path))
        cache[cache_key] = workflow_filename
        print(f"✅ 已生成 workflow: {workflow_filename}")

    save_cache(cache)

def git_commit_push():
    print("\n🌀 提交生成的 workflow 到 GitHub...")
    try:
        subprocess.run(["git", "pull", "--rebase"], check=True)
        subprocess.run(["git", "add", ".github/workflows"], check=True)
        subprocess.run(["git", "commit", "-m", "ci: auto-generate deep validation workflows"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✅ 已推送到远程仓库")
    except subprocess.CalledProcessError as e:
        print("⚠️ Git 操作失败:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--add-timestamp", action="store_true", help="在 workflow 文件名中加入时间戳（已忽略）")
    parser.add_argument("--no-push", action="store_true", help="仅生成，不执行 git push")
    args = parser.parse_args()

    generate_workflows(add_timestamp=args.add_timestamp)

    if not args.no_push:
        git_commit_push()
