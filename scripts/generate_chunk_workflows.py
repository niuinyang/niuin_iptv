import os
import glob
import re
from datetime import datetime

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)

template = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '0 20 * * *'  # 每天 UTC 20:00 触发，东八区凌晨4点
  workflow_dispatch:

permissions:
  contents: write

jobs:
  deep_chunk_{n}:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0  # 🔧 允许完整拉取历史记录，确保能 rebase

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install system dependencies
        run: sudo apt-get update && sudo apt-get install -y ffmpeg

      - name: Install Python dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pillow tqdm chardet

      - name: Run final scan on chunk {n}
        run: |
          python scripts/4.3final_scan.py --input {chunk_file} --output_dir output/chunk_final_scan

      - name: Commit and push scan results safely
        env:
          REPO_TOKEN: ${{{{ secrets.PERSONAL_ACCESS_TOKEN }}}}
          GITHUB_REPOSITORY: ${{{{ github.repository }}}}
          GITHUB_REF: ${{{{ github.ref }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # 拉取远程更新并自动 rebase，避免冲突
          git fetch origin ${{{{ github.ref }}}}
          git rebase origin/${{{{ github.ref }}}} || git rebase --abort

          git add output/chunk_final_scan/
          git commit -m "ci: add final scan results chunk {n}" || echo "No changes to commit"

          # 再次拉取，确保无冲突后推送
          git pull --rebase origin ${{{{ github.ref }}}} || true
          git push https://x-access-token:${{REPO_TOKEN}}@github.com/${{GITHUB_REPOSITORY}} HEAD:${{GITHUB_REF}}

      - name: Self delete workflow file
        env:
          REPO_TOKEN: ${{{{ secrets.PERSONAL_ACCESS_TOKEN }}}}
          WORKFLOW_FILE: ".github/workflows/deep_chunk_{n}.yml"
          GITHUB_REPOSITORY: ${{{{ github.repository }}}}
          GITHUB_REF: ${{{{ github.ref }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git fetch origin ${{{{ github.ref }}}}
          git rebase origin/${{{{ github.ref }}}} || git rebase --abort
          git rm "$WORKFLOW_FILE"
          git commit -m "ci: self delete workflow deep_chunk_{n}.yml after run"
          git pull --rebase origin ${{{{ github.ref }}}} || true
          git push https://x-access-token:${{REPO_TOKEN}}@github.com/${{GITHUB_REPOSITORY}} HEAD:${{GITHUB_REF}}
"""

chunk_files = sorted(glob.glob(os.path.join(CHUNK_DIR, "chunk_*.csv")))

for chunk_file in chunk_files:
    basename = os.path.basename(chunk_file)
    match = re.match(r"chunk_(\\d+)\\.csv", basename)
    if not match:
        print(f"跳过不匹配的文件: {basename}")
        continue
    n = match.group(1)
    wf_path = os.path.join(WORKFLOW_DIR, f"deep_chunk_{n}.yml")
    timestamp = datetime.now().astimezone().isoformat()
    content = template.format(n=n, chunk_file=chunk_file, timestamp=timestamp)
    with open(wf_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ 生成 workflow: {wf_path}")
