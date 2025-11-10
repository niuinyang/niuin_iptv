import os
import glob
import re
from datetime import datetime, timedelta

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)

# 计算每天从3:00起，每个chunk延迟的分钟数，步长10分钟
def get_cron_for_index(index):
    # index从0开始
    base_hour = 3  # 3点
    base_minute = 0
    # 每个 chunk 间隔10分钟
    total_minutes = base_minute + index * 10
    hour = base_hour + total_minutes // 60
    minute = total_minutes % 60
    # 返回cron字符串，UTC时间(东八区3点=UTC19点)
    # 注意GitHub Actions cron是UTC时间
    # 3点(北京时间) = 19点(UTC)
    utc_hour = (hour - 8) % 24  # 转成UTC小时，保证24小时内循环
    return f"{minute} {utc_hour} * * *"

template = """name: Deep Validation Chunk {n}

on:
  schedule:
    - cron: '{cron}'  # 每天 UTC {utc_hour}:{minute:02d} 触发，东八区{hour}: {minute:02d}点
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

for idx, chunk_file in enumerate(chunk_files):
    basename = os.path.basename(chunk_file)
    match = re.match(r"chunk_(\d+)\.csv", basename)
    if not match:
        print(f"跳过不匹配的文件: {basename}")
        continue
    n = match.group(1)

    # 计算 cron 表达式
    cron = get_cron_for_index(idx)
    # 计算对应的本地东八区小时和分钟，用于注释（便于理解）
    total_minutes = idx * 10
    hour_local = 3 + total_minutes // 60
    minute_local = total_minutes % 60
    # 转成UTC小时（0-23）
    utc_hour = (hour_local - 8) % 24

    wf_path = os.path.join(WORKFLOW_DIR, f"deep_chunk_{n}.yml")
    content = template.format(
        n=n,
        chunk_file=chunk_file,
        cron=cron,
        hour=hour_local,
        minute=minute_local,
        utc_hour=utc_hour,
        timestamp=datetime.now().astimezone().isoformat()
    )
    with open(wf_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ 生成 workflow: {wf_path}")
