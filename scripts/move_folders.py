name: Move Folders to depend/

on:
  workflow_dispatch:  # 支持手动触发
  push:
    branches:
      - main  # 默认分支

jobs:
  move_folders_job:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.x"

      - name: Move folders
        run: python3 scripts/move_folders.py

      - name: Commit and push changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # 读取移动的文件夹列表
          folders=$(cat moved_folders.json | jq -r '.[]')

          # 添加 depend/ 下的内容
          git add else/

          # 删除旧目录
          for folder in $folders; do
            if [ -d "$folder" ]; then
              git rm -r "$folder"
            fi
          done

          # 提交，如果没变动不会报错
          git commit -m "Move folders into depend/" || echo "No changes to commit"

          git push
      continue-on-error: true
