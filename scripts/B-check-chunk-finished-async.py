#!/usr/bin/env python3
import os
import re
import asyncio
import aiohttp

TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = os.getenv("REPO_OWNER")
REPO = os.getenv("REPO_NAME")

if not TOKEN or not OWNER or not REPO:
    print("❌ Missing environment variables.")
    print("TOKEN:", TOKEN)
    print("OWNER:", OWNER)
    print("REPO:", REPO)
    exit(1)

WORKFLOW_DIR = ".github/workflows"
PATTERN = re.compile(r"hash-chunk", re.IGNORECASE)


async def fetch_latest_run(session, workflow_file):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{workflow_file}/runs?per_page=1"
    headers = {"Authorization": f"token {TOKEN}"}

    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            print(f"⚠️ Failed to get runs for {workflow_file}, status={resp.status}")
            return workflow_file, None, None

        data = await resp.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            return workflow_file, None, None

        latest = runs[0]
        return workflow_file, latest["status"], latest["conclusion"]


async def main():
    # 1. 获取所有 chunk workflow 文件
    workflows = [
        f for f in os.listdir(WORKFLOW_DIR)
        if PATTERN.search(f)
    ]

    if not workflows:
        print("❌ No chunk workflow files found.")
        exit(1)

    print(f"🔍 Found {len(workflows)} chunk workflows")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_latest_run(session, wf) for wf in workflows]
        results = await asyncio.gather(*tasks)

    all_done = True

    for workflow_file, status, conclusion in results:
        if status is None:
            print(f"⚠️ {workflow_file}: No runs found")
            all_done = False
        else:
            print(f"📌 {workflow_file}: status={status}, conclusion={conclusion}")
            if status != "completed":
                all_done = False

    if all_done:
        print("🎉 All chunk workflows completed!")
        # ↓↓↓ 这里不要缩进错，只需 8 spaces 或一个 indent 块
        os.system("python scripts/C-merge_cache.py")
    else:
        print("⏳ Some workflows are still running.")


asyncio.run(main())
