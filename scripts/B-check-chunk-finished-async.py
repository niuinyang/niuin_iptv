#!/usr/bin/env python3
import os
import re
import asyncio
import aiohttp
import time

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

CHECK_INTERVAL = 20        # 每轮检查间隔（秒）
MAX_ROUNDS = 240            # 最多检查次数（240 次 * 20 秒 = 80 分钟）


async def fetch_latest_run(session, workflow_file):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{workflow_file}/runs?per_page=1"
    headers = {"Authorization": f"token {TOKEN}"}

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return workflow_file, None, None

            data = await resp.json()
            runs = data.get("workflow_runs", [])
            if not runs:
                return workflow_file, None, None

            latest = runs[0]
            return workflow_file, latest["status"], latest["conclusion"]

    except Exception as e:
        return workflow_file, None, None


async def check_all_finished():
    """执行一次检查，返回 True/False"""
    workflows = [
        f for f in os.listdir(WORKFLOW_DIR)
        if PATTERN.search(f)
    ]

    if not workflows:
        print("❌ No chunk workflow files found.")
        return False

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

    return all_done


async def main():
    print("🚀 Starting async chunk workflow monitor (auto-loop mode)...")
    start_time = time.time()

    for round_idx in range(1, MAX_ROUNDS + 1):
        print(f"\n🔎 Round {round_idx}/{MAX_ROUNDS} checking...")

        finished = await check_all_finished()

        if finished:
            print("\n🎉 All chunk workflows completed!")
            print("🔧 Running merge script C-merge_cache.py ...")
            os.system("python scripts/C-merge_cache.py")
            return

        print(f"⏳ Not done yet. Waiting {CHECK_INTERVAL} sec...\n")
        await asyncio.sleep(CHECK_INTERVAL)

    print("❌ Timeout: Some workflows did not finish in time.")
    exit(1)


asyncio.run(main())
