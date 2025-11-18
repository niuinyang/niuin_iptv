#!/usr/bin/env python3
import os
import sys
import asyncio
import aiohttp
from datetime import datetime, timezone

# ---- 强制立即输出，不缓冲 ----
sys.stdout.reconfigure(line_buffering=True)
print(">>> Script started, stdout is unbuffered.")

# ---- 读取环境变量 ----
TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = os.getenv("REPO_OWNER")
REPO = os.getenv("REPO_NAME")
PARENT_CREATED_AT = os.getenv("PARENT_CREATED_AT")

if not TOKEN or not OWNER or not REPO or not PARENT_CREATED_AT:
    print("❌ Missing required environment variables.")
    print("TOKEN:", TOKEN)
    print("OWNER:", OWNER)
    print("REPO:", REPO)
    print("PARENT_CREATED_AT:", PARENT_CREATED_AT)
    sys.exit(1)

print(f">>> Monitoring repo: {OWNER}/{REPO}")
print(f">>> Parent workflow created_at = {PARENT_CREATED_AT}")

API_BASE = f"https://api.github.com/repos/{OWNER}/{REPO}"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}


# ----------------------------------------------------------------------
# 获取仓库中所有 chunk workflow 的 workflow id
# ----------------------------------------------------------------------
async def list_chunk_workflows(session):
    url = f"{API_BASE}/actions/workflows"
    print(">>> Fetching workflow list:", url)

    async with session.get(url, headers=HEADERS) as resp:
        if resp.status != 200:
            print(f"❌ Failed to fetch workflows: HTTP {resp.status}")
            text = await resp.text()
            print(text)
            sys.exit(1)
        data = await resp.json()

    selected = []
    for wf in data.get("workflows", []):
        name = wf.get("name", "")
        if name.startswith("hash-chunk-"):
            selected.append({"id": wf["id"], "name": name})

    print(f">>> Found {len(selected)} chunk workflows:")
    for w in selected:
        print("    -", w["name"])
    return selected


# ----------------------------------------------------------------------
# 获取单个 workflow 的最新运行
# ----------------------------------------------------------------------
async def fetch_latest_run(session, workflow):
    wid = workflow["id"]
    name = workflow["name"]
    url = f"{API_BASE}/actions/workflows/{wid}/runs?per_page=1"
    print(f">>> Fetching latest run for {name}")

    async with session.get(url, headers=HEADERS) as resp:
        if resp.status != 200:
            print(f"❌ Failed to fetch runs for {name}: HTTP {resp.status}")
            return None
        data = await resp.json()

    runs = data.get("workflow_runs", [])
    if not runs:
        print(f"⚠️  No runs found for {name}")
        return None

    run = runs[0]
    run_created_at = run["created_at"]

    # 过滤：必须是 parent workflow 之后触发的 run
    if run_created_at < PARENT_CREATED_AT:
        print(f"⚠️  Ignoring old run for {name}")
        return None

    return {
        "name": name,
        "id": run["id"],
        "status": run["status"],
        "conclusion": run["conclusion"]
    }


# ----------------------------------------------------------------------
# 检查所有 chunk workflows 是否完成
# ----------------------------------------------------------------------
async def check_all_chunks():
    async with aiohttp.ClientSession() as session:
        print(">>> Pulling chunk workflow list...")
        workflows = await list_chunk_workflows(session)
        if not workflows:
            print("❌ No chunk workflows found. Exiting.")
            sys.exit(1)

        while True:
            print("\n>>> Checking workflow run status...")
            tasks = [fetch_latest_run(session, wf) for wf in workflows]
            results = await asyncio.gather(*tasks)

            pending = []
            finished = []

            for r in results:
                if not r:
                    pending.append("unknown")
                    continue

                if r["status"] != "completed":
                    pending.append(r["name"])
                else:
                    finished.append(r["name"])

            print(">>> Finished:", finished)
            print(">>> Pending:", pending)

            if len(finished) == len(workflows):
                print("\n🎉 All chunk workflows have completed!")
                return True

            print(">>> Not all finished, waiting 10 sec...\n")
            await asyncio.sleep(10)


# ----------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(check_all_chunks())
