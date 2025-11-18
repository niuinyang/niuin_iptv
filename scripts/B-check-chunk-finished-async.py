#!/usr/bin/env python3
# scripts/B-check-chunk-finished-async.py
# Async concurrent checker for chunk workflows
# Requires: aiohttp

import os
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Dict

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or os.getenv("PUSH_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER", "niuinyang")
REPO_NAME = os.getenv("REPO_NAME", "niuin_iptv")
COMMIT_SHA = os.getenv("COMMIT_SHA")
PARENT_CREATED_AT = os.getenv("PARENT_CREATED_AT")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "20"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "12"))


HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "niuin-iptv-checker",
}


def parse_iso(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


async def github_get(session: aiohttp.ClientSession, url: str):
    async with session.get(url, headers=HEADERS) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"GitHub API error {resp.status} for {url}: {text}")
        return await resp.json()


async def list_chunk_workflows(session: aiohttp.ClientSession) -> List[Dict]:
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows"
    data = await github_get(session, url)
    wfs = [
        {"name": wf["name"], "id": wf["id"]}
        for wf in data.get("workflows", [])
        if wf.get("name", "").startswith("hash-chunk-")
    ]
    return wfs


async def find_run_for_workflow(session: aiohttp.ClientSession, wf: Dict, parent_dt: datetime, commit_sha: str):
    wf_id = wf["id"]
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/{wf_id}/runs?per_page=30"
    data = await github_get(session, url)
    runs = data.get("workflow_runs", [])

    # 优先匹配 head_sha
    for r in runs:
        if commit_sha and r.get("head_sha") == commit_sha:
            return r

    # 再用时间匹配
    for r in runs:
        created = parse_iso(r.get("created_at", ""))
        if created and parent_dt and created >= parent_dt:
            return r

    # fallback
    if runs:
        return runs[0]
    return None


async def poll_run_until_done(session: aiohttp.ClientSession, run_id: int):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/runs/{run_id}"
    while True:
        data = await github_get(session, url)
        status = data.get("status")
        conclusion = data.get("conclusion")
        if status == "completed":
            return conclusion
        await asyncio.sleep(CHECK_INTERVAL)


async def main_async():
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN or PUSH_TOKEN must be set in env")

    parent_dt = parse_iso(PARENT_CREATED_AT) if PARENT_CREATED_AT else None

    async with aiohttp.ClientSession() as session:
        wfs = await list_chunk_workflows(session)
        if not wfs:
            raise RuntimeError("No chunk workflows found (prefix 'hash-chunk-')")

        print(f"Found {len(wfs)} chunk workflows. Resolving their triggered runs...")

        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker_find(wf):
            async with sem:
                try:
                    r = await find_run_for_workflow(session, wf, parent_dt, COMMIT_SHA)
                    if r:
                        print(f"Workflow {wf['name']} -> run_id {r['id']} created_at {r.get('created_at')}")
                        return wf['name'], r['id']
                    else:
                        print(f"Workflow {wf['name']} -> no run found")
                        return wf['name'], None
                except Exception as e:
                    print(f"Error finding run for {wf['name']}: {e}")
                    return wf['name'], None

        results = await asyncio.gather(*[worker_find(wf) for wf in wfs])
        runs_map = {name: rid for name, rid in results if rid}

        if not runs_map:
            raise RuntimeError("No runs matched for any chunk workflow (check commit/time filters)")

        print(f"\nMonitoring {len(runs_map)} runs concurrently...")

        async def worker_poll(name, rid):
            async with sem:
                print(f"Start polling {name} run {rid}")
                try:
                    conclusion = await poll_run_until_done(session, rid)
                    print(f"{name} run {rid} finished with conclusion: {conclusion}")
                    return name, rid, conclusion
                except Exception as e:
                    print(f"Error polling {name} run {rid}: {e}")
                    return name, rid, "error"

        poll_tasks = [worker_poll(n, rid) for n, rid in runs_map.items()]
        poll_results = await asyncio.gather(*poll_tasks)

        failed = [r for r in poll_results if r[2] != "success"]
        if failed:
            print("Some runs failed or not successful:")
            for name, rid, concl in failed:
                print(f" - {name} {rid} -> {concl}")
            raise RuntimeError("One or more chunk workflows did not succeed")
        else:
            print("All chunk workflows succeeded.")
            return True


if __name__ == "__main__":
    import sys
    try:
        ok = asyncio.run(main_async())
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
    if ok:
        print("All done.")
        sys.exit(0)