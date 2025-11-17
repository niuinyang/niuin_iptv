#!/usr/bin/env python3
import os
import sys
import time
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME_FULL = os.getenv("REPO_NAME_FULL")
COMMIT_SHA = os.getenv("COMMIT_SHA")

if not all([GITHUB_TOKEN, REPO_OWNER, REPO_NAME_FULL, COMMIT_SHA]):
    print("❌ 缺少环境变量 GITHUB_TOKEN / REPO_OWNER / REPO_NAME_FULL / COMMIT_SHA")
    sys.exit(10)

try:
    owner_from_full, repo_name = REPO_NAME_FULL.split("/")
except ValueError:
    print(f"❌ REPO_NAME_FULL 格式错误: {REPO_NAME_FULL}")
    sys.exit(11)

API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{repo_name}"
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"token {GITHUB_TOKEN}",
}

WORKFLOW_NAME_PREFIX = "Scan_chunk"
POLL_INTERVAL = 20       # 秒
TIMEOUT = 3600           # 秒，1小时

def github_get(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def get_scan_chunk_workflows():
    wf_list = github_get(f"{API_BASE}/actions/workflows")["workflows"]
    return [wf for wf in wf_list if wf["name"].startswith(WORKFLOW_NAME_PREFIX)]

def get_workflow_runs_for_sha(wf_id, sha):
    url = f"{API_BASE}/actions/workflows/{wf_id}/runs?per_page=100&head_sha={sha}"
    data = github_get(url)
    return data.get("workflow_runs", [])

def main():
    print(f"开始轮询检测，等待所有以'{WORKFLOW_NAME_PREFIX}'开头的 workflows 对 commit {COMMIT_SHA} 完成，超时设置：{TIMEOUT}秒")

    workflows = get_scan_chunk_workflows()
    if not workflows:
        print(f"❌ 未找到任何以 '{WORKFLOW_NAME_PREFIX}' 开头的 workflow")
        sys.exit(1)

    workflow_count = len(workflows)
    print(f"🔍 找到 {workflow_count} 个 chunk workflows，开始轮询...")

    start_time = time.time()

    while True:
        all_done = True
        all_success = True

        for wf in workflows:
            runs = get_workflow_runs_for_sha(wf["id"], COMMIT_SHA)
            if not runs:
                print(f"⚠️ {wf['name']}：无匹配本 commit {COMMIT_SHA} 的运行，等待中")
                all_done = False
                continue

            latest_run = runs[0]  # 按时间最新

            status = latest_run["status"]  # queued, in_progress, completed
            conclusion = latest_run.get("conclusion")

            if status != "completed":
                print(f"⏳ {wf['name']}：状态={status}，仍在运行")
                all_done = False
                continue

            if conclusion != "success":
                print(f"❌ {wf['name']}：运行失败，conclusion={conclusion}")
                all_success = False
            else:
                print(f"✅ {wf['name']}：运行成功")

        print()

        if all_done:
            if all_success:
                print("🎉 所有 workflows 均成功完成！")
                sys.exit(0)
            else:
                print("❌ 有 workflows 运行失败")
                sys.exit(2)

        if time.time() - start_time > TIMEOUT:
            print("⛔ 超时 1 小时，部分 workflows 未完成")
            sys.exit(3)

        print(f"⌛ 等待 {POLL_INTERVAL} 秒后继续轮询...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()