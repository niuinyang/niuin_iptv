#!/usr/bin/env python3
import os
import sys
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER") or "niuinyang"    # 请根据实际改
REPO_NAME = os.getenv("REPO_NAME") or "niuin_iptv"      # 请根据实际改

WORKFLOW_NAME_PREFIX = "scan_chunk-"  # 你生成的 chunk workflow 名称前缀

API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"token {GITHUB_TOKEN}",
}

def get_workflows():
    url = f"{API_BASE}/actions/workflows"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def get_latest_workflow_run_status(workflow_id):
    url = f"{API_BASE}/actions/workflows/{workflow_id}/runs?status=completed&per_page=1"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    runs = resp.json().get("workflow_runs", [])
    if not runs:
        return None
    return runs[0].get("conclusion")

def main():
    workflows = get_workflows().get("workflows", [])
    chunk_workflows = [wf for wf in workflows if wf["name"].startswith(WORKFLOW_NAME_PREFIX)]

    if not chunk_workflows:
        print(f"❌ 未找到任何以 '{WORKFLOW_NAME_PREFIX}' 开头的 workflow")
        sys.exit(1)

    print(f"找到 {len(chunk_workflows)} 个 chunk workflows，开始检查状态...")

    all_success = True
    for wf in chunk_workflows:
        status = get_latest_workflow_run_status(wf["id"])
        if status is None:
            print(f"⚠️ Workflow '{wf['name']}' 没有运行记录")
            all_success = False
        elif status != "success":
            print(f"⚠️ Workflow '{wf['name']}' 最新运行状态为 '{status}'，非成功")
            all_success = False
        else:
            print(f"✅ Workflow '{wf['name']}' 最新运行成功")

    if all_success:
        print("🎉 所有 chunk workflows 都已成功完成！")
        sys.exit(0)
    else:
        print("❌ 存在未完成或失败的 chunk workflows")
        sys.exit(2)

if __name__ == "__main__":
    if not GITHUB_TOKEN:
        print("❌ 请设置环境变量 GITHUB_TOKEN")
        sys.exit(10)
    main()
