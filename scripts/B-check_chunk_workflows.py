#!/usr/bin/env python3
import os
import sys
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME_FULL = os.getenv("REPO_NAME_FULL")

if not GITHUB_TOKEN:
    print("❌ 请设置环境变量 GITHUB_TOKEN")
    sys.exit(10)

if not REPO_OWNER or not REPO_NAME_FULL:
    print("❌ 缺少环境变量 REPO_OWNER 或 REPO_NAME_FULL")
    sys.exit(10)

try:
    repo_owner_from_full, repo_name = REPO_NAME_FULL.split("/")
except ValueError:
    print(f"❌ 环境变量 REPO_NAME_FULL 格式错误，应为 'owner/repo'，当前为: {REPO_NAME_FULL}")
    sys.exit(11)

if repo_owner_from_full != REPO_OWNER:
    print(f"⚠️ 环境变量 REPO_OWNER 与 REPO_NAME_FULL 中的 owner 不一致: {REPO_OWNER} vs {repo_owner_from_full}")

API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{repo_name}"
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"token {GITHUB_TOKEN}",
}

WORKFLOW_NAME_PREFIX = "scan_chunk-"  # 按实际前缀修改

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
    main()
