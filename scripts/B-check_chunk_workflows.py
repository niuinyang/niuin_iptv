#!/usr/bin/env python3
import os
import sys
import requests

# 配置区，和 workflow 环境变量对应
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER") or "niuinyang"    # 请替换为你的用户名或组织名
REPO_NAME = os.getenv("REPO_NAME") or "niuin_iptv"      # 请替换为你的仓库名
WORKFLOW_NAME_PREFIX = "hash-chunk"  # 改成你要检查的 workflow 名字前缀

API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"token {GITHUB_TOKEN}",
}

def get_workflows():
    url = f"{API_BASE}/actions/workflows"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_workflow_runs(workflow_id):
    url = f"{API_BASE}/actions/workflows/{workflow_id}/runs?status=completed&per_page=10"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def main():
    workflows = get_workflows()
    workflows_list = workflows.get("workflows", [])
    
    # 找出所有 chunk workflow（名字以 hash-chunk 开头）
    chunk_workflows = [w for w in workflows_list if w["name"].startswith(WORKFLOW_NAME_PREFIX)]
    if not chunk_workflows:
        print(f"❌ 没有找到名字以'{WORKFLOW_NAME_PREFIX}'开头的 workflow")
        sys.exit(1)
    
    print(f"找到 {len(chunk_workflows)} 个 chunk workflows，开始检查状态...")
    
    all_passed = True
    for wf in chunk_workflows:
        wf_id = wf["id"]
        wf_name = wf["name"]
        
        runs = get_workflow_runs(wf_id).get("workflow_runs", [])
        if not runs:
            print(f"⚠️ Workflow {wf_name} 没有任何运行记录")
            all_passed = False
            continue
        
        latest_run = runs[0]
        conclusion = latest_run.get("conclusion")
        print(f"Workflow {wf_name} 最近一次运行状态：{conclusion}")
        
        if conclusion != "success":
            print(f"⚠️ Workflow {wf_name} 最近运行没有成功！")
            all_passed = False
    
    if all_passed:
        print("✅ 所有 chunk workflows 都成功完成")
        sys.exit(0)
    else:
        print("❌ 存在未成功完成的 chunk workflows")
        sys.exit(2)

if __name__ == "__main__":
    main()
