#!/usr/bin/env python3
import os
import sys
import requests
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+

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

WORKFLOW_NAME_PREFIX = "hash-chunk"  # 按实际前缀修改

# 东八区时区对象
BJ_TZ = ZoneInfo("Asia/Shanghai")

def get_workflows():
    url = f"{API_BASE}/actions/workflows"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def get_latest_valid_workflow_run_status(workflow_id, timepoint):
    """
    查询某 workflow 最近几条完成运行记录，筛选当天(北京时间)且名字包含时间点的记录，返回结论。
    """
    url = f"{API_BASE}/actions/workflows/{workflow_id}/runs?status=completed&per_page=5"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    runs = resp.json().get("workflow_runs", [])
    today_bj = datetime.now(BJ_TZ).date()

    for run in runs:
        run_name = run['name']
        # 只看名字包含时间点的 workflow run
        if f"-{timepoint}" not in run_name:
            continue
        # 解析运行结束时间，GitHub 返回时间是 UTC 格式：2025-11-17T01:00:00Z
        run_completed_utc = datetime.strptime(run['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
        run_completed_bj = run_completed_utc.astimezone(BJ_TZ).date()
        # 只接受当天完成的运行
        if run_completed_bj == today_bj:
            return run.get("conclusion")
    # 没有当天的符合条件运行记录
    return None

def main():
    parser = argparse.ArgumentParser(description="检查 chunk workflows 状态，仅检查当天对应时间点的运行结果")
    parser.add_argument("--timepoint", required=True, choices=["0811","1612","2113"], help="当前时间点")
    args = parser.parse_args()

    workflows = get_workflows().get("workflows", [])
    chunk_workflows = [wf for wf in workflows if wf["name"].startswith(WORKFLOW_NAME_PREFIX)]

    if not chunk_workflows:
        print(f"❌ 未找到任何以 '{WORKFLOW_NAME_PREFIX}' 开头的 workflow")
        sys.exit(1)

    print(f"找到 {len(chunk_workflows)} 个 chunk workflows，开始检查状态 (仅当日时间点 {args.timepoint}) ...")

    all_success = True
    for wf in chunk_workflows:
        # 只检查名字包含当前时间点的 workflow
        if f"-{args.timepoint}" not in wf["name"]:
            # 跳过非当前时间点的 workflow
            continue

        status = get_latest_valid_workflow_run_status(wf["id"], args.timepoint)
        if status is None:
            print(f"⚠️ Workflow '{wf['name']}' 没有当天运行记录或未完成")
            all_success = False
        elif status != "success":
            print(f"⚠️ Workflow '{wf['name']}' 最新当天运行状态为 '{status}'，非成功")
            all_success = False
        else:
            print(f"✅ Workflow '{wf['name']}' 最新当天运行成功")

    if all_success:
        print("🎉 所有当天对应时间点的 chunk workflows 都已成功完成！")
        sys.exit(0)
    else:
        print("❌ 存在未完成或失败的 chunk workflows")
        sys.exit(2)

if __name__ == "__main__":
    main()