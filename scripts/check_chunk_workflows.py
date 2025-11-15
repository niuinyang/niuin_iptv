#!/usr/bin/env python3
import os
import time
import requests
import sys

GITHUB_API = "https://api.github.com"
TOKEN = os.environ.get("GITHUB_TOKEN")
OWNER = os.environ.get("REPO_OWNER")
REPO = os.environ.get("REPO_NAME")
WORKFLOWS = os.environ.get("WORKFLOWS", "").split(',')

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

def check_workflow_completed(workflow_file):
    url = f"{GITHUB_API}/repos/{OWNER}/{REPO}/actions/workflows/{workflow_file}/runs"
    try:
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"Error fetching runs for {workflow_file}: {e}")
        return False

    runs = resp.json().get("workflow_runs", [])
    if not runs:
        print(f"No runs found for workflow {workflow_file}")
        return False
    latest = runs[0]
    status = latest.get("status")
    conclusion = latest.get("conclusion")
    print(f"Workflow {workflow_file} latest run: status={status}, conclusion={conclusion}")
    return status == "completed" and conclusion == "success"

def all_completed():
    for wf in WORKFLOWS:
        wf = wf.strip()
        if not check_workflow_completed(wf):
            return False
    return True

def main():
    max_retries = 30
    interval = 10  # seconds
    print(f"Checking workflows: {WORKFLOWS}")
    for i in range(max_retries):
        if all_completed():
            print("All chunk workflows completed successfully.")
            sys.exit(0)
        print(f"Waiting for chunk workflows to complete... attempt {i+1}/{max_retries}")
        time.sleep(interval)
    print("Timeout waiting for chunk workflows to complete.")
    sys.exit(1)

if __name__ == "__main__":
    main()