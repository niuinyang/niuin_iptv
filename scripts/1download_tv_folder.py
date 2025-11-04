import requests
import os
import json

# ==============================
# 配置区
# ==============================
REPO = "fanmingming/live"        # GitHub 仓库
FOLDER_IN_REPO = "tv"            # 仓库内要下载的文件夹
OUTPUT_DIR = "png"                # 本地保存目录
BRANCH = "main"                   # 分支
HEADERS = {"User-Agent": "Python"}
HASH_FILE = os.path.join(OUTPUT_DIR, ".hashes.json")
RETRY_TIMES = 3

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================
# 读取本地 hash
# ==============================
if os.path.exists(HASH_FILE):
    with open(HASH_FILE, "r") as f:
        local_hashes = json.load(f)
else:
    local_hashes = {}

updated_hashes = local_hashes.copy()

# ==============================
# 获取 GitHub 文件列表
# ==============================
api_url = f"https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
print(f"📡 获取 GitHub 文件列表: {api_url}")
r = requests.get(api_url, headers=HEADERS)
r.raise_for_status()
tree = r.json().get("tree", [])

# ==============================
# 下载文件
# ==============================
for file in tree:
    path, sha, type_ = file["path"], file["sha"], file["type"]
    if type_ != "blob" or not path.startswith(FOLDER_IN_REPO + "/"):
        continue

    # 本地路径
    rel_path = os.path.relpath(path, FOLDER_IN_REPO)
    local_path = os.path.join(OUTPUT_DIR, rel_path)

    # 文件已存在且 hash 相同，跳过
    if local_hashes.get(path) == sha and os.path.exists(local_path):
        print(f"✔ 已存在，跳过: {rel_path}")
        continue

    # 下载文件
    raw_url = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/{path}"
    success = False
    for attempt in range(RETRY_TIMES):
        try:
            r_file = requests.get(raw_url, headers=HEADERS, timeout=15)
            r_file.raise_for_status()
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(r_file.content)
            updated_hashes[path] = sha
            print(f"⬇ 下载完成: {rel_path}")
            success = True
            break
        except Exception as e:
            print(f"⚠️ 下载失败 {attempt+1}/{RETRY_TIMES}: {rel_path} ({e})")
    if not success:
        print(f"❌ 下载失败，跳过: {rel_path}")

# ==============================
# 保存最新 hash
# ==============================
with open(HASH_FILE, "w") as f:
    json.dump(updated_hashes, f, indent=2)

print("✅ 增量下载完成！")
