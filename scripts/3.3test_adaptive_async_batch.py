import aiohttp
import asyncio
import csv
import os
import time
from datetime import datetime

# ==============================
# 配置区
# ==============================
INPUT_FILE = "output/merge_total.csv"
OUTPUT_DIR = "output"
WORKING_FILE = os.path.join(OUTPUT_DIR, "working.csv")
WORKING_M3U = os.path.join(OUTPUT_DIR, "working.m3u")
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
os.makedirs(LOG_DIR, exist_ok=True)

SKIPPED_FILE = os.path.join(LOG_DIR, "skipped.log")
ERROR_FILE = os.path.join(LOG_DIR, "error.log")

# 并发参数
MAX_CONCURRENCY = 40  # 异步并发数量
TIMEOUT = 8           # 超时时间(秒)

# 清晰度过滤：跳过 1080p 以下
LOW_RES_KEYWORDS = [
    "vga", "270p", "360p", "396p", "406p", "480p",
    "540p", "576p", "576i", "614p", "720p", "sd"
]
BLOCK_KEYWORDS = ["espanol"]
WHITELIST_PATTERNS = [".ctv", ".sdserver", ".sdn.", ".sda.", ".sdstream", "sdhd", "hdsd"]

# ==============================
# 工具函数
# ==============================
def log_to_file(path, msg):
    with open(path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def is_allowed(title, url):
    text = f"{title} {url}".lower()
    if any(w in text for w in WHITELIST_PATTERNS):
        return True
    if any(kw in text for kw in LOW_RES_KEYWORDS):
        log_to_file(SKIPPED_FILE, f"LOW_RESOLUTION_FILTER -> {title} | {url}")
        return False
    if any(kw in text for kw in BLOCK_KEYWORDS):
        log_to_file(SKIPPED_FILE, f"BLOCK_KEYWORD -> {title} | {url}")
        return False
    return True

# ==============================
# 核心异步检测
# ==============================
async def check_stream(session, sem, row):
    async with sem:
        name, url, source, logo = row
        if not is_allowed(name, url):
            return None

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.google.com/",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }

        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    detect_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"✅ 成功: {name}")
                    return [name, url, source, logo, detect_time, "网络源"]
                else:
                    log_to_file(ERROR_FILE, f"{resp.status} ❌ {name} -> {url}")
                    return None
        except Exception as e:
            log_to_file(ERROR_FILE, f"异常 {name} -> {url} | {str(e)}")
            return None

# ==============================
# 主任务控制
# ==============================
async def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 未找到输入文件: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required_cols = ["频道名", "地址", "来源", "图标"]
        for col in required_cols:
            if col not in reader.fieldnames:
                raise ValueError(f"CSV 文件缺少 required 列: '{col}'")

        rows = [[r["频道名"], r["地址"], r["来源"], r["图标"]] for r in reader]

    print(f"📊 读取源: {len(rows)} 条")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = [check_stream(session, sem, row) for row in rows]
        results = await asyncio.gather(*tasks)

    working = [r for r in results if r]
    print(f"\n✅ 有效源: {len(working)} 条")

    # 写 CSV
    with open(WORKING_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标", "检测时间", "分组"])
        writer.writerows(working)

    # 写 M3U
    with open(WORKING_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for name, url, src, logo, t, grp in working:
            f.write(f'#EXTINF:-1 tvg-logo="{logo}",{name}\n{url}\n')

    print(f"📁 输出: {WORKING_FILE} 和 {WORKING_M3U}")
    print(f"🕒 检测完成，共 {len(working)} 条有效源。")

if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    print(f"\n⏱️ 总耗时: {time.time() - start:.2f} 秒")