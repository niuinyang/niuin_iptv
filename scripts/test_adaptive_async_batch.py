import aiohttp
import asyncio
import csv
import os
import time
from aiohttp import ClientTimeout

# ==============================
# 配置区
# ==============================
INPUT_FILE = "input/network/network_sources.csv"
WORKING_CSV = "output/working.csv"
WORKING_M3U = "output/working.m3u"
SKIPPED_LOG = "output/skipped.log"
FAILED_LOG = "output/failed.log"

MAX_CONCURRENT = 50  # 并发检测数量
TIMEOUT_SECONDS = 8  # 检测超时（秒）

# ✅ 白名单：优先保留
WHITELIST_PATTERNS = [
    "cctv", "央视", "卫视", "凤凰", "bloomberg", "bbc", "cnn",
    "discovery", "hbo", "espn", "nba", "fox", "abc"
]

# 🚫 屏蔽低清晰度关键词（包含720p）
LOW_RES_KEYWORDS = [
    "vga", "480p", "576p", "360p", "240p", "144p", "sd", "720p"
]

# 🚫 黑名单关键词（测试源、音频、成人内容等）
BLOCK_KEYWORDS = [
    "test", "offline", "cam", "porn", "xxx", "sex",
    "radio", "audio", "music", "vr", "demo"
]

# ==============================
# 日志函数
# ==============================
def log_skip(reason, title, url):
    with open(SKIPPED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{reason},{title},{url}\n")

def log_fail(reason, title, url):
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{reason},{title},{url}\n")

# ==============================
# 核心过滤逻辑
# ==============================
def is_allowed(title, url):
    text = f"{title} {url}".lower()
    # ✅ 白名单优先保留
    if any(w in text for w in WHITELIST_PATTERNS):
        return True
    # 🚫 排除低清晰度
    if any(kw in text for kw in LOW_RES_KEYWORDS):
        log_skip("LOW_RES_SKIP", title, url)
        return False
    # 🚫 排除黑名单关键词
    if any(kw in text for kw in BLOCK_KEYWORDS):
        log_skip("BLOCK_KEYWORD", title, url)
        return False
    return True

# ==============================
# 异步检测函数
# ==============================
async def check_url(session, title, url, logo):
    start = time.time()
    try:
        async with session.get(url, timeout=ClientTimeout(total=TIMEOUT_SECONDS)) as resp:
            if resp.status == 200:
                elapsed = time.time() - start
                print(f"✅ {title} 正常 ({elapsed:.2f}s)")
                return True, elapsed, url, title, logo
            else:
                log_fail(f"HTTP_{resp.status}", title, url)
    except Exception as e:
        log_fail(str(e), title, url)
    return False, None, url, title, logo

# ==============================
# 写入结果文件
# ==============================
def write_working_csv(all_working):
    with open(WORKING_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        # ✅ 按要求修改表头
        writer.writerow(["频道名", "地址", "来源", "检测时间", "图标", "分组"])
        for ok, elapsed, url, name, logo in all_working:
            if ok:
                writer.writerow([
                    name,            # 频道名
                    url,             # 地址
                    "网络源",         # 来源
                    f"{elapsed:.2f}",# 检测时间（秒）
                    logo or "",      # 图标
                    ""               # 分组（留空）
                ])
    print(f"📁 生成 working.csv: {WORKING_CSV}")

def write_working_m3u(all_working):
    with open(WORKING_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ok, elapsed, url, name, logo in all_working:
            if ok:
                logo_part = f'tvg-logo="{logo}" ' if logo else ""
                f.write(f'#EXTINF:-1 {logo_part},{name}\n{url}\n')
    print(f"📺 生成 working.m3u: {WORKING_M3U}")

# ==============================
# 主流程
# ==============================
async def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 未找到输入文件：{INPUT_FILE}")
        return

    pairs = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            title, url = row[0].strip(), row[1].strip()
            logo = row[2].strip() if len(row) > 2 else ""
            pairs.append((title, url, logo))

    print(f"📖 读取源共 {len(pairs)} 条")

    # ✅ 过滤低清晰度与黑名单
    filtered_pairs = [p for p in pairs if is_allowed(p[0], p[1])]
    print(f"🚫 跳过源: {len(pairs) - len(filtered_pairs)} 条（低清晰度或黑名单）")
    print(f"🧪 待检测源: {len(filtered_pairs)} 条")

    # 异步检测
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_url(session, name, url, logo) for name, url, logo in filtered_pairs]
        results = await asyncio.gather(*tasks)

    # 写入结果文件
    write_working_csv(results)
    write_working_m3u(results)
    print("✅ 检测完成")

# ==============================
# 运行入口
# ==============================
if __name__ == "__main__":
    asyncio.run(main())
