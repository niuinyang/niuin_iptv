import aiohttp
import asyncio
import csv
import os
import time
from datetime import datetime
from PIL import Image
import imagehash
import tempfile
import subprocess

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

MAX_CONCURRENCY = 40
TIMEOUT = 8

LOW_RES_KEYWORDS = [
    "vga", "270p", "360p", "396p", "406p", "480p",
    "540p", "576p", "576i", "614p"
]
BLOCK_KEYWORDS = ["espanol"]
WHITELIST_PATTERNS = [".ctv", ".sdserver", ".sdn.", ".sda.", ".sdstream", "sdhd", "hdsd"]

# 假源检测阈值，哈希差异小于等于此值判定为假源
FAKE_HASH_DIFF_THRESHOLD = 5

# 缓存已检测起始帧哈希：{频道名: phash}
start_frame_hash_cache = {}

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

async def ffprobe_check(url):
    """使用 ffprobe 获取流信息，只返回第一条视频流信息，避免重复和换行"""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height",
            "-of", "csv=p=0", url
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if stdout:
            lines = stdout.decode().strip().splitlines()
            if lines:
                return lines[0]
        return None
    except Exception:
        return None

async def get_start_frame_hash(url):
    """
    抓取流的第一帧截图并计算感知哈希。
    返回 imagehash.phash 对象或 None。
    """
    # 先创建临时文件用于保存帧截图
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=True) as tmpfile:
        tmp_path = tmpfile.name
        # ffmpeg 命令抓取第一帧，-y覆盖文件，-frames:v 1 抓1帧，-loglevel quiet 静默
        cmd = [
            "ffmpeg",
            "-timeout", "5000000",  # 微秒单位，5秒超时
            "-i", url,
            "-frames:v", "1",
            "-q:v", "2",
            "-y",
            tmp_path
        ]
        # 调用 ffmpeg（同步阻塞，asyncio不太好控制超时）
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(proc.communicate(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return None

            if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
                img = Image.open(tmp_path)
                phash = imagehash.phash(img)
                return phash
        except Exception:
            return None
    return None

def is_fake_source(new_hash, known_hashes):
    """
    判断当前帧哈希是否和已知假源哈希列表中某个哈希非常接近。
    """
    for h in known_hashes:
        if new_hash - h <= FAKE_HASH_DIFF_THRESHOLD:
            return True
    return False

# ==============================
# 核心异步检测函数
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

        start_time = time.time()
        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    latency = time.time() - start_time

                    # 抓起始帧哈希
                    phash = await get_start_frame_hash(url)
                    if phash is None:
                        log_to_file(ERROR_FILE, f"无法获取起始帧哈希 -> {name} | {url}")
                        return None

                    # 判断是否是假源
                    known_hashes = list(start_frame_hash_cache.values())
                    if is_fake_source(phash, known_hashes):
                        log_to_file(SKIPPED_FILE, f"假源排除 -> {name} | {url}")
                        return None

                    # 缓存当前流的起始帧哈希
                    start_frame_hash_cache[name] = phash

                    ff_info = await ffprobe_check(url)
                    detect_time = f"{latency:.2f}s"
                    print(f"✅ 成功: {name} | 延迟: {detect_time} | 非假源")
                    return [name, url, source, logo, detect_time, "网络源", ff_info or ""]
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

    total = len(rows)
    print(f"📊 读取源: {total} 条")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    start = time.time()
    completed = 0
    success = 0
    working = []

    async with aiohttp.ClientSession() as session:
        tasks = [check_stream(session, sem, row) for row in rows]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            if result:
                success += 1
                working.append(result)

            if completed % 100 == 0 or completed == total:
                elapsed = time.time() - start
                rate = completed / elapsed
                eta = (total - completed) / rate if rate > 0 else 0
                print(
                    f"📈 进度: {completed}/{total} | ✅ 成功: {success} | ⏱️ 速率: {rate:.2f}/s | 预计剩余: {eta/60:.1f} 分钟"
                )

    # 写 CSV
    with open(WORKING_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标", "检测时间(延迟)", "分组", "视频信息"])
        writer.writerows(working)

    # 写 M3U
    with open(WORKING_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for name, url, src, logo, t, grp, info in working:
            f.write(f'#EXTINF:-1 tvg-logo="{logo}",{name}\n{url}\n')

    print(f"\n✅ 有效源: {len(working)} 条")
    print(f"📁 输出: {WORKING_FILE} 和 {WORKING_M3U}")
    print(f"🕒 总耗时: {time.time() - start:.2f} 秒")

# ==============================
# 入口
# ==============================
if __name__ == "__main__":
    asyncio.run(main())