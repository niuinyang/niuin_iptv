import aiohttp
import asyncio
import csv
import os
import subprocess
import json
import time
import tempfile
from PIL import Image
import imagehash

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
FAKE_FILE = os.path.join(LOG_DIR, "fake.log")

MAX_CONCURRENCY = 10  # ffprobe和ffmpeg较耗资源，适当调低
TIMEOUT = 8           # 超时时间(秒)
FAKE_HASH_THRESHOLD = 5  # 帧哈希差异阈值，小于此则视为假源

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
# 第1步 HTTP HEAD 检测
# ==============================
async def http_head_check(session, url):
    try:
        async with session.head(url, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                return True
    except Exception as e:
        log_to_file(ERROR_FILE, f"HTTP HEAD失败: {url} | {str(e)}")
    return False


# ==============================
# 第2步 FFprobe检测
# ==============================
async def ffprobe_check(url):
    cmd = [
        "ffprobe",
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        url
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        log_to_file(ERROR_FILE, f"FFprobe失败: {url} | {stderr.decode(errors='ignore')}")
        return None
    try:
        info = json.loads(stdout.decode())
    except Exception as e:
        log_to_file(ERROR_FILE, f"FFprobe JSON解析失败: {url} | {str(e)}")
        return None
    streams = info.get("streams", [])
    has_video = any(s.get("codec_type") == "video" for s in streams)
    return info if has_video else None


# ==============================
# 第3步 抓取流数据检测
# ==============================
async def fetch_stream_data(session, url):
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.content.read(4096)
                if b"#EXTM3U" in data or b"TS" in data:
                    return True
    except Exception as e:
        log_to_file(ERROR_FILE, f"抓取流数据失败: {url} | {str(e)}")
    return False


# ==============================
# 第4步 播放模拟检测
# ==============================
def simulate_playback(url):
    cmd = [
        "ffmpeg",
        "-y",
        "-i", url,
        "-t", "5",
        "-f", "null",
        "-"
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        log_to_file(ERROR_FILE, f"播放模拟失败: {url} | {proc.stderr.decode(errors='ignore')}")
        return False
    return True


# ==============================
# 第5步 假源检测（帧哈希分析）
# ==============================
def is_fake_stream(url, threshold=FAKE_HASH_THRESHOLD):
    tmpdir = tempfile.mkdtemp()
    cmd = [
        "ffmpeg", "-y", "-i", url,
        "-vf", "select='eq(pict_type,I)'",
        "-frames:v", "5",
        os.path.join(tmpdir, "frame_%02d.jpg")
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    hashes = []
    for f in sorted(os.listdir(tmpdir)):
        path = os.path.join(tmpdir, f)
        try:
            h = imagehash.phash(Image.open(path))
            hashes.append(h)
        except Exception:
            continue

    diffs = [abs(hashes[i] - hashes[i+1]) for i in range(len(hashes)-1)]
    avg_diff = sum(diffs)/len(diffs) if diffs else 0

    # 清理临时文件
    for f in os.listdir(tmpdir):
        os.remove(os.path.join(tmpdir, f))
    os.rmdir(tmpdir)

    # 平均差异过低表示画面几乎不变
    if avg_diff < threshold:
        return True
    return False


# ==============================
# 综合检测流程
# ==============================
async def full_check(session, sem, row):
    async with sem:
        name, url, source, logo = row
        if not is_allowed(name, url):
            return None

        start_time = time.perf_counter()

        # 1. HTTP检测
        if not await http_head_check(session, url):
            return None

        # 2. FFprobe检测
        info = await ffprobe_check(url)
        if not info:
            return None

        # 3. 抓取流数据检测
        if not await fetch_stream_data(session, url):
            return None

        # 4. 播放模拟检测
        if not simulate_playback(url):
            return None

        # 5. 假源检测
        if is_fake_stream(url):
            log_to_file(FAKE_FILE, f"假源 -> {name} | {url}")
            return None

        elapsed = time.perf_counter() - start_time
        detect_time = f"{elapsed:.2f}s"
        print(f"✅ 通过检测: {name} | 耗时 {detect_time}")
        return [name, url, source, logo, detect_time, "网络源"]


# ==============================
# 主任务
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
                raise ValueError(f"CSV 文件缺少列: '{col}'")
        rows = [[r["频道名"], r["地址"], r["来源"], r["图标"]] for r in reader]

    print(f"📊 读取源: {len(rows)} 条")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [full_check(session, sem, row) for row in rows]
        results = await asyncio.gather(*tasks)

    working = [r for r in results if r]
    print(f"\n✅ 有效源: {len(working)} 条")

    with open(WORKING_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标", "检测时间", "分组"])
        writer.writerows(working)

    with open(WORKING_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for name, url, source, logo, detect_time, group in working:
            f.write(f'#EXTINF:-1 tvg-logo="{logo}",{name}\n{url}\n')

    print(f"📁 输出文件：{WORKING_FILE} 和 {WORKING_M3U}")
    print(f"🕒 检测完成，共 {len(working)} 条有效源。")


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    print(f"\n⏱️ 总耗时: {time.time() - start:.2f} 秒")