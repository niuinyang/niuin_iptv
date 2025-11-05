import aiohttp
import asyncio
import csv
import os
import time
from PIL import Image
import imagehash
import tempfile

# ==============================
# 配置区
# ==============================
INPUT_FILE = "output/middle/stage2b_verified.csv"  # 第二阶段检测后的文件
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(OUTPUT_DIR, "stage3_final_checked.csv")
OUTPUT_M3U = os.path.join(OUTPUT_DIR, "stage3_final_checked.m3u")

MAX_CONCURRENCY = 40      # 并发数，可调
CHECK_TIMES = 2           # 每个源检测次数
INTERVAL_BETWEEN_CHECKS = 1.5  # 每次检测间隔秒数，控制整体时长

TIMEOUT = 8               # 请求超时秒数

FAKE_HASH_DIFF_THRESHOLD = 5  # 哈希差阈值，低于则视为假源

# ==============================
# 工具函数
# ==============================
def log(msg):
    print(msg)

async def get_start_frame_hash(url):
    """利用ffmpeg截取起始帧生成感知哈希"""
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=True) as tmpfile:
        tmp_path = tmpfile.name
        cmd = [
            "ffmpeg",
            "-timeout", "5000000",
            "-i", url,
            "-frames:v", "1",
            "-q:v", "2",
            "-y",
            tmp_path
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(proc.communicate(), timeout=15)
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

def is_fake_source(hashes):
    """判断多次检测哈希是否高度相似，判定假源"""
    for i in range(len(hashes)):
        for j in range(i+1, len(hashes)):
            if hashes[i] - hashes[j] <= FAKE_HASH_DIFF_THRESHOLD:
                return True
    return False

async def ffprobe_check(url):
    """简单调用ffprobe获取视频流信息"""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,codec_name",
            "-of", "csv=p=0", url
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if stdout:
            lines = stdout.decode().strip().splitlines()
            return lines[0] if lines else ""
        return ""
    except Exception:
        return ""

# ==============================
# 核心异步检测函数
# ==============================
async def check_stream_multiple(session, sem, row):
    async with sem:
        name, url, source, logo = row
        phashes = []
        # 多次检测
        for i in range(CHECK_TIMES):
            phash = await get_start_frame_hash(url)
            if phash is None:
                log(f"⚠️ {name} 第{i+1}次检测无法获取起始帧哈希")
                return None
            phashes.append(phash)
            if i < CHECK_TIMES - 1:
                await asyncio.sleep(INTERVAL_BETWEEN_CHECKS)  # 间隔等待

        # 判断是否为假源
        if is_fake_source(phashes):
            log(f"❌ 假源排除: {name}")
            return None

        # ffprobe获取视频信息
        ff_info = await ffprobe_check(url)
        detect_time = "N/A"  # 这里没计算网络延迟，你可以加

        log(f"✅ 有效源: {name}")

        return [name, url, source, logo, detect_time, "网络源", ff_info or ""]

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
    print(f"📦 总源数: {total} 条，开始第3阶段多次检测...")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    start_time = time.time()
    completed = 0
    valid_results = []

    async with aiohttp.ClientSession() as session:
        tasks = [check_stream_multiple(session, sem, row) for row in rows]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            if result:
                valid_results.append(result)

            if completed % 100 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0
                print(f"📈 进度: {completed}/{total} | 有效: {len(valid_results)} | 速率: {rate:.2f}/s | 预计剩余: {eta/60:.1f} 分钟")

    # 写入csv
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标", "检测时间(延迟)", "分组", "视频信息"])
        writer.writerows(valid_results)

    # 写入m3u
    with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for name, url, source, logo, t, grp, info in valid_results:
            f.write(f'#EXTINF:-1 tvg-logo="{logo}",{name}\n{url}\n')

    total_time = time.time() - start_time
    print(f"\n✅ 第3阶段完成，有效源: {len(valid_results)} 条")
    print(f"📁 输出文件: {OUTPUT_CSV} 和 {OUTPUT_M3U}")
    print(f"🕒 总耗时: {total_time:.2f} 秒")

# ==============================
# 入口
# ==============================
if __name__ == "__main__":
    asyncio.run(main())
