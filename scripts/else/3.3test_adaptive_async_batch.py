import aiohttp
import asyncio
import csv
import os
import time
from datetime import datetime
from PIL import Image, ImageStat
import imagehash
import tempfile
import subprocess

# ==============================
# 配置区（可根据需要调整）
# ==============================
INPUT_FILE = "output/merge_total.csv"
OUTPUT_DIR = "output"
WORKING_FILE = os.path.join(OUTPUT_DIR, "working.csv")
WORKING_M3U = os.path.join(OUTPUT_DIR, "working.m3u")
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
os.makedirs(LOG_DIR, exist_ok=True)

SKIPPED_FILE = os.path.join(LOG_DIR, "skipped.log")
ERROR_FILE = os.path.join(LOG_DIR, "error.log")

MAX_CONCURRENCY = 40        # overall HTTP 并发（原有）
TIMEOUT = 8                # HTTP 超时秒

# ffmpeg / ffprobe 并发池大小（进阶版）
FFMPEG_CONCURRENCY = 6
FFPROBE_CONCURRENCY = 10

LOW_RES_KEYWORDS = [
    "vga", "270p", "360p", "396p", "406p", "480p",
    "540p", "576p", "576i", "614p"
]
BLOCK_KEYWORDS = ["espanol"]
WHITELIST_PATTERNS = [".ctv", ".sdserver", ".sdn.", ".sda.", ".sdstream", "sdhd", "hdsd"]

# 假源检测阈值，哈希差异小于等于此值判定为假源（单帧比对）
FAKE_HASH_DIFF_THRESHOLD = 5

# 静帧/循环帧判定阈值：若连续多帧两两差异都小于 STATIC_FRAME_THRESHOLD 即视为静帧（假源）
STATIC_FRAME_THRESHOLD = 3
STATIC_FRAME_CHECK_COUNT = 3  # 取样帧数（例如 3 帧：第1、第5、第10）

# 黑屏判断阈值（平均亮度）
BLACKSCREEN_BRIGHTNESS_THRESHOLD = 8  # 0-255，越小越暗

# 缓存已检测起始帧哈希：{频道名: phash}
start_frame_hash_cache = {}

# 可持久化的 URL->phash 缓存（内存中），在需要时可以扩展为文件存储
url_phash_cache = {}

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

# ------------------------------
# 辅助：判断图像是否为"黑屏"
# ------------------------------
def is_black_frame(img: Image.Image) -> bool:
    try:
        # 转灰度并计算平均亮度
        gray = img.convert("L")
        stat = ImageStat.Stat(gray)
        mean_brightness = stat.mean[0]
        return mean_brightness < BLACKSCREEN_BRIGHTNESS_THRESHOLD
    except Exception:
        return False

# ==============================
# 异步 ffprobe：只返回一行、并检测音频流（受并发池控制）
# ==============================
async def ffprobe_check(url, ffprobe_sem: asyncio.Semaphore):
    """使用 ffprobe 获取流信息（只返回第一条视频流信息），并检测是否有音频流。
    返回一个单行字符串（无换行），格式例如:
        "video:h264,1920,1080; audio:aac"
    或者 None（表示未能获取到信息）
    """
    async with ffprobe_sem:
        try:
            # -show_entries 取视频流 codec,width,height 和 所有流的 codec_type,codec_name 用于判断是否有 audio
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height",
                "-of", "csv=p=0", url
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            video_info = None
            if stdout:
                # 可能有多行（多视频流），只取第一行并去除换行
                lines = [l.strip() for l in stdout.decode(errors="ignore").strip().splitlines() if l.strip()]
                if lines:
                    # 视频信息第一行
                    video_info = lines[0].replace("\n", " ").replace("\r", " ")

            # 另行检测是否存在音频流（单独 ffprobe 调用，以避免覆盖 -select_streams v:0 的输出）
            # 这里我们用一次小命令检测是否存在 audio 流
            cmd_audio = [
                "ffprobe", "-v", "error",
                "-show_entries", "stream=codec_type",
                "-of", "csv=p=0", url
            ]
            proc2 = await asyncio.create_subprocess_exec(
                *cmd_audio, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout2, _ = await proc2.communicate()
            has_audio = False
            if stdout2:
                entries = [l.strip() for l in stdout2.decode(errors="ignore").splitlines() if l.strip()]
                # 如果在 entries 中看到 "audio"，说明有音频流
                for e in entries:
                    if "audio" in e.lower():
                        has_audio = True
                        break

            # 构建返回字符串，保证无换行、无重复
            parts = []
            if video_info:
                parts.append(f"video:{video_info}")
            if has_audio:
                parts.append("audio:yes")
            else:
                parts.append("audio:no")
            return "; ".join(parts)
        except Exception as e:
            # 不抛异常，上层处理
            return None

# ==============================
# 异步抓帧并计算多帧 phash（受并发池控制）
# ==============================
async def get_start_frame_hashes(url, ffmpeg_sem: asyncio.Semaphore, sample_offsets=(0, 5, 10)):
    """抓取多帧（通常第1、第5、第10帧），返回帧的 phash 列表（按顺序），
       会过滤掉抓取失败或为黑屏的帧（但记录黑屏情况）。
       返回列表，可能为空。
    """
    phashes = []
    async with ffmpeg_sem:
        # 为每个偏移创建独立临时文件并依次抓取（避免一次命令抓多帧在不同流上的不稳定）
        for n in sample_offsets:
            # 使用 ffmpeg 的 select 取第 n 帧；若 n==0 使用 -frames:v 1 简单抓第一帧
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmpfile:
                tmp_path = tmpfile.name
            try:
                if n == 0:
                    cmd = [
                        "ffmpeg",
                        "-timeout", "5000000",
                        "-i", url,
                        "-frames:v", "1",
                        "-q:v", "2",
                        "-y",
                        tmp_path
                    ]
                else:
                    # 使用 -vf select=eq(n\,N) 来选择第 N 帧（帧索引从0开始）
                    cmd = [
                        "ffmpeg",
                        "-timeout", "5000000",
                        "-i", url,
                        "-vf", f"select=eq(n\\,{n})",
                        "-frames:v", "1",
                        "-q:v", "2",
                        "-y",
                        tmp_path
                    ]

                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                try:
                    await asyncio.wait_for(proc.communicate(), timeout=12)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.communicate()
                    # 清理临时文件
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                    continue

                if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
                    try:
                        img = Image.open(tmp_path).convert("RGB")
                        # 黑屏检测
                        if is_black_frame(img):
                            log_to_file(SKIPPED_FILE, f"黑屏帧 -> offset {n} | {url}")
                            # 抓到黑屏则不加入 phash（但也不立即判定为假源，需看其他帧）
                        else:
                            ph = imagehash.phash(img)
                            phashes.append(ph)
                    except Exception:
                        pass
                # remove temp file
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            except Exception:
                # 任何意外都跳过此帧
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                continue
    return phashes

# ==============================
# 假源判断（基于多帧和缓存）
# ==============================
def is_fake_source_from_hashes(phashes, known_hashes):
    """如果 phashes 为空则视为可疑（上层决定），
       若 phashes 内帧之间差异都很小（静帧/循环），判为假源；
       若与已知假源哈希接近，也判为假源。
    """
    if not phashes:
        # 无有效帧信息 —— 可视为假源/不可用
        return True

    # 1) 检查帧间差异：若多帧之间差异均小于 STATIC_FRAME_THRESHOLD，则视为静帧/循环（假源）
    if len(phashes) >= 2:
        small_diffs = 0
        pairs = 0
        for i in range(len(phashes) - 1):
            diff = phashes[i] - phashes[i + 1]
            pairs += 1
            if diff <= STATIC_FRAME_THRESHOLD:
                small_diffs += 1
        # 如果所有比较都小于阈值（或占比极高），判为静帧
        if pairs > 0 and small_diffs == pairs:
            return True

    # 2) 与已知哈希对比（缓存或历史）：
    for ph in phashes:
        for h in known_hashes:
            try:
                if ph - h <= FAKE_HASH_DIFF_THRESHOLD:
                    return True
            except Exception:
                continue

    return False

# ==============================
# 核心异步检测函数（将使用 ffmpeg_sem / ffprobe_sem）
# ==============================
async def check_stream(session, sem, ffmpeg_sem, ffprobe_sem, row):
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

                    # --- 抓取多帧并计算 phash（受 ffmpeg_sem 控制） ---
                    phashes = await get_start_frame_hashes(url, ffmpeg_sem, sample_offsets=(0, 5, 10))
                    if not phashes:
                        log_to_file(ERROR_FILE, f"无法获取有效帧或均为黑屏 -> {name} | {url}")
                        return None

                    # 判断是否是假源（包括静帧 / 与已知假源相似）
                    known_hashes = list(start_frame_hash_cache.values()) + list(url_phash_cache.values())
                    if is_fake_source_from_hashes(phashes, known_hashes):
                        log_to_file(SKIPPED_FILE, f"假源排除(多帧检测) -> {name} | {url}")
                        return None

                    # 缓存第一个 phash 到频道缓存与 URL 缓存（便于后续对比）
                    start_frame_hash_cache[name] = phashes[0]
                    url_phash_cache[url] = phashes[0]

                    # --- ffprobe 深度检测（受 ffprobe_sem 控制） ---
                    ff_info = await ffprobe_check(url, ffprobe_sem)
                    detect_time = f"{latency:.2f}s"
                    print(f"✅ 成功: {name} | 延迟: {detect_time} | 非假源 | {ff_info or ''}")
                    return [name, url, source, logo, detect_time, "网络源", ff_info or ""]
                else:
                    log_to_file(ERROR_FILE, f"{resp.status} ❌ {name} -> {url}")
                    return None
        except Exception as e:
            log_to_file(ERROR_FILE, f"异常 {name} -> {url} | {str(e)}")
            return None

# ==============================
# 主任务控制（创建并传递并发池）
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

    # 并发控制：HTTP 总并发（原有）和 ffmpeg / ffprobe 专用并发池
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    ffmpeg_sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    ffprobe_sem = asyncio.Semaphore(FFPROBE_CONCURRENCY)

    start = time.time()
    completed = 0
    success = 0
    working = []

    async with aiohttp.ClientSession() as session:
        tasks = [check_stream(session, sem, ffmpeg_sem, ffprobe_sem, row) for row in rows]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            if result:
                success += 1
                working.append(result)

            if completed % 100 == 0 or completed == total:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0
                print(
                    f"📈 进度: {completed}/{total} | ✅ 成功: {success} | ⏱️ 速率: {rate:.2f}/s | 预计剩余: {eta/60:.1f} 分钟"
                )

    # 写 CSV（列名与原始保持一致）
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
