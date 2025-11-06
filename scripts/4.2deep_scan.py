import asyncio
import aiohttp
import ffmpeg
import csv
import argparse
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor

# ==========================
# 异步 ffprobe 调用
# ==========================
def probe_stream(url, timeout):
    """调用 ffprobe 获取视频流信息"""
    try:
        probe = ffmpeg.probe(
            url,
            select_streams='v',
            v='error',
            show_entries='stream=codec_name,width,height,r_frame_rate',
            show_format=True,
            timeout=timeout,
        )
        video_streams = [s for s in probe['streams'] if s.get('codec_type') == 'video']
        audio_streams = [s for s in probe['streams'] if s.get('codec_type') == 'audio']

        if not video_streams:
            return None, None, None, "无音频"

        v = video_streams[0]
        codec = v.get('codec_name', '未知').upper()
        width = v.get('width', '?')
        height = v.get('height', '?')
        frame_rate_raw = v.get('r_frame_rate', '0')
        try:
            frame_rate = round(eval(frame_rate_raw), 2) if frame_rate_raw != '0' else 0
        except Exception:
            frame_rate = 0

        resolution = f"{width}x{height}"
        has_audio = "有音频" if audio_streams else "无音频"
        frame_rate_str = f"{frame_rate}fps" if frame_rate else "未知"

        return codec, resolution, frame_rate_str, has_audio
    except Exception:
        return None, None, None, "无音频"


# ==========================
# 异步检测单源
# ==========================
async def process_url(session, row, semaphore, executor, timeout):
    """检测单个频道"""
    async with semaphore:
        url = row['地址']

        # Step 1: 检查网络可达
        try:
            async with session.head(url, timeout=timeout) as resp:
                if resp.status >= 400:
                    raise Exception(f"HTTP状态码 {resp.status}")
        except Exception as e:
            return [
                row['频道名'], row['地址'], row['来源'], row['图标'],
                row['检测时间'], row['分组'],
                '', '', '', '', f"无法连接 ({str(e)})"
            ], False

        # Step 2: ffprobe 视频信息
        loop = asyncio.get_running_loop()
        codec, resolution, frame_rate, audio_str = await loop.run_in_executor(
            executor, probe_stream, url, timeout
        )

        if not codec:
            return [
                row['频道名'], row['地址'], row['来源'], row['图标'],
                row['检测时间'], row['分组'],
                '', '', '', '', "无视频流"
            ], False

        return [
            row['频道名'], row['地址'], row['来源'], row['图标'],
            row['检测时间'], row['分组'],
            codec, resolution, frame_rate, audio_str
        ], True


# ==========================
# 主检测逻辑
# ==========================
async def deep_scan(input_file, output_ok, output_fail, concurrency, timeout):
    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get('地址', '').startswith("http"):
                rows.append(r)

    if not rows:
        print("⚠️ 没有可检测的源")
        return

    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    executor = ThreadPoolExecutor(max_workers=5)

    ok_rows, fail_rows = [], []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        tasks = [process_url(session, r, semaphore, executor, timeout) for r in rows]
        async for result, ok in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="deep-scan"):
            if ok:
                ok_rows.append(result)
            else:
                fail_rows.append(result)

    # 输出结果
    with open(output_ok, "w", newline="", encoding="utf-8") as f_ok:
        writer_ok = csv.writer(f_ok)
        writer_ok.writerow(["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频编码", "分辨率", "帧率", "音频"])
        writer_ok.writerows(ok_rows)

    with open(output_fail, "w", newline="", encoding="utf-8") as f_fail:
        writer_fail = csv.writer(f_fail)
        writer_fail.writerow(["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频编码", "分辨率", "帧率", "音频", "失败原因"])
        writer_fail.writerows(fail_rows)

    print(f"✅ Deep scan 完成：成功 {len(ok_rows)} 条，失败 {len(fail_rows)} 条，总计 {len(rows)} 条")


# ==========================
# 主入口
# ==========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--invalid", default="output/middle/deep_scan_invalid.csv")
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    asyncio.run(deep_scan(args.input, args.output, args.invalid, args.concurrency, args.timeout))


if __name__ == "__main__":
    main()