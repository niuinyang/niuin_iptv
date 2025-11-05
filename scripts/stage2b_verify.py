import csv
import os
import asyncio
import ffmpeg
from tqdm import tqdm
import time

INPUT_CSV = "output/middle/stage2a_valid.csv"
OUTPUT_FINAL = "output/middle/stage2b_verified.csv"

MAX_CONCURRENCY = 50
sem = asyncio.Semaphore(MAX_CONCURRENCY)

async def ffprobe_check(url):
    try:
        probe = ffmpeg.probe(url)
        return "✅有效", probe
    except ffmpeg.Error as e:
        return "❌错误", str(e)

async def check_item(item):
    url = item[1]
    async with sem:
        result, detail = await ffprobe_check(url)
    return item + [result, detail]

async def main():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ 输入文件不存在: {INPUT_CSV}")
        return

    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.reader(f))

    total = len(rows)
    results = []

    start_time = time.time()
    pbar = tqdm(total=total, desc="检测进度", unit="条")

    for idx, item in enumerate(rows):
        checked = await check_item(item)
        results.append(checked)
        pbar.update(1)

        if (idx + 1) % 500 == 0 or (idx + 1) == total:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            eta = (total - idx - 1) / speed if speed > 0 else 0
            print(f"进度: {idx + 1}/{total} | 速率: {speed:.2f}条/s | 预计剩余: {eta/60:.1f} 分钟")

    pbar.close()

    with open(OUTPUT_FINAL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(results)

    print(f"✅ 阶段2完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    asyncio.run(main())
