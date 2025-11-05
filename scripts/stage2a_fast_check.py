import csv
import os
import asyncio
import aiohttp
import time
from tqdm import tqdm

INPUT_CSV = "output/merge_total.csv"
OUTPUT_FINAL = "output/middle/stage2a_valid.csv"

MAX_CONCURRENCY = 200   # 并发数，根据情况调节

sem = asyncio.Semaphore(MAX_CONCURRENCY)

async def check_source(session, item):
    url = item[1]
    async with sem:
        try:
            headers = {"Range": "bytes=0-1023"}
            async with session.get(url, headers=headers, timeout=10) as resp:
                status = resp.status
            if status == 200:
                result = "✅有效"
            else:
                result = f"❌状态{status}"
        except Exception as e:
            result = f"❌错误:{e}"
        return item + [result]

async def main():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ 输入文件不存在: {INPUT_CSV}")
        return

    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.reader(f))

    total = len(rows)
    results = []

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        pbar = tqdm(total=total, desc="检测进度", unit="条")
        start_time = time.time()
        for idx, item in enumerate(rows):
            checked = await check_source(session, item)
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

    print(f"✅ 阶段1完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    asyncio.run(main())
