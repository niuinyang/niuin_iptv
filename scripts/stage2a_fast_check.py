import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio
import csv
import time
import os

INPUT_CSV = "output/merge_total.csv"  # 你实际的输入路径
OUTPUT_CSV = "output/middle/stage2a_valid.csv"

MAX_CONCURRENT_REQUESTS = 100  # 并发数，机器网络好可以调大点

sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def check_url(session, row):
    name, url, source, logo = row
    async with sem:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    return row + ["有效"]
                else:
                    return row + ["无效"]
        except Exception:
            return row + ["无效"]

async def main():
    if not os.path.exists(INPUT_CSV):
        print(f"输入文件不存在：{INPUT_CSV}")
        return

    with open(INPUT_CSV, encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    results = []
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [check_url(session, row) for row in rows]
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="检测进度", unit="条"):
            res = await future
            results.append(res)

    elapsed = time.time() - start_time
    print(f"检测完成，耗时: {elapsed:.2f}秒，平均速率: {len(rows)/elapsed:.2f}条/秒")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(results)

if __name__ == "__main__":
    asyncio.run(main())
