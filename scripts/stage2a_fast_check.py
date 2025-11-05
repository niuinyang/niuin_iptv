import csv
import os
import asyncio
import aiohttp
import time
from tqdm import tqdm

INPUT_CSV = "output/merge_total.csv"
OUTPUT_SNAPSHOT = "output/middle/stage2a_valid.csv"
OUTPUT_FINAL = "output/middle/stage2a_valid.csv"

MAX_CONCURRENCY = 100   # 并发数，可根据机器调整
SAVE_INTERVAL = 500     # 每500条保存快照

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
    if os.path.exists(OUTPUT_SNAPSHOT):
        print(f"🔄 恢复检测，加载快照文件：{OUTPUT_SNAPSHOT}")
        with open(OUTPUT_SNAPSHOT, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))
    else:
        print(f"🚀 开始第1阶段快速检测")
        with open(INPUT_CSV, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))

    total = len(rows)
    results = []
    start_idx = 0

    if os.path.exists(OUTPUT_SNAPSHOT):
        start_idx = len(rows)
        if start_idx >= total:
            print("✔️ 快照已完成检测，跳过")
            return

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        pbar = tqdm(total=total, desc="检测进度", unit="条", initial=start_idx)
        start_time = time.time()
        for idx in range(start_idx, total):
            item = rows[idx]
            checked = await check_source(session, item)
            results.append(checked)
            pbar.update(1)

            if (idx + 1) % SAVE_INTERVAL == 0 or (idx + 1) == total:
                with open(OUTPUT_SNAPSHOT, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(results)
                elapsed = time.time() - start_time
                speed = (idx + 1 - start_idx) / elapsed if elapsed > 0 else 0
                eta = (total - idx - 1) / speed if speed > 0 else 0
                print(f"💾 已保存快照：{len(results)}/{total} | 速率: {speed:.2f}条/s | 预计剩余: {eta/60:.1f} 分钟")

        pbar.close()

    with open(OUTPUT_FINAL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(results)

    if os.path.exists(OUTPUT_SNAPSHOT):
        os.remove(OUTPUT_SNAPSHOT)
        print(f"🗑️ 快照文件已删除：{OUTPUT_SNAPSHOT}")

    print(f"✅ 阶段1完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    asyncio.run(main())
