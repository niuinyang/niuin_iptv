import asyncio
import aiohttp
import csv
import os
import time
from tqdm import tqdm

INPUT_FILE = "output/merge_total.csv"
OUTPUT_FILE = "output/middle/stage2a_valid.csv"
CHECKPOINT_FILE = "output/middle/stage2a_checkpoint.csv"
TIMEOUT = 8
CONCURRENT_LIMIT = 200
SAVE_INTERVAL = 500  # 每500条保存一次

async def check_channel(session, row):
    url = row[1]
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                return row + ["✅有效"]
            else:
                return row + [f"❌状态{resp.status}"]
    except Exception as e:
        return row + [f"❌错误:{str(e)[:30]}"]

async def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    print("🚀 开始第1阶段检测（HTTP快速检测）")

    # --- 自动恢复 ---
    completed_urls = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, newline='', encoding='utf-8') as f:
            completed_urls = {r[1] for r in csv.reader(f)}
        print(f"🔄 检测到已有 {len(completed_urls)} 条快照，将跳过这些源")

    # --- 加载输入 ---
    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        header = reader[0] + ["检测结果"]
        rows = [r for r in reader[1:] if r[1] not in completed_urls]

    print(f"📦 当前待检测源数：{len(rows)}")

    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    results, count = [], 0

    async with aiohttp.ClientSession() as session:
        async def sem_task(row):
            async with sem:
                return await check_channel(session, row)

        with tqdm(total=len(rows), ncols=90, desc="检测进度") as pbar:
            for i in range(0, len(rows), CONCURRENT_LIMIT):
                batch = rows[i:i + CONCURRENT_LIMIT]
                res = await asyncio.gather(*[sem_task(r) for r in batch])
                results.extend(res)
                count += len(batch)
                pbar.update(len(batch))

                # 每500条保存一次快照
                if count % SAVE_INTERVAL == 0:
                    with open(CHECKPOINT_FILE, 'w', newline='', encoding='utf-8') as f:
                        csv.writer(f).writerows(results)
                    print(f"💾 已保存快照：{count}/{len(rows)}")

    # --- 写出最终结果 ---
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(results)

    print(f"✅ 阶段1完成，结果输出：{OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
