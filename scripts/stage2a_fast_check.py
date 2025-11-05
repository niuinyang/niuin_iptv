import csv
import os
from tqdm import tqdm
import asyncio
import aiohttp

INPUT_CSV = "output/merge_total.csv"
OUTPUT_SNAPSHOT = "output/middle/stage2a_valid_snapshot.csv"
OUTPUT_FINAL = "output/middle/stage2a_valid.csv"

SAVE_INTERVAL = 500  # 每500条保存快照

async def check_source(session, item):
    url = item[1]
    try:
        async with session.head(url, timeout=10) as resp:
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
        start_idx = len(rows)
    else:
        print(f"🚀 开始第1阶段快速检测")
        with open(INPUT_CSV, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))
        start_idx = 0

    total = len(rows)
    results = [] if start_idx == 0 else rows

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=total, desc="检测进度", unit="条", initial=start_idx)
        for idx in range(start_idx, total):
            item = rows[idx]
            checked = await check_source(session, item)
            results.append(checked)
            pbar.update(1)

            if (idx + 1) % SAVE_INTERVAL == 0 or (idx + 1) == total:
                with open(OUTPUT_SNAPSHOT, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(results)
                print(f"💾 已保存快照：{len(results)}/{total}")

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
