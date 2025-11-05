import csv
import os
from tqdm import tqdm
import asyncio
import aiohttp
import ffmpeg

INPUT_CSV = "output/middle/stage2a_valid.csv"
OUTPUT_SNAPSHOT = "output/middle/stage2b_verified.csv"
OUTPUT_FINAL = "output/middle/stage2b_verified.csv"

SAVE_INTERVAL = 500

async def ffprobe_check(session, url):
    try:
        # 这里示例调用 ffmpeg probe 检测, 你需根据自己代码调整
        # 异步或同步均可，这里用同步调用示范：
        probe = ffmpeg.probe(url)
        return "✅有效", probe
    except ffmpeg.Error as e:
        return "❌错误", str(e)

async def check_item(session, item):
    url = item[1]
    result, detail = await ffprobe_check(session, url)
    return item + [result, detail]

async def main():
    if os.path.exists(OUTPUT_SNAPSHOT):
        print(f"🔄 恢复检测，从快照加载：{OUTPUT_SNAPSHOT}")
        with open(OUTPUT_SNAPSHOT, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))
    else:
        print(f"🚀 开始第2阶段检测（FFprobe验证）")
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

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=total, desc="检测进度", unit="条", initial=start_idx)
        for idx in range(start_idx, total):
            item = rows[idx]
            checked = await check_item(session, item)
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

    print(f"✅ 阶段2完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    asyncio.run(main())
