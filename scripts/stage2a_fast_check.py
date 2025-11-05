import asyncio
import aiohttp
import csv
import time
import os
from aiohttp import ClientTimeout

INPUT_FILE = "output/merge_total.csv"
OUTPUT_FILE = "output/middle/stage2a_valid.csv"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

async def check_url(session, url):
    try:
        start = time.time()
        async with session.get(url, timeout=ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                latency = round(time.time() - start, 2)
                return True, latency
    except:
        pass
    return False, None

async def main():
    results = []
    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    async with aiohttp.ClientSession() as session:
        for i, row in enumerate(rows, 1):
            ok, latency = await check_url(session, row["地址"])
            if ok:
                row["延迟"] = latency
                results.append(row)
            if i % 200 == 0:
                print(f"✅ 已检测 {i}/{len(rows)}")

    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() | {"延迟"})
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    asyncio.run(main())
