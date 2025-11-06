#!/usr/bin/env python3
# scripts/4.1fast_scan.py
import asyncio
import aiohttp
import csv
import time
import argparse
from aiohttp import ClientTimeout
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore

DEFAULT_INPUT = "output/merge_total.csv"
OUTPUT = "output/middle/fast_scan.csv"

POSSIBLE_URL_COLS = ("url", "address", "地址", "stream", "地址/url", "link")

async def check_one(session, url, timeout, sem, retries=2):
    async with sem:
        start = time.time()
        for attempt in range(retries):
            try:
                # try HEAD first
                async with session.head(url, timeout=timeout) as resp:
                    rtt = (time.time() - start) * 1000
                    return {"url": url, "status": resp.status, "rtt_ms": int(rtt), "method": "HEAD", "ok": 200 <= resp.status < 400}
            except Exception:
                try:
                    start2 = time.time()
                    async with session.get(url, timeout=timeout) as resp:
                        rtt = (time.time() - start2) * 1000
                        return {"url": url, "status": resp.status, "rtt_ms": int(rtt), "method": "GET", "ok": 200 <= resp.status < 400}
                except Exception as e:
                    last_exc = e
                    await asyncio.sleep(0.1 * (attempt + 1))
        return {"url": url, "status": None, "rtt_ms": None, "method": None, "ok": False, "error": str(last_exc)}

async def run_all(records, concurrency=100, timeout_seconds=8):
    timeout = ClientTimeout(total=timeout_seconds)
    sem = Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    results = []
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_one(session, r["url"], timeout, sem) for r in records]
        for fut in tqdm_asyncio.as_completed(tasks, desc="fast-scan", total=len(tasks)):
            res = await fut
            results.append(res)
    return results

def read_input(input_path):
    records = []
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        url_col = None
        for c in fieldnames:
            if c.lower() in POSSIBLE_URL_COLS or c in POSSIBLE_URL_COLS:
                url_col = c
                break
        if not url_col:
            for c in fieldnames:
                if "http" in c.lower() or "地址" in c:
                    url_col = c
                    break
        if not url_col and fieldnames:
            url_col = fieldnames[0]

        for r in reader:
            u = r.get(url_col, "").strip()
            if u:
                record = {
                    "频道名": r.get("频道名", ""),
                    "地址": u,
                    "来源": "网络源",
                    "图标": r.get("图标", ""),
                    "分组": "未分组",
                }
                records.append(record)
    return records

def write_results(records, results, outpath=OUTPUT):
    # 输出字段顺序
    fieldnames = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频信息"]
    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        # records和results对应，逐条输出
        for rec, res in zip(records, results):
            row = {
                "频道名": rec["频道名"],
                "地址": rec["地址"],
                "来源": rec["来源"],
                "图标": rec["图标"],
                "检测时间": res.get("rtt_ms") if res.get("ok") else "",
                "分组": rec["分组"],
                "视频信息": ""
            }
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEFAULT_INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--timeout", type=int, default=8)
    args = p.parse_args()

    records = read_input(args.input)
    print(f"Loaded {len(records)} records from {args.input}")
    # 给每条记录加上 url 字段，传给 run_all
    urls = [{"url": r["地址"]} for r in records]
    # 这里直接传入records列表，改写run_all接收records同时进行检测
    # 但为了兼容原有run_all，改成只传url列表即可
    # 这里改成传url列表是最简单的，改写run_all和check_one太多，先保持传url列表
    # 重新设计，run_all传入url列表
    # 重新组织代码，使用run_all检查urls，返回结果，再跟records配对输出
    urls_list = [r["地址"] for r in records]
    results = asyncio.run(run_all(urls_list, concurrency=args.concurrency, timeout_seconds=args.timeout))
    write_results(records, results, args.output)
    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"Fast scan finished: {ok_count}/{len(results)} OK -> wrote {args.output}")

if __name__ == "__main__":
    main()
