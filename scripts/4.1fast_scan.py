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

# 可能的 URL 列名
POSSIBLE_URL_COLS = ("url","address","地址","stream","地址/url","link")

async def check_one(session, url, timeout, sem, retries=2):
    async with sem:
        for attempt in range(retries):
            start = time.time()
            try:
                # 尝试 HEAD 请求
                async with session.head(url, timeout=timeout) as resp:
                    rtt = (time.time() - start) * 1000
                    return {"url": url, "status": resp.status, "rtt_ms": int(rtt), "method": "HEAD", "ok": 200 <= resp.status < 400}
            except Exception:
                # HEAD 失败则尝试 GET 请求
                try:
                    start2 = time.time()
                    async with session.get(url, timeout=timeout) as resp:
                        rtt = (time.time() - start2) * 1000
                        return {"url": url, "status": resp.status, "rtt_ms": int(rtt), "method": "GET", "ok": 200 <= resp.status < 400}
                except Exception as e:
                    last_exc = e
                    await asyncio.sleep(0.1 * (attempt + 1))
        # 多次尝试失败
        return {"url": url, "status": None, "rtt_ms": None, "method": None, "ok": False, "error": str(last_exc)}

async def run_all(urls, concurrency=100, timeout_seconds=8):
    timeout = ClientTimeout(total=timeout_seconds)
    sem = Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    results = []
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_one(session, url, timeout, sem) for url in urls]
        for fut in tqdm_asyncio.as_completed(tasks, desc="fast-scan", total=len(tasks)):
            res = await fut
            results.append(res)
    return results

def read_urls(input_path):
    urls = []
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        url_col = None
        # 找可能的 URL 列名
        for c in fieldnames:
            if c.lower() in POSSIBLE_URL_COLS or c in POSSIBLE_URL_COLS:
                url_col = c
                break
        # 兜底选第一列或含 http 的列
        if not url_col:
            for c in fieldnames:
                if "http" in c.lower() or "地址" in c:
                    url_col = c
                    break
        if not url_col and fieldnames:
            url_col = fieldnames[0]
        # 读取所有 URL 字符串
        for r in reader:
            u = r.get(url_col,"").strip()
            if u:
                urls.append(u)
    return urls

def write_results(results, outpath=OUTPUT):
    fieldnames = ["url","status","rtt_ms","method","ok","error"]
    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, None) for k in fieldnames}
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEFAULT_INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--timeout", type=int, default=8)
    args = p.parse_args()

    urls = read_urls(args.input)
    print(f"Loaded {len(urls)} urls from {args.input}")
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, timeout_seconds=args.timeout))
    write_results(results, args.output)
    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"Fast scan finished: {ok_count}/{len(results)} OK -> wrote {args.output}")

if __name__ == "__main__":
    main()
