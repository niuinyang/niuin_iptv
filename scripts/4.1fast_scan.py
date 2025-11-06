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

# 并发和timeout参数区间配置
INITIAL_CONCURRENCY = 100
MIN_CONCURRENCY = 20
MAX_CONCURRENCY = 200

INITIAL_TIMEOUT = 8
MIN_TIMEOUT = 4
MAX_TIMEOUT = 15

PROGRESS_LOG_INTERVAL = 0.01  # 每1%输出一次日志

async def check_one(session, url, timeout, sem, retries=2):
    async with sem:
        last_exc = None
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

async def run_all(urls,
                  initial_concurrency=INITIAL_CONCURRENCY,
                  min_conc=MIN_CONCURRENCY,
                  max_conc=MAX_CONCURRENCY,
                  initial_timeout=INITIAL_TIMEOUT,
                  min_timeout=MIN_TIMEOUT,
                  max_timeout=MAX_TIMEOUT):

    concurrency = initial_concurrency
    timeout_seconds = initial_timeout

    total = len(urls)
    sem = Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=max_conc, ssl=False)  # limit设为最大，避免受限
    results = []
    success_count = 0
    total_rtt = 0
    checked = 0
    last_log_percent = 0

    async with aiohttp.ClientSession(connector=connector) as session:

        async def run_check(url):
            nonlocal success_count, total_rtt, checked, concurrency, timeout_seconds, sem

            res = await check_one(session, url, ClientTimeout(total=timeout_seconds), sem)
            checked += 1

            if res.get("ok") and res.get("rtt_ms"):
                success_count += 1
                total_rtt += res["rtt_ms"]

            # 每100条数据或结尾时调整参数
            if checked % 100 == 0 or checked == total:
                success_rate = success_count / checked if checked else 0
                avg_rtt = total_rtt / success_count if success_count else timeout_seconds * 1000

                old_concurrency = concurrency
                if success_rate > 0.8 and concurrency < max_conc:
                    concurrency = min(max_conc, int(concurrency * 1.2))
                elif success_rate < 0.5 and concurrency > min_conc:
                    concurrency = max(min_conc, int(concurrency * 0.7))
                if concurrency != old_concurrency:
                    sem = Semaphore(concurrency)  # 重新创建信号量控制并发

                # 动态调整timeout
                if avg_rtt > timeout_seconds * 1000 * 0.8 and timeout_seconds < max_timeout:
                    timeout_seconds = min(max_timeout, timeout_seconds + 1)
                elif avg_rtt < timeout_seconds * 1000 * 0.5 and timeout_seconds > min_timeout:
                    timeout_seconds = max(min_timeout, timeout_seconds - 1)

            percent = checked / total
            if percent - last_log_percent >= PROGRESS_LOG_INTERVAL or checked == total:
                print(f"fast-scan: {percent:.0%} done, concurrency={concurrency}, timeout={timeout_seconds}s, success_rate={success_rate:.2%}, avg_rtt={int(avg_rtt)}ms")
                nonlocal last_log_percent
                last_log_percent = percent

            return res

        tasks = [run_check(url) for url in urls]

        for fut in tqdm_asyncio.as_completed(tasks, desc="fast-scan", total=total):
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

def write_results(results, input_path, outpath=OUTPUT):
    fieldnames = ["频道名","地址","来源","图标","检测时间","分组","视频信息"]
    url_map = {}
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            url_map[row.get("url") or row.get("地址") or row.get("address")] = row

    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = url_map.get(r.get("url"), {})
            out_row = {
                "频道名": row.get("频道名",""),
                "地址": r.get("url",""),
                "来源": "网络源",
                "图标": row.get("图标",""),
                "检测时间": r.get("rtt_ms") or "",
                "分组": "未分组",
                "视频信息": ""
            }
            w.writerow(out_row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEFAULT_INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=INITIAL_CONCURRENCY)
    p.add_argument("--timeout", type=int, default=INITIAL_TIMEOUT)
    args = p.parse_args()

    input_path = args.input  # 改成局部变量

    urls = read_urls(input_path)
    print(f"Loaded {len(urls)} urls from {input_path}")
    results = asyncio.run(run_all(urls,
                                  initial_concurrency=args.concurrency,
                                  initial_timeout=args.timeout))
    write_results(results, input_path, args.output)
    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"Fast scan finished: {ok_count}/{len(results)} OK -> wrote {args.output}")

if __name__ == "__main__":
    main()
