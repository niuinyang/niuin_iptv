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

POSSIBLE_URL_COLS = ("url","address","地址","stream","地址/url","link")

# 动态参数配置区
INITIAL_CONCURRENCY = 20
MAX_CONCURRENCY = 100
MIN_CONCURRENCY = 5

INITIAL_TIMEOUT = 5  # 秒
MAX_TIMEOUT = 15
MIN_TIMEOUT = 3

RETRIES = 2  # 最大重试次数

PROGRESS_LOG_INTERVAL = 0.01  # 每完成1%输出一次日志

async def check_one(session, url, timeout, sem, retries=RETRIES):
    async with sem:
        for attempt in range(retries):
            start = time.time()
            try:
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
                    # 重试时稍微等待，避免短时间爆发失败
                    await asyncio.sleep(0.1 * (attempt + 1))
        return {"url": url, "status": None, "rtt_ms": None, "method": None, "ok": False, "error": str(last_exc)}

async def run_all(urls, initial_concurrency=INITIAL_CONCURRENCY, 
                  min_conc=MIN_CONCURRENCY, max_conc=MAX_CONCURRENCY, 
                  initial_timeout=INITIAL_TIMEOUT, min_timeout=MIN_TIMEOUT, max_timeout=MAX_TIMEOUT):

    concurrency = initial_concurrency
    timeout_seconds = initial_timeout

    total = len(urls)
    sem = Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)

    results = []
    success_count = 0
    total_rtt = 0
    checked = 0
    last_log_percent = 0

    async with aiohttp.ClientSession(connector=connector) as session:

        # 内部协程：每次检测
        async def run_check(url):
            nonlocal success_count, total_rtt, checked, concurrency, timeout_seconds, sem, connector, session

            res = await check_one(session, url, ClientTimeout(total=timeout_seconds), sem)
            checked += 1

            # 动态调整逻辑（每100个请求动态调节一次）
            if res.get("ok") and res.get("rtt_ms"):
                success_count += 1
                total_rtt += res["rtt_ms"]

            if checked % 100 == 0 or checked == total:
                # 计算成功率和平均RTT
                success_rate = success_count / checked if checked else 0
                avg_rtt = total_rtt / success_count if success_count else timeout_seconds * 1000

                # 调整并发数
                if success_rate > 0.8 and concurrency < max_conc:
                    concurrency = min(max_conc, int(concurrency * 1.2))
                elif success_rate < 0.5 and concurrency > min_conc:
                    concurrency = max(min_conc, int(concurrency * 0.7))

                # 调整 timeout，适当加长避免超时误判
                if avg_rtt > timeout_seconds * 1000 * 0.8 and timeout_seconds < max_timeout:
                    timeout_seconds = min(max_timeout, timeout_seconds + 1)
                elif avg_rtt < timeout_seconds * 1000 * 0.5 and timeout_seconds > min_timeout:
                    timeout_seconds = max(min_timeout, timeout_seconds - 1)

                # 更新信号量和连接数限制
                if sem._value != concurrency or connector.limit != concurrency:
                    sem = Semaphore(concurrency)
                    connector.limit = concurrency
                    # 重新绑定session对象，注意这里不关闭旧session
                    session._connector = connector

            # 每完成1%进度，输出日志
            percent = checked / total
            nonlocal last_log_percent
            if percent - last_log_percent >= PROGRESS_LOG_INTERVAL or checked == total:
                print(f"fast-scan: {percent:.0%} done, concurrency={concurrency}, timeout={timeout_seconds}s, success_rate={success_rate:.2%}, avg_rtt={int(avg_rtt)}ms")
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
            u = r.get(url_col,"").strip()
            if u:
                urls.append(u)
    return urls

def write_results(results, outpath=OUTPUT):
    fieldnames = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频信息"]
    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = {
                "频道名": "",
                "地址": r.get("url", ""),
                "来源": "网络源",
                "图标": "",
                "检测时间": r.get("rtt_ms") if r.get("rtt_ms") is not None else "",
                "分组": "未分组",
                "视频信息": ""
            }
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEFAULT_INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=INITIAL_CONCURRENCY)
    p.add_argument("--timeout", type=int, default=INITIAL_TIMEOUT)
    args = p.parse_args()

    urls = read_urls(args.input)
    print(f"Loaded {len(urls)} urls from {args.input}")
    results = asyncio.run(run_all(
        urls, 
        initial_concurrency=args.concurrency, 
        initial_timeout=args.timeout))
    write_results(results, args.output)
    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"Fast scan finished: {ok_count}/{len(results)} OK -> wrote {args.output}")

if __name__ == "__main__":
    main()
