#!/usr/bin/env python3
# scripts/4.1fast_scan.py

import asyncio
import aiohttp
import csv
import time
import argparse
import os
from aiohttp import ClientTimeout
from asyncio import Semaphore
from tqdm import tqdm

DEFAULT_INPUT = "output/merge_total.csv"
OUTPUT = "output/middle/fast_scan.csv"
FAILED_OUTPUT = "output/middle/fast_scan_failed.csv"

# ================= 工具函数 =================

def normalize_header(h):
    if not h:
        return ""
    return h.strip().replace("\ufeff", "").replace(" ", "").lower()

def find_colname(headers, candidates):
    norm_headers = {normalize_header(h): h for h in headers}
    for c in candidates:
        key = normalize_header(c)
        if key in norm_headers:
            return norm_headers[key]
    return None

# ================= 核心检测函数 =================

async def check_one(session, url, timeout, sem, retries=2):
    async with sem:
        for attempt in range(retries):
            start = time.time()
            try:
                async with session.head(url, timeout=timeout) as resp:
                    ms = int((time.time() - start) * 1000)
                    return True, ms, resp.status
            except Exception:
                try:
                    start2 = time.time()
                    async with session.get(url, timeout=timeout) as resp:
                        ms = int((time.time() - start2) * 1000)
                        return True, ms, resp.status
                except Exception as e:
                    last_error = str(e)
        return False, None, last_error

# ================= 主逻辑 =================

async def fast_scan(input_file, output_file, failed_file, concurrency, timeout):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(failed_file), exist_ok=True)

    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        col_name = find_colname(headers, ["频道名", "name", "channel", "title"])
        col_url = find_colname(headers, ["地址", "url", "link", "stream", "播放地址"])
        col_source = find_colname(headers, ["来源", "source", "origin"])
        col_icon = find_colname(headers, ["图标", "icon", "logo"])

        if not col_url:
            raise ValueError("❌ 未找到地址列！请确认输入文件中包含 '地址' 或 'url' 等字段。")

        rows = list(reader)

    sem = Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    results_ok, results_fail = [], []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for row in rows:
            url = row.get(col_url, "").strip()
            if not url:
                continue
            tasks.append((row, asyncio.create_task(check_one(session, url, ClientTimeout(total=timeout), sem))))

        pbar = tqdm(total=len(tasks), desc="fast-scan", unit="条", ncols=100)
        completed = 0
        success_times = []

        for row, task in tasks:
            ok, ms, info = await task
            name = row.get(col_name, "").strip() if col_name else ""
            src = row.get(col_source, "").strip() if col_source else ""
            icon = row.get(col_icon, "").strip() if col_icon else ""
            addr = row.get(col_url, "").strip()

            if ok:
                results_ok.append({
                    "频道名": name,
                    "地址": addr,
                    "来源": src,
                    "图标": icon,
                    "检测时间": ms,
                    "分组": "未分组",
                    "视频信息": ""
                })
                success_times.append(ms)
            else:
                results_fail.append({
                    "频道名": name,
                    "地址": addr,
                    "来源": src,
                    "图标": icon,
                    "失败原因": info
                })

            completed += 1
            pbar.update(1)

            if completed % 100 == 0 or completed == len(tasks):
                success_rate = len(success_times) / completed if completed > 0 else 0
                avg_rtt = sum(success_times) / len(success_times) if success_times else 0
                pbar.set_postfix({
                    "成功率": f"{success_rate:.1%}",
                    "平均延迟": f"{avg_rtt:.0f}ms",
                    "并发": concurrency,
                    "超时": f"{timeout}s"
                })

        pbar.close()

    # 写入输出文件
    with open(output_file, "w", newline="", encoding="utf-8") as f_ok:
        writer = csv.DictWriter(f_ok, fieldnames=["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频信息"])
        writer.writeheader()
        writer.writerows(results_ok)

    if results_fail:
        with open(failed_file, "w", newline="", encoding="utf-8") as f_fail:
            writer = csv.DictWriter(f_fail, fieldnames=["频道名", "地址", "来源", "图标", "失败原因"])
            writer.writeheader()
            writer.writerows(results_fail)

    print(f"\n✅ 有效源 {len(results_ok)} 条，已写入 {output_file}")
    print(f"❌ 无效源 {len(results_fail)} 条，已写入 {failed_file}")

# ================= 命令行入口 =================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT, help="输入文件路径")
    parser.add_argument("--output", default=OUTPUT, help="输出文件路径")
    parser.add_argument("--failed", default=FAILED_OUTPUT, help="失败输出文件路径")
    parser.add_argument("--concurrency", type=int, default=100, help="并发数")
    parser.add_argument("--timeout", type=int, default=8, help="超时时间（秒）")
    args = parser.parse_args()

    asyncio.run(fast_scan(args.input, args.output, args.failed, args.concurrency, args.timeout))