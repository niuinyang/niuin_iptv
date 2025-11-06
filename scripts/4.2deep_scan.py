#!/usr/bin/env python3
# scripts/4.2deep_scan.py
import asyncio
import csv
import json
import argparse
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import time

INPUT = "output/middle/fast_scan.csv"
OUTPUT = "output/middle/deep_scan.csv"

async def ffprobe_json(url, timeout=20):
    cmd = ["ffprobe","-v","quiet","-print_format","json","-show_streams","-show_format", url]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return {"url": url, "error": "timeout"}
        if stdout:
            try:
                data = json.loads(stdout.decode('utf-8', errors='ignore'))
                return {"url": url, "probe": data}
            except Exception as e:
                return {"url": url, "error": f"json_parse_error: {e}"}
        else:
            return {"url": url, "error": stderr.decode('utf-8', errors='ignore') or "no_output"}
    except FileNotFoundError:
        return {"url": url, "error": "ffprobe_not_installed"}

def parse_probe(probe):
    info = {"has_video": False, "has_audio": False, "video_codec": None, "width": None, "height": None, "frame_rate": None, "duration": None, "bit_rate": None}
    if not probe: 
        return info
    streams = probe.get("streams", [])
    for s in streams:
        if s.get("codec_type") == "video":
            info["has_video"] = True
            info["video_codec"] = s.get("codec_name")
            info["width"] = s.get("width")
            info["height"] = s.get("height")
            r = s.get("avg_frame_rate") or s.get("r_frame_rate")
            if r and "/" in str(r):
                num, den = r.split("/")
                try:
                    info["frame_rate"] = float(num) / float(den) if float(den) != 0 else None
                except Exception:
                    info["frame_rate"] = None
    fmt = probe.get("format", {})
    info["duration"] = float(fmt.get("duration")) if fmt.get("duration") else None
    info["bit_rate"] = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None
    if any(s.get("codec_type") == "audio" for s in streams):
        info["has_audio"] = True
    return info

async def probe_one(url, sem, timeout):
    async with sem:
        res = await ffprobe_json(url, timeout=timeout)
        if "probe" in res:
            parsed = parse_probe(res["probe"])
            parsed["url"] = url
            parsed["error"] = ""
            return parsed
        else:
            return {"url": url, "has_video": False, "has_audio": False, "video_codec": None, "width": None, "height": None, "frame_rate": None, "duration": None, "bit_rate": None, "error": res.get("error","unknown")}

async def run_all(urls, concurrency=30, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [probe_one(u, sem, timeout) for u in urls]
    results = []

    total = len(tasks)
    checked = 0
    has_video_count = 0
    last_log_time = time.time()
    log_interval = 2  # 每2秒打印一次状态

    for fut in tqdm_asyncio.as_completed(tasks, desc="deep-scan", total=total):
        r = await fut
        results.append(r)
        checked += 1
        if r.get("has_video"):
            has_video_count += 1

        now = time.time()
        if now - last_log_time >= log_interval or checked == total:
            print(f"deep-scan: {checked}/{total} done, has_video: {has_video_count} ({has_video_count/checked:.1%}), concurrency={concurrency}, timeout={timeout}s")
            last_log_time = now

    return results

def read_fast_scan(path):
    urls = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ok = r.get("ok","").lower()
            if ok in ("true","1","yes"):
                urls.append(r.get("url"))
    return urls

def write_out(results, outpath=OUTPUT):
    fieldnames = ["url","has_video","has_audio","video_codec","width","height","frame_rate","duration","bit_rate","error","视频信息"]
    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            video_info_str = ""
            if r.get("has_video"):
                video_info_str = f"{r.get('width') or '?'}x{r.get('height') or '?'} @{r.get('frame_rate') or '?'}fps, duration {r.get('duration') or '?'}s, bitrate {r.get('bit_rate') or '?'}bps"
            row = {k: r.get(k, "") for k in fieldnames[:-1]}
            row["视频信息"] = video_info_str
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--timeout", type=int, default=20)
    args = p.parse_args()

    urls = read_fast_scan(args.input)
    print(f"Probing {len(urls)} urls with concurrency={args.concurrency}")
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, timeout=args.timeout))
    write_out(results, args.output)
    ok = sum(1 for r in results if r.get("has_video"))
    print(f"Deep scan finished: {ok}/{len(results)} have video. Wrote {args.output}")

if __name__ == "__main__":
    main()