#!/usr/bin/env python3
# scripts/4.2deep_scan.py
import asyncio
import csv
import json
import argparse
import shlex
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from asyncio import Semaphore

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
            # frame_rate might be reported like "25/1"
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
    # audio presence
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

async def run_all(urls, concurrency=10, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [probe_one(u, sem, timeout) for u in urls]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="deep-scan", total=len(tasks)):
        r = await fut
        results.append(r)
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
    fieldnames = ["url","has_video","has_audio","video_codec","width","height","frame_rate","duration","bit_rate","error"]
    with open(outpath, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--concurrency", type=int, default=10)
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