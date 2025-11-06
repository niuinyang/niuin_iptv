#!/usr/bin/env python3
# scripts/4.2deep_scan.py
import asyncio
import csv
import json
import argparse
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore

INPUT = "output/middle/fast_scan.csv"
OUTPUT_OK = "output/middle/deep_scan.csv"
OUTPUT_FAIL = "output/middle/deep_scan_invalid.csv"

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

async def probe_one(item, sem, timeout):
    """
    item 是字典，包含原始行所有字段，至少有 url(地址)
    """
    url = item.get("地址") or item.get("url") or item.get("URL")
    async with sem:
        res = await ffprobe_json(url, timeout=timeout)
        if "probe" in res:
            parsed = parse_probe(res["probe"])
            parsed.update(item)
            parsed["error"] = ""
            return parsed
        else:
            r = {
                "has_video": False,
                "has_audio": False,
                "video_codec": None,
                "width": None,
                "height": None,
                "frame_rate": None,
                "duration": None,
                "bit_rate": None,
                "error": res.get("error", "unknown"),
            }
            r.update(item)
            return r

async def run_all(items, output_ok, output_fail, concurrency=30, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [probe_one(item, sem, timeout) for item in items]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="deep-scan", total=len(tasks)):
        r = await fut
        results.append(r)

    # 写入文件
    import os
    os.makedirs(os.path.dirname(output_ok), exist_ok=True)
    os.makedirs(os.path.dirname(output_fail), exist_ok=True)

    fieldnames = ["频道名","地址","来源","图标","检测时间","分组","视频信息","has_video","has_audio","video_codec","width","height","frame_rate","duration","bit_rate","error"]

    with open(output_ok, "w", encoding="utf-8", newline='') as f_ok, \
         open(output_fail, "w", encoding="utf-8", newline='') as f_fail:
        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames)
        writer_fail = csv.DictWriter(f_fail, fieldnames=fieldnames)
        writer_ok.writeheader()
        writer_fail.writeheader()
        for r in results:
            # 生成视频信息字符串
            if r.get("has_video"):
                r["视频信息"] = f"{r.get('width') or '?'}x{r.get('height') or '?'} @{r.get('frame_rate') or '?'}fps, duration {r.get('duration') or '?'}s, bitrate {r.get('bit_rate') or '?'}bps"
            else:
                r["视频信息"] = ""

            if r.get("has_video"):
                writer_ok.writerow(r)
            else:
                writer_fail.writerow(r)
    print(f"Deep scan finished: {sum(1 for r in results if r.get('has_video'))}/{len(results)} have video. Wrote {output_ok} and {output_fail}")

def read_fast_scan(path):
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        items = []
        for row in reader:
            # 过滤检测时间，非空且不为0的才检测
            dt = row.get("检测时间", "")
            if dt and dt != "0":
                items.append(row)
        return items

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=INPUT)
    p.add_argument("--output_ok", default=OUTPUT_OK, help="成功输出文件")
    p.add_argument("--output_fail", default=OUTPUT_FAIL, help="失败输出文件")
    p.add_argument("--concurrency", "-c", type=int, default=30)
    p.add_argument("--timeout", "-t", type=int, default=20)
    args = p.parse_args()

    items = read_fast_scan(args.input)
    print(f"Probing {len(items)} urls with concurrency={args.concurrency}")
    asyncio.run(run_all(items, args.output_ok, args.output_fail, args.concurrency, args.timeout))

if __name__ == "__main__":
    main()