#!/usr/bin/env python3
# scripts/4.2deep_scan.py
import asyncio
import csv
import json
import argparse
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import os

INPUT = "output/middle/fast_scan.csv"
OUTPUT_OK = "output/middle/deep_scan.csv"             # 成功文件名
OUTPUT_FAIL = "output/middle/deep_scan_invalid.csv"   # 失败文件名

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

async def probe_one(row, sem, timeout):
    async with sem:
        url = row.get("地址", "") or row.get("url", "")
        res = await ffprobe_json(url, timeout=timeout)
        base_info = {k: row.get(k, "") for k in ["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频信息"]}
        if "probe" in res:
            parsed = parse_probe(res["probe"])
            video_info_str = ""
            if parsed.get("has_video"):
                video_info_str = f"{parsed.get('width') or '?'}x{parsed.get('height') or '?'} @{parsed.get('frame_rate') or '?'}fps, duration {parsed.get('duration') or '?'}s, bitrate {parsed.get('bit_rate') or '?'}bps"
            return {
                **base_info,
                "has_video": parsed.get("has_video", False),
                "has_audio": parsed.get("has_audio", False),
                "video_codec": parsed.get("video_codec"),
                "width": parsed.get("width"),
                "height": parsed.get("height"),
                "frame_rate": parsed.get("frame_rate"),
                "duration": parsed.get("duration"),
                "bit_rate": parsed.get("bit_rate"),
                "error": "",
                "视频信息": video_info_str
            }
        else:
            return {
                **base_info,
                "has_video": False,
                "has_audio": False,
                "video_codec": None,
                "width": None,
                "height": None,
                "frame_rate": None,
                "duration": None,
                "bit_rate": None,
                "error": res.get("error", "unknown"),
                "视频信息": ""
            }

async def run_all(rows, concurrency=30, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [probe_one(row, sem, timeout) for row in rows]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="deep-scan", total=len(tasks)):
        r = await fut
        results.append(r)
    return results

def read_fast_scan(path):
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # 只取状态为成功的行
            if r.get("状态", "").lower() == "成功":
                rows.append(r)
    return rows

def write_out(results, output_ok, output_fail):
    # 成功写入列
    fieldnames_ok = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频信息"]
    # 失败写入列，多加 error
    fieldnames_fail = fieldnames_ok + ["error"]

    valid_rows = [r for r in results if r.get("error") == "" and r.get("has_video")]
    invalid_rows = [r for r in results if r.get("error") != "" or not r.get("has_video")]

    os.makedirs(os.path.dirname(output_ok), exist_ok=True)
    with open(output_ok, "w", newline='', encoding='utf-8') as f_ok, \
         open(output_fail, "w", newline='', encoding='utf-8') as f_fail:
        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames_ok)
        writer_fail = csv.DictWriter(f_fail, fieldnames=fieldnames_fail)
        writer_ok.writeheader()
        writer_fail.writeheader()
        for r in valid_rows:
            out_row = {k: r.get(k, "") for k in fieldnames_ok}
            writer_ok.writerow(out_row)
        for r in invalid_rows:
            out_row = {k: r.get(k, "") for k in fieldnames_fail}
            writer_fail.writerow(out_row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=INPUT)
    p.add_argument("--output_ok", default=OUTPUT_OK)
    p.add_argument("--output_fail", default=OUTPUT_FAIL)
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--timeout", type=int, default=20)
    args = p.parse_args()

    rows = read_fast_scan(args.input)
    print(f"Probing {len(rows)} urls with concurrency={args.concurrency}")
    results = asyncio.run(run_all(rows, concurrency=args.concurrency, timeout=args.timeout))
    write_out(results, args.output_ok, args.output_fail)
    ok_count = sum(1 for r in results if r.get("error") == "" and r.get("has_video"))
    print(f"Deep scan finished: {ok_count}/{len(results)} have video. Wrote {args.output_ok} and {args.output_fail}")

if __name__ == "__main__":
    main()