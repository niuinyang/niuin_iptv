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
OUTPUT = "output/middle/deep_scan.csv"
OUTPUT_INVALID = "output/middle/deep_scan_invalid.csv"

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
    url = row["地址"]
    async with sem:
        res = await ffprobe_json(url, timeout=timeout)
        if "probe" in res:
            parsed = parse_probe(res["probe"])
            parsed.update({
                "频道名": row.get("频道名",""),
                "地址": url,
                "来源": row.get("来源",""),
                "图标": row.get("图标",""),
                "检测时间": row.get("检测时间",""),
                "分组": row.get("分组","未分组"),
                "视频信息": "",
                "error": "",
            })
            return parsed
        else:
            return {
                "频道名": row.get("频道名",""),
                "地址": url,
                "来源": row.get("来源",""),
                "图标": row.get("图标",""),
                "检测时间": row.get("检测时间",""),
                "分组": row.get("分组","未分组"),
                "视频信息": "",
                "error": res.get("error","unknown"),
                "has_video": False,
                "has_audio": False,
                "video_codec": None,
                "width": None,
                "height": None,
                "frame_rate": None,
                "duration": None,
                "bit_rate": None,
            }

async def run_all(rows, output, output_invalid, concurrency=30, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [probe_one(row, sem, timeout) for row in rows]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="deep-scan", total=len(tasks), ncols=80):
        r = await fut
        results.append(r)
    # 分离有效和无效
    valid = [r for r in results if r.get("has_video")]
    invalid = [r for r in results if not r.get("has_video")]

    # 生成视频信息字段
    def gen_video_info(r):
        if r.get("has_video"):
            return f"{r.get('width') or '?'}x{r.get('height') or '?'} @{r.get('frame_rate') or '?'}fps, duration {r.get('duration') or '?'}s, bitrate {r.get('bit_rate') or '?'}bps"
        else:
            return ""

    for r in valid:
        r["视频信息"] = gen_video_info(r)
    for r in invalid:
        r["视频信息"] = ""

    fieldnames = ["频道名","地址","来源","图标","检测时间","分组","视频信息"]

    # 写成功文件
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in valid:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    # 写失败文件，增加 error 列
    fieldnames_invalid = fieldnames + ["error"]
    with open(output_invalid, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_invalid)
        writer.writeheader()
        for r in invalid:
            writer.writerow({k: r.get(k, "") for k in fieldnames_invalid})

    print(f"Deep scan finished: {len(valid)}/{len(results)} have video. Wrote {output} and {output_invalid}", flush=True)

def read_fast_scan(path):
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # 不再判断检测时间，全部读取
            rows.append(r)
    return rows

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=INPUT)
    p.add_argument("--output", "-o", default=OUTPUT)
    p.add_argument("--invalid", default=OUTPUT_INVALID)
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--timeout", type=int, default=20)
    args = p.parse_args()

    rows = read_fast_scan(args.input)
    print(f"Probing {len(rows)} urls with concurrency={args.concurrency}")
    asyncio.run(run_all(rows, args.output, args.invalid, args.concurrency, args.timeout))

if __name__ == "__main__":
    main()