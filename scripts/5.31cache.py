#!/usr/bin/env python3
import argparse
import os
import csv
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from PIL import Image
import io
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import json

CACHE_DIR = "output/cache/chunk"

# pHash, aHash, dHash 实现用 imagehash 库更方便，但此处自定义演示
import imagehash
from PIL import Image

async def grab_frame(url, at_time=1, timeout=15):
    cmd = ["ffmpeg", "-ss", str(at_time), "-i", url, "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg", "pipe:1", "-hide_banner", "-loglevel", "error"]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        if stdout:
            return stdout, ""
        else:
            return None, stderr.decode(errors="ignore") or "no_output"
    except Exception as e:
        return None, str(e)

def phash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    phash = imagehash.phash(im)
    return str(phash)

def ahash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    ahash = imagehash.average_hash(im)
    return str(ahash)

def dhash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    dhash = imagehash.dhash(im)
    return str(dhash)

async def process_one(url, sem, timeout=20):
    async with sem:
        img_bytes, err = await grab_frame(url, at_time=1, timeout=timeout)
        if not img_bytes:
            return {"url": url, "error": err, "phash": None, "ahash": None, "dhash": None}
        try:
            phash = phash_bytes(img_bytes)
            ahash = ahash_bytes(img_bytes)
            dhash = dhash_bytes(img_bytes)
            return {"url": url, "error": None, "phash": phash, "ahash": ahash, "dhash": dhash}
        except Exception as e:
            return {"url": url, "error": str(e), "phash": None, "ahash": None, "dhash": None}

async def main_async(input_file, timepoint, chunk_id, concurrency=6):
    sem = Semaphore(concurrency)
    urls = []
    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("地址") or row.get("url")
            if url:
                urls.append(url)

    results = []
    for fut in tqdm_asyncio.as_completed([process_one(url, sem) for url in urls], total=len(urls), desc=f"Cache {timepoint}"):
        res = await fut
        results.append(res)

    # 缓存路径：按日期存，例：output/cache/chunk/20251115/chunk-1_cache.json
    date_str = timepoint_date = datetime.now().strftime("%Y%m%d")
    chunk_cache_dir = os.path.join(CACHE_DIR, date_str)
    os.makedirs(chunk_cache_dir, exist_ok=True)
    cache_file = os.path.join(chunk_cache_dir, f"{chunk_id}_cache.json")

    # 读取旧缓存
    old_cache = {}
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            try:
                old_cache = json.load(f)
            except Exception:
                old_cache = {}

    # 更新三时间点哈希
    # 格式: {url: {"0811": {phash,ahash,dhash}, "1612": {...}, "2113": {...}}}
    for r in results:
        url = r["url"]
        if url not in old_cache:
            old_cache[url] = {}
        old_cache[url][timepoint] = {
            "phash": r["phash"],
            "ahash": r["ahash"],
            "dhash": r["dhash"],
            "error": r["error"]
        }

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(old_cache, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="chunk csv 文件路径")
    parser.add_argument("--timepoint", required=True, choices=["0811","1612","2113"], help="时间点")
    parser.add_argument("--chunk_id", required=True, help="chunk id")
    args = parser.parse_args()
    asyncio.run(main_async(args.input, args.timepoint, args.chunk_id))

if __name__ == "__main__":
    main()