#!/usr/bin/env python3
# scripts/4.3final_scan.py
import argparse
import csv
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from PIL import Image
import io
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import json
import os
import chardet

DEEP_INPUT = "output/middle/deep_scan.csv"

CACHE_DIR = "output/cache"
CHUNK_CACHE_DIR = os.path.join(CACHE_DIR, "chunk")
CACHE_FILE = os.path.join(CACHE_DIR, "cache_hashes.json")

# ----- aHash implementation (64-bit) -----
def image_to_ahash_bytes(img_bytes, hash_size=8):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((hash_size, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    avg = sum(pixels) / len(pixels)
    bits = 0
    for p in pixels:
        bits = (bits << 1) | (1 if p > avg else 0)
    return bits  # integer representing hash

def hamming(a, b):
    x = a ^ b
    return x.bit_count()

async def grab_frame(url, at_time=1, timeout=15):
    cmd = ["ffmpeg", "-ss", str(at_time), "-i", url, "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg", "pipe:1", "-hide_banner", "-loglevel", "error"]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return None, "timeout"
        if stdout:
            return stdout, ""
        else:
            return None, stderr.decode('utf-8', errors='ignore') or "no_output"
    except FileNotFoundError:
        return None, "ffmpeg_not_installed"

# ✅ 修改：所有分块扫描都读取主缓存，只写入自己的 chunk 缓存
def load_cache(chunk_id=None):
    """
    始终从主缓存 output/cache/cache_hashes.json 加载。
    不再尝试加载 chunk 缓存。
    """
    os.makedirs(CHUNK_CACHE_DIR, exist_ok=True)
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(data, chunk_id=None):
    """
    如果有 chunk_id，只保存 chunk 缓存；
    不再覆盖主缓存。
    """
    os.makedirs(CHUNK_CACHE_DIR, exist_ok=True)
    if chunk_id:
        chunk_cache_file = os.path.join(CHUNK_CACHE_DIR, f"cache_hashes_chunk_{chunk_id}.json")
        with open(chunk_cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    else:
        pass  # 不再保存主缓存

async def process_one(url, sem, cache, timeout=20):
    async with sem:
        old_hash = cache.get(url)
        img_bytes, err = await grab_frame(url, at_time=1, timeout=timeout)
        if not img_bytes:
            return {"url": url, "status": "error", "errors": [err], "is_fake": False, "similarity": 0.0, "hashes": []}
        try:
            new_hash = image_to_ahash_bytes(img_bytes)
        except Exception as e:
            return {"url": url, "status": f"hash_error:{e}", "errors": [], "is_fake": False, "similarity": 0.0, "hashes": []}

        if old_hash is not None:
            bits = 64
            d = hamming(new_hash, old_hash)
            sim = 1.0 - (d / bits)
            is_fake = sim >= 0.95
        else:
            sim = 0.0
            is_fake = False

        cache[url] = new_hash

        return {"url": url, "status": "ok", "errors": [], "is_fake": is_fake, "similarity": sim, "hashes": [new_hash]}

async def run_all(urls, concurrency=6, cache=None, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [process_one(u, sem, cache, timeout=timeout) for u in urls]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="final-scan", total=len(tasks)):
        r = await fut
        results.append(r)
    return results

def read_deep_input(path):
    urls = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            url = r.get("地址") or r.get("url")
            if url:
                urls.append(url)
    return urls

def write_final(results, input_path, final_out=None, final_invalid_out=None):
    final_map = {r["url"]: r for r in results}

    with open(input_path, "rb") as fb:
        raw = fb.read(20000)
        detected_enc = chardet.detect(raw)["encoding"] or "utf-8"

    with open(input_path, newline='', encoding=detected_enc, errors='ignore') as fin, \
         open(final_out, "w", newline='', encoding='utf-8') as fvalid, \
         open(final_invalid_out, "w", newline='', encoding='utf-8') as finvalid:

        reader = csv.DictReader(fin)

        # 设定输出字段
        fields = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "视频编码", "分辨率", "帧率", "音频", "哈希相似度"]

        w_valid = csv.DictWriter(fvalid, fieldnames=fields)
        w_invalid = csv.DictWriter(finvalid, fieldnames=fields + ["未通过信息"])

        w_valid.writeheader()
        w_invalid.writeheader()

        for row in reader:
            url = (row.get("地址") or row.get("url") or "").strip()
            if not url:
                continue
            r = final_map.get(url)
            passed = False
            similarity = ""
            fail_reason = ""

            if r:
                if r.get("status") == "ok" and not r.get("is_fake", False):
                    passed = True
                    similarity = round(r.get("similarity", 0), 4)
                else:
                    fail_reason = r.get("status", "")
                    if r.get("is_fake", False):
                        fail_reason += "; 伪源相似度: {:.4f}".format(r.get("similarity", 0))
                    similarity = round(r.get("similarity", 0), 4)

            out_row = {
                "频道名": row.get("频道名", ""),
                "地址": url,
                "来源": row.get("来源", ""),
                "图标": row.get("图标", ""),
                "检测时间": row.get("检测时间", ""),
                "分组": row.get("分组", ""),
                "视频编码": row.get("视频编码", ""),
                "分辨率": row.get("分辨率", ""),
                "帧率": row.get("帧率", ""),
                "音频": row.get("音频", ""),
                "哈希相似度": similarity
            }

            if passed:
                w_valid.writerow(out_row)
            else:
                out_row["未通过信息"] = fail_reason or "未知错误"
                w_invalid.writerow(out_row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEEP_INPUT)
    p.add_argument("--chunk_id", type=str, default=None, help="Chunk ID，用于分块缓存")
    p.add_argument("--cache_dir", type=str, default="output/cache", help="缓存目录")
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--working_gbk", action="store_true", help="是否生成 GBK 编码的 working.csv")
    args = p.parse_args()

    global CACHE_DIR, CHUNK_CACHE_DIR, CACHE_FILE
    CACHE_DIR = args.cache_dir
    CHUNK_CACHE_DIR = os.path.join(CACHE_DIR, "chunk")
    CACHE_FILE = os.path.join(CACHE_DIR, "cache_hashes.json")

    input_dir = os.path.dirname(args.input)
    if input_dir:
        os.makedirs(input_dir, exist_ok=True)

    urls = read_deep_input(args.input)
    print(f"Final-stage checking {len(urls)} urls (chunk_id={args.chunk_id})")

    cache = load_cache(args.chunk_id)
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, cache=cache, timeout=args.timeout))
    save_cache(cache, args.chunk_id)

    os.makedirs("output/chunk_final_scan", exist_ok=True)
    input_name = os.path.splitext(os.path.basename(args.input))[0]

    final_out = os.path.join("output/chunk_final_scan", f"final_{input_name}.csv")
    final_invalid_out = os.path.join("output/chunk_final_scan", f"final_invalid_{input_name}.csv")

    write_final(
        results,
        input_path=args.input,
        final_out=final_out,
        final_invalid_out=final_invalid_out,
    )

    fake_count = sum(1 for r in results if r.get("is_fake"))
    print(f"Final scan finished. Fake found: {fake_count}/{len(results)}. Wrote outputs to output/chunk_final_scan")

if __name__ == "__main__":
    main()
