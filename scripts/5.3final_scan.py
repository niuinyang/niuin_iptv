#!/usr/bin/env python3
# scripts/5.3final_scan.py
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

CACHE_DIR = "output/cache"
TOTAL_CACHE_FILE = os.path.join(CACHE_DIR, "total_cache.json")

# 哈希尺寸（64位）
HASH_SIZE = 8
HASH_BITS = HASH_SIZE * HASH_SIZE  # 64

# --- 计算ahash ---
def image_to_ahash_bytes(img_bytes, hash_size=HASH_SIZE):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((hash_size, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    avg = sum(pixels) / len(pixels)
    bits = 0
    for p in pixels:
        bits = (bits << 1) | (1 if p > avg else 0)
    return bits  # int

# --- 计算phash ---
def image_to_phash_bytes(img_bytes, hash_size=HASH_SIZE):
    # pHash 经典算法：先缩放到 32*32，再做 DCT，取左上8*8
    from PIL import ImageFilter
    import numpy as np

    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((32, 32), Image.Resampling.LANCZOS)
    pixels = np.array(im, dtype=np.float32)
    dct = dct2(pixels)
    dct_low_freq = dct[:hash_size, :hash_size]
    avg = dct_low_freq[1:,1:].mean()
    bits = 0
    for v in dct_low_freq.flatten():
        bits = (bits << 1) | (1 if v > avg else 0)
    return bits

def dct2(a):
    import numpy as np
    return np.round(np.real(np.fft.fft2(a)))

# --- 计算dhash ---
def image_to_dhash_bytes(img_bytes, hash_size=HASH_SIZE):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    bits = 0
    for row in range(hash_size):
        for col in range(hash_size):
            left_pixel = pixels[row * (hash_size + 1) + col]
            right_pixel = pixels[row * (hash_size + 1) + col + 1]
            bits = (bits << 1) | (1 if left_pixel > right_pixel else 0)
    return bits

def hamming(a, b):
    return (a ^ b).bit_count()

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

def load_cache(chunk_ids):
    """
    读取 total_cache.json，返回两个结果：
    - result: 结构化哈希字典
    - raw_cache: 原始完整缓存，供失败判断用
    """
    if not os.path.exists(TOTAL_CACHE_FILE):
        print(f"缓存文件不存在: {TOTAL_CACHE_FILE}")
        return {}, {}

    with open(TOTAL_CACHE_FILE, "r", encoding="utf-8") as f:
        raw_cache = json.load(f)

    result = {}
    for url, times in raw_cache.items():
        result[url] = {}
        for cid in chunk_ids:
            if cid in times:
                entry = times[cid]
                try:
                    phash_int = int(entry["phash"], 16)
                    ahash_int = int(entry["ahash"], 16)
                    dhash_int = int(entry["dhash"], 16)
                    result[url][cid] = {
                        "phash": phash_int,
                        "ahash": ahash_int,
                        "dhash": dhash_int
                    }
                except Exception as e:
                    print(f"转换哈希失败: url={url}, chunk_id={cid}, error={e}")
    return result, raw_cache

def is_all_timepoints_failed(url, chunk_ids, raw_cache):
    # 判断该 url 的所有时间点缓存是否全部失败（无哈希或全空）
    if url not in raw_cache:
        return True
    data = raw_cache[url]
    for cid in chunk_ids:
        if cid in data:
            entry = data[cid]
            # 有任意时间点有有效 phash，则不算全部失败
            if entry.get("phash") not in (None, "", "null") and entry.get("phash") is not None:
                return False
    # 全部都无效
    return True

def save_cache(data, chunk_id=None):
    # 不写缓存
    pass

async def process_one(url, sem, cache, chunk_ids, threshold=0.95, timeout=20):
    async with sem:
        has_cache = url in cache and any(cid in cache[url] for cid in chunk_ids)
        if has_cache:
            # 有缓存，只抓一帧
            img_bytes, err = await grab_frame(url, timeout=timeout)
            if not img_bytes:
                return {"url": url, "status": "error", "errors": [err], "is_fake": False, "similarity": 0.0, "hashes": []}
        else:
            # 无缓存，抓多帧，间隔0.5秒抓3帧（示例），超时改为较长
            imgs = []
            errors = []
            for t in [1, 1.5, 2]:
                img, err = await grab_frame(url, at_time=t, timeout=timeout+5)
                if img:
                    imgs.append(img)
                else:
                    errors.append(err)
            if not imgs:
                return {"url": url, "status": "error", "errors": errors, "is_fake": False, "similarity": 0.0, "hashes": []}
            # 多帧平均相似度
            phash_list = []
            ahash_list = []
            dhash_list = []
            for img_bytes in imgs:
                try:
                    phash_list.append(image_to_phash_bytes(img_bytes))
                    ahash_list.append(image_to_ahash_bytes(img_bytes))
                    dhash_list.append(image_to_dhash_bytes(img_bytes))
                except Exception as e:
                    return {"url": url, "status": f"hash_error:{e}", "errors": [], "is_fake": False, "similarity": 0.0, "hashes": []}

            # 对每帧与缓存对比，取最大相似度
            max_similarity = 0.0
            is_fake = False
            for phash_new, ahash_new, dhash_new in zip(phash_list, ahash_list, dhash_list):
                if url in cache:
                    for cid in chunk_ids:
                        if cid not in cache[url]:
                            continue
                        c = cache[url][cid]
                        sim_phash = 1.0 - hamming(phash_new, c["phash"]) / HASH_BITS
                        sim_ahash = 1.0 - hamming(ahash_new, c["ahash"]) / HASH_BITS
                        sim_dhash = 1.0 - hamming(dhash_new, c["dhash"]) / HASH_BITS
                        avg_sim = (sim_phash + sim_ahash + sim_dhash) / 3

                        if avg_sim > max_similarity:
                            max_similarity = avg_sim
                        if avg_sim >= threshold:
                            is_fake = True
                            break
                    if is_fake:
                        break
            return {
                "url": url,
                "status": "ok",
                "errors": [],
                "is_fake": is_fake,
                "similarity": max_similarity,
                "hashes": []
            }

        # 有缓存的单帧处理（上面部分），继续判断相似度
        try:
            phash_new = image_to_phash_bytes(img_bytes)
            ahash_new = image_to_ahash_bytes(img_bytes)
            dhash_new = image_to_dhash_bytes(img_bytes)
        except Exception as e:
            return {"url": url, "status": f"hash_error:{e}", "errors": [], "is_fake": False, "similarity": 0.0, "hashes": []}

        max_similarity = 0.0
        is_fake = False

        if url in cache:
            for cid in chunk_ids:
                if cid not in cache[url]:
                    continue
                c = cache[url][cid]
                sim_phash = 1.0 - hamming(phash_new, c["phash"]) / HASH_BITS
                sim_ahash = 1.0 - hamming(ahash_new, c["ahash"]) / HASH_BITS
                sim_dhash = 1.0 - hamming(dhash_new, c["dhash"]) / HASH_BITS
                avg_sim = (sim_phash + sim_ahash + sim_dhash) / 3

                if avg_sim > max_similarity:
                    max_similarity = avg_sim
                if avg_sim >= threshold:
                    is_fake = True
                    break

        return {
            "url": url,
            "status": "ok",
            "errors": [],
            "is_fake": is_fake,
            "similarity": max_similarity,
            "hashes": [phash_new, ahash_new, dhash_new]
        }

async def run_all(urls, concurrency, cache, chunk_ids, threshold=0.95, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [process_one(u, sem, cache, chunk_ids, threshold, timeout) for u in urls]
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

def write_final(results, input_path, final_out=None, final_invalid_out=None, raw_cache=None, chunk_ids=None):
    final_map = {r["url"]: r for r in results}

    with open(input_path, "rb") as fb:
        raw = fb.read(20000)
        detected_enc = chardet.detect(raw)["encoding"] or "utf-8"

    with open(input_path, newline='', encoding=detected_enc, errors='ignore') as fin, \
         open(final_out, "w", newline='', encoding='utf-8') as fvalid, \
         open(final_invalid_out, "w", newline='', encoding='utf-8') as finvalid:

        reader = csv.DictReader(fin)

        working_fields = [
            "频道名","地址","来源","图标","检测时间","分组",
            "视频编码","分辨率","帧率","音频"
        ]
        valid_fields = working_fields + ["相似度"]
        invalid_fields = working_fields + ["未通过信息", "相似度"]

        w_valid = csv.DictWriter(fvalid, fieldnames=valid_fields)
        w_invalid = csv.DictWriter(finvalid, fieldnames=invalid_fields)

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

            # 判断是否所有时间点都失败
            if raw_cache and chunk_ids and is_all_timepoints_failed(url, chunk_ids, raw_cache):
                fail_reason = "所有时间点抓帧失败"
                similarity = 0.0
            else:
                if r:
                    if r.get("status") == "ok" and not r.get("is_fake", False):
                        passed = True
                        similarity = round(r.get("similarity", 0), 4)
                    else:
                        fail_reason = r.get("status", "")
                        if r.get("is_fake", False):
                            fail_reason += "; 伪源相似度: {:.4f}".format(r.get("similarity", 0))
                        similarity = round(r.get("similarity", 0), 4)

            if passed:
                valid_row = {k: row.get(k, "") for k in working_fields}
                valid_row["相似度"] = similarity
                w_valid.writerow(valid_row)
            else:
                invalid_row = {k: row.get(k, "") for k in working_fields}
                invalid_row["未通过信息"] = fail_reason or "未知错误"
                invalid_row["相似度"] = similarity
                w_invalid.writerow(invalid_row)

def main():
    global CACHE_DIR, TOTAL_CACHE_FILE

    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="输入文件路径（deep_scan 输出）")
    p.add_argument("--output", required=True, help="最终有效输出文件完整路径（带后缀）")
    p.add_argument("--invalid", required=True, help="最终无效输出文件完整路径（带后缀）")
    p.add_argument("--chunk_ids", required=True, help="Chunk ID 列表，逗号分隔，例如 0811,1612,2113")
    p.add_argument("--cache_dir", default=CACHE_DIR, help="缓存目录（默认 output/cache）")
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--threshold", type=float, default=0.95, help="判定假源的相似度阈值，默认0.95")
    args = p.parse_args()

    CACHE_DIR = args.cache_dir
    TOTAL_CACHE_FILE = os.path.join(CACHE_DIR, "total_cache.json")

    chunk_ids = [cid.strip() for cid in args.chunk_ids.split(",") if cid.strip()]
    if not chunk_ids:
        print("请提供至少一个有效的chunk_id")
        return

    input_dir = os.path.dirname(args.input)
    if input_dir:
        os.makedirs(input_dir, exist_ok=True)

    urls = read_deep_input(args.input)
    print(f"Final-stage checking {len(urls)} urls (chunk_ids={chunk_ids})")

    cache, raw_cache = load_cache(chunk_ids)
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, cache=cache, chunk_ids=chunk_ids, threshold=args.threshold, timeout=args.timeout))

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    invalid_dir = os.path.dirname(args.invalid)
    if invalid_dir:
        os.makedirs(invalid_dir, exist_ok=True)

    write_final(
        results,
        input_path=args.input,
        final_out=args.output,
        final_invalid_out=args.invalid,
        raw_cache=raw_cache,
        chunk_ids=chunk_ids,
    )

    fake_count = sum(1 for r in results if r.get("is_fake"))
    print(f"Final scan finished. Fake found: {fake_count}/{len(results)}. Wrote outputs to {args.output} and {args.invalid}")

if __name__ == "__main__":
    main()