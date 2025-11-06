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

DEEP_INPUT = "output/middle/deep_scan.csv"
FINAL_OUT = "output/middle/final_scan.csv"
WORKING_OUT = "output/working.csv"

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
    # Use ffmpeg to grab 1 frame at timestamp at_time (seconds)
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

async def process_one(url, sem, samples=3, interval=600, start_offset=1, timeout=20):
    async with sem:
        hashes = []
        errors = []
        for i in range(samples):
            at = start_offset + i * interval
            img_bytes, err = await grab_frame(url, at_time=at, timeout=timeout)
            if img_bytes:
                try:
                    h = image_to_ahash_bytes(img_bytes)
                    hashes.append(h)
                except Exception as e:
                    errors.append(f"hash_err:{e}")
            else:
                errors.append(err)
        if len(hashes) < max(1, samples//2):
            # not enough successful captures -> unknown
            return {"url": url, "status": "insufficient_frames", "hashes": hashes, "errors": errors, "is_fake": False, "similarity": 0.0}
        # compute pairwise normalized similarity (1 - normalized hamming)
        bits = 64  # hash_size=8 => 64 bits
        pairs = 0
        sim_total = 0.0
        for i in range(len(hashes)):
            for j in range(i+1, len(hashes)):
                pairs += 1
                d = hamming(hashes[i], hashes[j])
                sim = 1.0 - (d / bits)
                sim_total += sim
        avg_sim = sim_total / pairs if pairs else 0.0
        # is_fake when avg_sim very high (i.e., frames highly similar). threshold default 0.95 => diff <5%
        is_fake = avg_sim >= 0.95
        return {"url": url, "status": "ok", "hashes": hashes, "errors": errors, "is_fake": is_fake, "similarity": avg_sim}

async def run_all(urls, concurrency=6, samples=3, interval=600, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [process_one(u, sem, samples=samples, interval=interval, timeout=timeout) for u in urls]
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
            has_video = r.get("has_video","").lower() in ("true","1","yes")
            if has_video:
                urls.append(r.get("url"))
    return urls

def write_final(results, final_out=FINAL_OUT, working_out=WORKING_OUT):
    # final csv with per-url result summary
    fieldnames = ["url","status","is_fake","similarity","hashes","errors"]
    with open(final_out, "w", newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = { "url": r["url"], "status": r.get("status",""), "is_fake": r.get("is_fake",False), "similarity": r.get("similarity",0.0), "hashes": "|".join(str(h) for h in r.get("hashes",[])), "errors": "|".join(r.get("errors",[]))}
            w.writerow(row)
    # create working.csv: take deep_scan rows but exclude is_fake
    # We'll try to merge deep_scan.csv + final results by url; keep first occurrence per url
    final_map = {r["url"]: r for r in results}
    # read deep_scan, write working_out with deep fields for urls not fake and unique
    seen = set()
    with open("output/middle/deep_scan.csv", newline='', encoding='utf-8') as fin, open(working_out, "w", newline='', encoding='utf-8') as fout:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []
        # extend with is_fake, similarity
        out_fields = fieldnames + ["is_fake","similarity"]
        w = csv.DictWriter(fout, fieldnames=out_fields)
        w.writeheader()
        for r in reader:
            url = r.get("url")
            if not url or url in seen:
                continue
            seen.add(url)
            f = final_map.get(url)
            if f and f.get("is_fake"):
                continue
            row = {k: r.get(k,"") for k in fieldnames}
            row["is_fake"] = f.get("is_fake") if f else ""
            row["similarity"] = f.get("similarity") if f else ""
            w.writerow(row)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEEP_INPUT)
    p.add_argument("--final", default=FINAL_OUT)
    p.add_argument("--working", default=WORKING_OUT)
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--samples", type=int, default=3)
    p.add_argument("--interval", type=int, default=600, help="seconds between samples (for CI/testing set small value)")
    p.add_argument("--timeout", type=int, default=20)
    args = p.parse_args()

    urls = read_deep_input(args.input)
    print(f"Final-stage checking {len(urls)} urls (samples={args.samples}, interval={args.interval}s)")
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, samples=args.samples, interval=args.interval, timeout=args.timeout))
    write_final(results, final_out=args.final, working_out=args.working)
    fake_count = sum(1 for r in results if r.get("is_fake"))
    print(f"Final scan finished. Fake found: {fake_count}/{len(results)}. Wrote {args.final} and {args.working}")

if __name__ == "__main__":
    main()