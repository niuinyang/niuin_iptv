#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta

CACHE_DIR = "output/cache/chunk"
TOTAL_CACHE_FILE = "output/cache/total_cache.json"

def merge_caches():
    # 只合并最近一天缓存目录（或者你想合并所有当天缓存文件）
    today = datetime.now().strftime("%Y%m%d")
    cache_path = os.path.join(CACHE_DIR, today)
    if not os.path.exists(cache_path):
        print(f"No cache directory for today: {cache_path}")
        return

    merged = {}
    for fname in os.listdir(cache_path):
        if not fname.endswith("_cache.json"):
            continue
        full_path = os.path.join(cache_path, fname)
        with open(full_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for url, v in data.items():
                if url not in merged:
                    merged[url] = {}
                for timepoint, hashes in v.items():
                    merged[url][timepoint] = hashes

    os.makedirs(os.path.dirname(TOTAL_CACHE_FILE), exist_ok=True)
    with open(TOTAL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"Merged cache saved to {TOTAL_CACHE_FILE}")

def main():
    merge_caches()

if __name__ == "__main__":
    main()
