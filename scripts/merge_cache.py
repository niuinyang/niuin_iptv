#!/usr/bin/env python3
# scripts/merge_caches.py
import os
import json

CACHE_DIR = "output/cache"
CHUNK_CACHE_DIR = os.path.join(CACHE_DIR, "chunk")
MAIN_CACHE_FILE = os.path.join(CACHE_DIR, "cache_hashes.json")

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def merge_caches():
    main_cache = load_json(MAIN_CACHE_FILE)
    print(f"Loaded main cache with {len(main_cache)} entries")

    chunk_files = [f for f in os.listdir(CHUNK_CACHE_DIR) if f.startswith("cache_hashes_chunk_") and f.endswith(".json")]

    merged_count = 0
    for cf in chunk_files:
        path = os.path.join(CHUNK_CACHE_DIR, cf)
        chunk_cache = load_json(path)
        if not chunk_cache:
            continue
        main_cache.update(chunk_cache)
        merged_count += len(chunk_cache)
        print(f"Merged {len(chunk_cache)} entries from {cf}")

    print(f"Total merged {merged_count} entries from chunk caches")

    save_json(main_cache, MAIN_CACHE_FILE)
    print(f"Saved merged main cache to {MAIN_CACHE_FILE}")

if __name__ == "__main__":
    merge_caches()