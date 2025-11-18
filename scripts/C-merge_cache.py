#!/usr/bin/env python3
import os
import json
from datetime import datetime

CACHE_DIR = "output/cache/chunk"
TOTAL_CACHE_FILE = "output/cache/total_cache.json"
MERGE_RECORD_FILE = "output/cache/merge_record.json"

# 固定时间顺序
TIME_KEYS = ["0811", "1612", "2113"]

def load_merge_record():
    if os.path.exists(MERGE_RECORD_FILE):
        with open(MERGE_RECORD_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_merge_record(record):
    with open(MERGE_RECORD_FILE, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

def merge_caches():
    # 读取上次合并的日期
    merge_record = load_merge_record()
    last_merged_date = merge_record.get("last_merged_date", "")

    # 获取所有日期目录，按日期排序
    all_dates = sorted([d for d in os.listdir(CACHE_DIR) if d.isdigit()])

    # 筛选出未合并的日期
    if last_merged_date:
        dates_to_merge = [d for d in all_dates if d > last_merged_date]
    else:
        dates_to_merge = all_dates

    if not dates_to_merge:
        print("无新增缓存目录，无需合并。")
        return

    # 加载已有 total_cache.json
    if os.path.exists(TOTAL_CACHE_FILE):
        with open(TOTAL_CACHE_FILE, "r", encoding="utf-8") as f:
            merged = json.load(f)
    else:
        merged = {}

    # ================================
    #   合并所有 *_cache.json
    # ================================
    for date_dir in dates_to_merge:
        cache_path = os.path.join(CACHE_DIR, date_dir)
        if not os.path.exists(cache_path):
            continue

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
                    # ⚠ **确保哈希字段顺序也固定**
                    merged[url][timepoint] = {
                        "phash": hashes.get("phash"),
                        "ahash": hashes.get("ahash"),
                        "dhash": hashes.get("dhash"),
                        "error": hashes.get("error")
                    }

    # ================================
    #   最终排序（关键修改）
    # ================================
    sorted_merged = {}

    for url in sorted(merged.keys()):
        timepoint_dict = merged[url]
        ordered_timepoint = {}

        for tk in TIME_KEYS:
            if tk in timepoint_dict:
                ordered_timepoint[tk] = {
                    "phash": timepoint_dict[tk].get("phash"),
                    "ahash": timepoint_dict[tk].get("ahash"),
                    "dhash": timepoint_dict[tk].get("dhash"),
                    "error": timepoint_dict[tk].get("error"),
                }

        sorted_merged[url] = ordered_timepoint

    # ================================
    #   写入 total_cache.json
    # ================================
    os.makedirs(os.path.dirname(TOTAL_CACHE_FILE), exist_ok=True)
    with open(TOTAL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted_merged, f, ensure_ascii=False, indent=2)

    # 更新合并记录
    merge_record["last_merged_date"] = dates_to_merge[-1]
    save_merge_record(merge_record)

    print(f"合并完成 → {TOTAL_CACHE_FILE}，最新日期：{dates_to_merge[-1]}")

def main():
    merge_caches()

if __name__ == "__main__":
    main()
