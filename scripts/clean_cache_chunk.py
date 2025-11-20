#!/usr/bin/env python3
import os

TARGET_DIR = "output/cache/chunk"
PREFIX = "cache_hashes_chunk_chunk"
EXTENSION = ".json"

def clean_cache_chunk_files():
    if not os.path.exists(TARGET_DIR):
        print(f"{TARGET_DIR} 不存在，跳过删除。")
        return

    removed_files = []
    for fname in os.listdir(TARGET_DIR):
        if fname.startswith(PREFIX) and fname.endswith(EXTENSION):
            full_path = os.path.join(TARGET_DIR, fname)
            try:
                os.remove(full_path)
                removed_files.append(fname)
            except Exception as e:
                print(f"删除文件 {fname} 失败：{e}")

    if removed_files:
        print(f"已删除 {len(removed_files)} 个文件：")
        for f in removed_files:
            print(f"  - {f}")
    else:
        print("未找到匹配的文件，无需删除。")

if __name__ == "__main__":
    clean_cache_chunk_files()
