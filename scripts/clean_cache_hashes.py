#!/usr/bin/env python3
import os
import glob

def clean_cache_hashes_chunk_files():
    folder = "output/cache/chunk"
    pattern = os.path.join(folder, "*cache_hashes_chunk*.json")
    files = glob.glob(pattern)
    if not files:
        print(f"No files matching pattern '{pattern}' found.")
        return
    for file in files:
        try:
            os.remove(file)
            print(f"Deleted file: {file}")
        except Exception as e:
            print(f"Failed to delete {file}: {e}")

if __name__ == "__main__":
    clean_cache_hashes_chunk_files()
