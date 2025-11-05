import csv
import subprocess
import os

INPUT_FILE = "output/middle/stage2a_valid.csv"
OUTPUT_FILE = "output/middle/stage2b_verified.csv"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

def check_with_ffprobe(url):
    cmd = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", url
    ]
    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        return True
    except:
        return False

def main():
    results = []
    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for i, row in enumerate(rows, 1):
        if check_with_ffprobe(row["地址"]):
            results.append(row)
        if i % 100 == 0:
            print(f"🔍 ffprobe检测 {i}/{len(rows)}")

    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
