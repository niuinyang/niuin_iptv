import csv
import os

INPUT_FILE = "output/middle/stage2b_verified.csv"
OUTPUT_FILE_STAGE = "output/middle/stage2c_final.csv"
OUTPUT_WORKING = "output/working.csv"

os.makedirs(os.path.dirname(OUTPUT_FILE_STAGE), exist_ok=True)

def is_fake_source(url):
    fake_keywords = ["sample", "test", "error", "dummy", "invalid", "null"]
    return any(k in url.lower() for k in fake_keywords)

def main():
    results = []
    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for i, row in enumerate(rows, 1):
        if not is_fake_source(row["地址"]):
            results.append(row)
        if i % 200 == 0:
            print(f"🧹 假源过滤 {i}/{len(rows)}")

    # 输出中间与最终文件
    with open(OUTPUT_FILE_STAGE, "w", newline='', encoding='utf-8') as f1, \
         open(OUTPUT_WORKING, "w", newline='', encoding='utf-8') as f2:
        writer1 = csv.DictWriter(f1, fieldnames=rows[0].keys())
        writer2 = csv.DictWriter(f2, fieldnames=rows[0].keys())
        writer1.writeheader()
        writer2.writeheader()
        writer1.writerows(results)
        writer2.writerows(results)

    print(f"✅ 输出完成，共 {len(results)} 条有效源")

if __name__ == "__main__":
    main()
