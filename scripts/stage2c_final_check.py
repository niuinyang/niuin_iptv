import csv
import os
from tqdm import tqdm

INPUT_CSV = "output/middle/stage2b_verified.csv"
OUTPUT_SNAPSHOT = "output/middle/stage2c_final.csv"
OUTPUT_FINAL = "output/working.csv"

SAVE_INTERVAL = 500

def process_item(item):
    detect_result = item[4] if len(item) > 4 else ""
    ffprobe_result = item[5] if len(item) > 5 else ""

    if "✅有效" in detect_result and "❌错误" not in ffprobe_result:
        return item
    return None

def main():
    print(f"🚀 开始第3阶段最终处理")
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.reader(f))

    total = len(rows)
    results = []

    pbar = tqdm(total=total, desc="处理进度", unit="条")

    for idx, item in enumerate(rows):
        processed = process_item(item)
        if processed:
            results.append(processed)
        pbar.update(1)

        if (idx + 1) % SAVE_INTERVAL == 0 or (idx + 1) == total:
            with open(OUTPUT_SNAPSHOT, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(results)
            print(f"💾 已保存快照：{len(results)}/{total}")

    pbar.close()

    with open(OUTPUT_FINAL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(results)

    # 注释掉删除快照，保留文件，避免 git 报错
    # if os.path.exists(OUTPUT_SNAPSHOT):
    #     os.remove(OUTPUT_SNAPSHOT)
    #     print(f"🗑️ 快照文件已删除：{OUTPUT_SNAPSHOT}")

    print(f"✅ 阶段3完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    main()
