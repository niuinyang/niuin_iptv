import csv
import os
from tqdm import tqdm

INPUT_CSV = "output/middle/stage2b_verified.csv"
OUTPUT_SNAPSHOT = "output/middle/stage2c_final.csv"
OUTPUT_FINAL = "output/working.csv"

SAVE_INTERVAL = 500

def process_item(item):
    # 这里示例根据 ffprobe 结果做最终判断或处理，具体你自己写逻辑
    # 例如只保留有效且无错误的
    if "✅有效" in item and "❌错误" not in item:
        return item
    return None

def main():
    if os.path.exists(OUTPUT_SNAPSHOT):
        print(f"🔄 恢复检测，从快照加载：{OUTPUT_SNAPSHOT}")
        with open(OUTPUT_SNAPSHOT, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))
    else:
        print(f"🚀 开始第3阶段最终处理")
        with open(INPUT_CSV, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))

    total = len(rows)
    results = []
    start_idx = 0

    if os.path.exists(OUTPUT_SNAPSHOT):
        start_idx = len(rows)
        if start_idx >= total:
            print("✔️ 快照已完成检测，跳过")
            return

    pbar = tqdm(total=total, desc="处理进度", unit="条", initial=start_idx)
    for idx in range(start_idx, total):
        item = rows[idx]
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

    if os.path.exists(OUTPUT_SNAPSHOT):
        os.remove(OUTPUT_SNAPSHOT)
        print(f"🗑️ 快照文件已删除：{OUTPUT_SNAPSHOT}")

    print(f"✅ 阶段3完成，结果输出：{OUTPUT_FINAL}")

if __name__ == "__main__":
    main()
