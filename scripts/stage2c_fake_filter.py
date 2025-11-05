import csv
import os
from tqdm import tqdm

INPUT_FILE = "output/middle/stage2b_verified.csv"
OUTPUT_FILE = "output/middle/stage2c_final.csv"
WORKING_FILE = "output/working.csv"
CHECKPOINT_FILE = "output/middle/stage2c_checkpoint.csv"
SAVE_INTERVAL = 500

def is_fake_source(name, url):
    name_l, url_l = name.lower(), url.lower()
    keywords = ["test", "ad", "demo", "sample", "fake", "error"]
    return any(k in name_l or k in url_l for k in keywords)

def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    print("🚀 开始第3阶段检测（假源过滤与结果合并）")

    completed_urls = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, newline='', encoding='utf-8') as f:
            completed_urls = {r[1] for r in csv.reader(f)}
        print(f"🔄 检测到已有 {len(completed_urls)} 条快照，将跳过这些源")

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        header = reader[0] + ["假源检测"]
        rows = [r for r in reader[1:] if r[1] not in completed_urls]

    print(f"📦 当前待检测源数：{len(rows)}")

    results, count = [], 0
    with tqdm(total=len(rows), ncols=90, desc="检测进度") as pbar:
        for row in rows:
            fake = "❌假源" if is_fake_source(row[0], row[1]) else "✅正常"
            results.append(row + [fake])
            count += 1
            pbar.update(1)
            if count % SAVE_INTERVAL == 0:
                with open(CHECKPOINT_FILE, 'w', newline='', encoding='utf-8') as f:
                    csv.writer(f).writerows(results)
                print(f"💾 已保存快照：{count}/{len(rows)}")

    # 输出最终结果
    for path in [OUTPUT_FILE, WORKING_FILE]:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(results)

    print(f"✅ 阶段3完成，最终文件输出：{WORKING_FILE}")

if __name__ == "__main__":
    main()
