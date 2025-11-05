import csv
import subprocess
import os
import time
from tqdm import tqdm

INPUT_FILE = "output/middle/stage2a_valid.csv"
OUTPUT_FILE = "output/middle/stage2b_verified.csv"
CHECKPOINT_FILE = "output/middle/stage2b_checkpoint.csv"
SAVE_INTERVAL = 500

def check_ffprobe(url):
    try:
        cmd = ["ffprobe", "-v", "error", "-show_streams", "-show_format", url]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        return "✅通过" if result.returncode == 0 else "❌失败"
    except Exception as e:
        return f"❌错误:{str(e)[:30]}"

def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    print("🚀 开始第2阶段检测（FFprobe验证）")

    # --- 自动恢复 ---
    completed_urls = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, newline='', encoding='utf-8') as f:
            completed_urls = {r[1] for r in csv.reader(f)}
        print(f"🔄 检测到已有 {len(completed_urls)} 条快照，将跳过这些源")

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        header = reader[0] + ["ffprobe结果"]
        rows = [r for r in reader[1:] if r[1] not in completed_urls]

    print(f"📦 当前待检测源数：{len(rows)}")

    results, count = [], 0
    with tqdm(total=len(rows), ncols=90, desc="检测进度") as pbar:
        for row in rows:
            result = row + [check_ffprobe(row[1])]
            results.append(result)
            count += 1
            pbar.update(1)

            if count % SAVE_INTERVAL == 0:
                with open(CHECKPOINT_FILE, 'w', newline='', encoding='utf-8') as f:
                    csv.writer(f).writerows(results)
                print(f"💾 已保存快照：{count}/{len(rows)}")

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(results)

    print(f"✅ 阶段2完成，结果输出：{OUTPUT_FILE}")

if __name__ == "__main__":
    main()
