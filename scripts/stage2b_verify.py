import csv
import subprocess
import concurrent.futures
from tqdm import tqdm
import os

INPUT_CSV = "output/middle/stage2a_valid.csv"
OUTPUT_CSV = "output/middle/stage2b_verified.csv"
SNAPSHOT_INTERVAL = 500
MAX_WORKERS = 20  # 并发线程数，视服务器调整

def run_ffprobe(url):
    """调用 ffprobe 验证流，返回结果字符串或错误信息"""
    try:
        # ffprobe 命令，-v quiet 静默，-show_format 显示格式信息
        # timeout 10秒防止卡住
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            return "✅有效"
        else:
            return f"❌错误: {result.stderr.strip()[:100]}"
    except subprocess.TimeoutExpired:
        return "❌超时"
    except Exception as e:
        return f"❌异常: {str(e)}"

def process_row(row):
    url = row['地址']
    ffprobe_result = run_ffprobe(url)
    return {**row, 'ffprobe结果': ffprobe_result}

def save_snapshot(data, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def main():
    if not os.path.exists("output/middle"):
        os.makedirs("output/middle")

    # 读取待检测数据
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    results = []
    start_index = 0

    # 恢复检测，若快照文件存在则加载继续
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            results = list(reader)
        start_index = len(results)
        print(f"恢复检测，从第 {start_index} 条开始，共 {len(rows)} 条")

    total = len(rows)
    print(f"🚀 开始第2阶段检测（FFprobe验证）")
    print(f"📦 当前待检测源数：{total - start_index}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交剩余任务
        future_to_index = {
            executor.submit(process_row, rows[i]): i
            for i in range(start_index, total)
        }

        # 使用 tqdm 进度条监控
        for future in tqdm(concurrent.futures.as_completed(future_to_index), total=total - start_index, desc="检测进度"):
            idx = future_to_index[future]
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                # 出错时返回错误信息
                row = rows[idx]
                row['ffprobe结果'] = f"❌异常: {str(e)}"
                results.append(row)

            # 每500条保存快照，防止意外中断丢失进度
            if len(results) % SNAPSHOT_INTERVAL == 0:
                save_snapshot(results, OUTPUT_CSV)
                print(f"💾 已保存快照：{len(results)}/{total}")

    # 全部完成后保存最终结果
    save_snapshot(results, OUTPUT_CSV)
    print(f"✅ 阶段2完成，结果输出：{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
