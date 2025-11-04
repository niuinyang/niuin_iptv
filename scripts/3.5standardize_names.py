import os
import csv
import re
import time
from rapidfuzz import fuzz, process

# ==============================
# 配置区
# ==============================
INPUT_MY_SUM = "input/mysource/my_sum.csv"       # 自有源
INPUT_WORKING = "output/working.csv"             # 网络源
INPUT_NETWORK = "output/sum_network.csv"         # 网络匹配源
OUTPUT_TOTAL = "output/total.csv"                # 最终汇总输出
OUTPUT_CHANNEL = "input/channel.csv"             # 标准化映射输出
OUTPUT_UNMATCHED = "output/unmatched_channels.txt"

# ==============================
# 工具函数
# ==============================
def clean_channel_name(name):
    """去除频道名中括号或中括号内的无关标识"""
    if not name:
        return name
    name = re.sub(r'（.*?）', '', name)  # 中文括号
    name = re.sub(r'\(.*?\)', '', name)  # 英文括号
    name = re.sub(r'\[.*?\]', '', name)  # 中括号
    return name.strip()

def read_csv(file_path):
    """读取CSV，返回列表"""
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        rows = [row for row in reader if row]
    return rows

def write_csv(file_path, rows):
    """写入CSV"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

# ==============================
# 主逻辑
# ==============================
def main():
    start_time = time.time()
    print("🚀 开始执行标准化匹配流程...")
    print(f"读取源文件：\n  📁 {INPUT_MY_SUM}\n  📁 {INPUT_WORKING}\n  📁 {INPUT_NETWORK}")

    # 读取文件
    my_sum_rows = read_csv(INPUT_MY_SUM)
    working_rows = read_csv(INPUT_WORKING)
    network_rows = read_csv(INPUT_NETWORK)

    # 提取 my_sum 的频道名列表
    my_sum_names = [r[0].strip() for r in my_sum_rows if len(r) > 0]
    network_names = [r[0].strip() for r in network_rows if len(r) > 0]

    total_output = []
    unmatched_channels = set()
    channel_map = []

    # ========== 处理 my_sum.csv ==========
    print(f"📦 处理自有源 my_sum.csv 共 {len(my_sum_rows)} 条")
    for row in my_sum_rows:
        if len(row) < 4:
            continue
        name, group, url, source = row[:4]
        total_output.append([name, url, source, "", name, "自有源", "100.0"])
        channel_map.append([name, group])

    # ========== 处理 working.csv ==========
    print(f"🌐 处理网络源 working.csv 共 {len(working_rows)} 条")
    for idx, row in enumerate(working_rows, 1):
        if len(row) < 4:
            continue
        name, group, url, source = row[:4]
        cleaned_name = clean_channel_name(name)

        # Step 1: 优先匹配 my_sum.csv
        match_my, score_my = process.extractOne(
            cleaned_name, my_sum_names, scorer=fuzz.partial_ratio
        ) if my_sum_names else (None, 0)

        if score_my >= 95:
            standardized_name = match_my
            match_source = "my_sum匹配"
            score = score_my
        else:
            # Step 2: 再与网络匹配
            match_network, score_network = process.extractOne(
                cleaned_name, network_names, scorer=fuzz.partial_ratio
            ) if network_names else (None, 0)
            if score_network >= 95:
                standardized_name = match_network
                match_source = "网络匹配"
                score = score_network
            else:
                standardized_name = name
                match_source = "未匹配"
                score = 0.0
                unmatched_channels.add(name)

        total_output.append([
            name, url, source, group,
            standardized_name, match_source,
            f"{score:.1f}"
        ])
        channel_map.append([standardized_name, group])

        # 日志输出
        if idx % 100 == 0 or idx == len(working_rows):
            print(f"✅ 已处理 {idx}/{len(working_rows)} 条...")

    # 写入输出文件
    write_csv(OUTPUT_TOTAL, total_output)
    write_csv(OUTPUT_CHANNEL, channel_map)
    os.makedirs(os.path.dirname(OUTPUT_UNMATCHED), exist_ok=True)
    with open(OUTPUT_UNMATCHED, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(unmatched_channels)))

    duration = time.time() - start_time
    print(f"\n🎯 匹配完成，共处理 {len(total_output)} 条记录")
    print(f"📂 输出文件：{OUTPUT_TOTAL}")
    print(f"📂 标准化映射：{OUTPUT_CHANNEL}")
    print(f"⚠️ 未匹配频道数：{len(unmatched_channels)}（详情见 unmatched_channels.txt）")
    print(f"⏱️ 总耗时：{duration:.2f} 秒")

if __name__ == "__main__":
    main()