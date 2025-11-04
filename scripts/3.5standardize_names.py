import os
import csv
import re
import pandas as pd
from rapidfuzz import process, fuzz

# ==============================
# 配置路径
# ==============================
INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
INPUT_NETWORK = "output/sum_network.csv"
OUTPUT_TOTAL = "output/total.csv"
OUTPUT_CHANNEL = "input/channel.csv"
OUTPUT_UNMATCHED = "output/unmatched_channels.txt"

os.makedirs("output", exist_ok=True)
os.makedirs("input", exist_ok=True)
os.makedirs("input/mysource", exist_ok=True)

# ==============================
# 清理频道名中括号内容
# ==============================
def clean_channel_name(name):
    if not isinstance(name, str):
        return ""
    # 去除括号及其中内容
    name = re.sub(r"[\[\(（【〔].*?[\]\)）】〕]", "", name)
    # 去掉多余空格和特殊符号
    return name.strip()

# ==============================
# 主逻辑
# ==============================
def main():
    print("🚀 开始执行标准化匹配流程...\n")

    print("读取源文件：")
    print(f"  📁 {INPUT_MY}")
    print(f"  📁 {INPUT_WORKING}")
    print(f"  📁 {INPUT_NETWORK}\n")

    # 读取CSV文件（使用UTF-8防止乱码）
    my_df = pd.read_csv(INPUT_MY, encoding="utf-8")
    working_df = pd.read_csv(INPUT_WORKING, encoding="utf-8")
    network_df = pd.read_csv(INPUT_NETWORK, encoding="utf-8")

    print(f"📦 处理自有源 my_sum.csv 共 {len(my_df)} 条")
    print(f"🌐 处理网络源 working.csv 共 {len(working_df)} 条\n")

    # 清理频道名
    working_df["频道名"] = working_df["频道名"].apply(clean_channel_name)
    network_df["频道名"] = network_df["频道名"].apply(clean_channel_name)

    # 获取所有自有源频道名
    my_channels = my_df["频道名"].dropna().unique().tolist()

    results = []
    unmatched_channels = []

    # ====== Step 1: 自有源，直接输出 ======
    for _, row in my_df.iterrows():
        results.append({
            "原频道名": row["频道名"],
            "来源": "自有源",
            "匹配值": 100.0,
            "标准化名": row["频道名"],
            "分组": "自有源"
        })

    # ====== Step 2: working.csv 匹配 ======
    network_channels = network_df["频道名"].dropna().unique().tolist()

    for _, row in working_df.iterrows():
        ch_name = str(row["频道名"]).strip()
        clean_name = clean_channel_name(ch_name)

        # 优先匹配自有源
        match_my = process.extractOne(
            clean_name, my_channels, scorer=fuzz.token_sort_ratio, score_cutoff=95
        )

        if match_my:
            matched_name, score_my, _ = match_my
            results.append({
                "原频道名": ch_name,
                "来源": "working",
                "匹配值": score_my,
                "标准化名": matched_name,
                "分组": "匹配自有源"
            })
            continue

        # 再匹配网络源
        match_net = process.extractOne(
            clean_name, network_channels, scorer=fuzz.token_sort_ratio
        )

        if match_net:
            matched_name, score_net, _ = match_net
            if score_net < 95:
                matched_name = ch_name  # 低于95直接保留原名
                note = f"未高匹配({score_net})"
            else:
                note = "匹配网络源"
            results.append({
                "原频道名": ch_name,
                "来源": "working",
                "匹配值": score_net,
                "标准化名": matched_name,
                "分组": note
            })
        else:
            results.append({
                "原频道名": ch_name,
                "来源": "working",
                "匹配值": 0,
                "标准化名": ch_name,
                "分组": "未匹配"
            })
            unmatched_channels.append(ch_name)

    # ====== 保存结果 ======
    total_df = pd.DataFrame(results)
    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")  # ✅ 修复乱码
    print(f"✅ 已生成标准化结果文件：{OUTPUT_TOTAL}")

    # 提取标准化名和分组
    channel_df = total_df[["标准化名", "分组"]].drop_duplicates()
    channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成频道名映射文件：{OUTPUT_CHANNEL}")

    # 保存未匹配列表
    if unmatched_channels:
        with open(OUTPUT_UNMATCHED, "w", encoding="utf-8-sig") as f:
            f.write("\n".join(unmatched_channels))
        print(f"⚠️ 未匹配频道已保存：{OUTPUT_UNMATCHED}")

    print("\n🎯 全部完成！")

if __name__ == "__main__":
    main()