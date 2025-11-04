import os
import re
import pandas as pd
from rapidfuzz import process

# ==============================
# 配置区
# ==============================
IPTV_DB_PATH = "./iptv-database"
INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
INPUT_NETWORK = "output/sum_network.csv"
OUTPUT_TOTAL = "output/total.csv"
OUTPUT_CHANNEL = "input/channel.csv"

# ==============================
# 自动检测与转码
# ==============================
def safe_read_csv(path):
    """尝试多种编码读取 CSV 文件，若非 UTF-8 则自动转码保存"""
    encodings_to_try = ["utf-8", "utf-8-sig", "gbk", "big5", "latin-1"]
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(path, encoding=enc)
            if enc not in ["utf-8", "utf-8-sig"]:
                print(f"🔄 检测到 {os.path.basename(path)} 编码为 {enc}，正在转换为 UTF-8...")
                df.to_csv(path, index=False, encoding="utf-8-sig")
                print(f"✅ 已转码并覆盖保存为 UTF-8: {path}")
            return df
        except UnicodeDecodeError:
            continue
        except pd.errors.ParserError:
            continue
    raise ValueError(f"❌ 无法识别文件编码: {path}")

# ==============================
# 加载网络数据库
# ==============================
def load_name_map():
    """加载 iptv-org 数据库频道名及别名映射"""
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"未找到数据库文件: {path}")
    with open(path, encoding="utf-8") as f:
        for row in pd.read_csv(f).to_dict(orient="records"):
            std_name = row.get("name", "").strip()
            if not std_name:
                continue
            name_map[std_name.lower()] = std_name
            others = str(row.get("other_names", ""))
            for alias in others.split(","):
                alias = alias.strip()
                if alias:
                    name_map[alias.lower()] = std_name
    return name_map

# ==============================
# 名称预清理
# ==============================
def clean_channel_name(name):
    """去除括号内容与特殊标记"""
    name = str(name)
    name = re.sub(r"（.*?）|\(.*?\)|\[.*?\]", "", name)
    return name.strip()

# ==============================
# 自有源标准化（不做匹配）
# ==============================
def standardize_my_sum(path):
    print(f"📂 正在读取自有源 (不匹配): {path}")
    df = safe_read_csv(path)
    df.insert(0, "标准频道名", df.iloc[:, 0].astype(str))
    df.insert(1, "匹配状态", ["未匹配-跳过"] * len(df))
    out_path = path.replace(".csv", "_standardized.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成: {out_path}")
    return df

# ==============================
# working.csv 匹配流程
# ==============================
def standardize_working(working_path, my_df, name_map):
    print(f"📂 正在处理网络源匹配: {working_path}")
    df = safe_read_csv(working_path)
    original_names = df.iloc[:, 0].astype(str)

    my_names = my_df["标准频道名"].astype(str).tolist()
    all_network_keys = list(name_map.keys())

    final_names, match_status = [], []

    for name in original_names:
        cleaned_name = clean_channel_name(name)

        # Step 1️⃣ 与自有源匹配
        my_match = process.extractOne(cleaned_name, my_names, score_cutoff=90)
        if my_match:
            std_name = my_match[0]
            score = my_match[1]
            status = f"与自有源匹配({score})"
        else:
            # Step 2️⃣ 与网络数据库匹配
            net_match = process.extractOne(cleaned_name.lower(), all_network_keys)
            if net_match:
                matched_key, score, _ = net_match
                if score >= 95:
                    std_name = name_map[matched_key]
                    status = f"网络匹配({score})"
                else:
                    std_name = name
                    status = f"低置信度({score})"
            else:
                std_name = name
                status = "未匹配"

        final_names.append(std_name)
        match_status.append(status)

    df.insert(0, "标准频道名", final_names)
    df.insert(1, "匹配状态", match_status)

    out_path = working_path.replace(".csv", "_standardized.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成: {out_path}")
    return df

# ==============================
# 主流程
# ==============================
def main():
    print("🚀 开始执行标准化匹配流程...\n")
    print("读取源文件：")
    print(f"  📁 {INPUT_MY}")
    print(f"  📁 {INPUT_WORKING}")
    print(f"  📁 {INPUT_NETWORK}\n")

    # 自动读取并转码
    my_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)
    network_df = safe_read_csv(INPUT_NETWORK)

    print(f"📦 处理自有源 my_sum.csv 共 {len(my_df)} 条")
    my_df = standardize_my_sum(INPUT_MY)

    print(f"🌐 加载 iptv-org 数据库中...")
    name_map = load_name_map()
    print(f"📚 已加载 {len(name_map)} 个频道映射")

    print(f"🌐 处理网络源 working.csv 共 {len(working_df)} 条")
    working_df = standardize_working(INPUT_WORKING, my_df, name_map)

    # 生成总汇总文件
    total_df = pd.concat([my_df, working_df], ignore_index=True)
    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成汇总文件: {OUTPUT_TOTAL}")

    # 提取频道名与分组
    if "标准频道名" in total_df.columns and total_df.shape[1] > 5:
        channel_df = total_df[["标准频道名", total_df.columns[5]]]
        channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")
        print(f"✅ 已提取频道映射: {OUTPUT_CHANNEL}")

    print("\n🎉 全部处理完成！")

# ==============================
# 执行入口
# ==============================
if __name__ == "__main__":
    main()