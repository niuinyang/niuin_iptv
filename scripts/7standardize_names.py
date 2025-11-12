#!/usr/bin/env python3
# scripts/merge_and_match.py
import csv
import os
import pandas as pd
from rapidfuzz import process
import re
import chardet

IPTV_DB_PATH = "./iptv-database"

INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
OUTPUT_TOTAL = "output/total.csv"
INPUT_CHANNEL = "input/channel.csv"   # 作为输入的channel.csv
OUTPUT_CHANNEL = "input/channel.csv"  # 覆盖写回
MANUAL_MAP_PATH = "input/manual_map.csv"


# -----------------------------
# 🔹 自动检测 CSV 编码
# -----------------------------
def read_csv_auto_encoding(path):
    with open(path, "rb") as fb:
        raw = fb.read(20000)
        detected_enc = chardet.detect(raw)["encoding"] or "utf-8"
    try:
        df = pd.read_csv(path, encoding=detected_enc)
    except Exception:
        df = pd.read_csv(path, encoding="utf-8-sig")
    return df


# -----------------------------
# 🔹 载入频道数据库
# -----------------------------
def load_channel_database():
    db_channels = []
    for root, _, files in os.walk(IPTV_DB_PATH):
        for f in files:
            if f.endswith(".csv"):
                try:
                    df = read_csv_auto_encoding(os.path.join(root, f))
                    if "name" in df.columns:
                        db_channels.extend(df["name"].dropna().astype(str).tolist())
                except Exception:
                    pass
    return list(set(db_channels))


# -----------------------------
# 🔹 模糊匹配函数
# -----------------------------
def fuzzy_match_channel(name, db_channels, threshold=80):
    if not isinstance(name, str) or not name.strip():
        return None, 0
    result = process.extractOne(name, db_channels, score_cutoff=threshold)
    if result:
        return result[0], result[1]
    return None, 0


# -----------------------------
# 🔹 导出未匹配频道以人工补全（修正版 ✅）
# -----------------------------
def export_unmatched_for_manual(working_df, manual_map_path=MANUAL_MAP_PATH):
    """
    导出未匹配或低匹配频道，用于人工补全标准名称
    输出列：原始名称, 标准名称(空), 拟匹配频道
    """
    # 筛选出未匹配或低匹配的频道
    unmatched_df = working_df[working_df['match_info'].str.contains("未匹配|低匹配", na=False)]

    # 从 match_info 中提取“拟匹配频道”
    def extract_candidate(info):
        m = re.search(r"拟匹配频道:([^,，]*)", str(info))
        return m.group(1).strip() if m else ""

    # 构造导出 DataFrame（只保留三列）
    export_df = pd.DataFrame({
        "原始名称": unmatched_df['original_channel_name'],
        "标准名称": "",
        "拟匹配频道": unmatched_df['match_info'].apply(extract_candidate)
    }).drop_duplicates(subset=["原始名称"], keep="first")

    # 检查 manual_map.csv 是否存在
    if os.path.exists(manual_map_path):
        existing = pd.read_csv(manual_map_path, encoding="utf-8-sig")
        existing_names = existing['原始名称'].str.lower().tolist()
    else:
        os.makedirs(os.path.dirname(manual_map_path), exist_ok=True)
        with open(manual_map_path, "w", encoding="utf-8-sig", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["原始名称", "标准名称", "拟匹配频道"])
        existing_names = []

    # 只写入新的未匹配频道
    new_rows = export_df[~export_df['原始名称'].str.lower().isin(existing_names)]

    if not new_rows.empty:
        new_rows.to_csv(manual_map_path, mode='a', index=False, header=False, encoding="utf-8-sig")
        print(f"🔔 有 {len(new_rows)} 个未匹配或低匹配频道写入到 {manual_map_path}，请手动补全标准名称。")
    else:
        print(f"🔔 无新增未匹配或低匹配频道写入 {manual_map_path}。")


# -----------------------------
# 🔹 主处理逻辑
# -----------------------------
def main():
    print("🚀 载入数据中...")

    df_my = read_csv_auto_encoding(INPUT_MY)
    df_working = read_csv_auto_encoding(INPUT_WORKING)

    db_channels = load_channel_database()
    print(f"📚 频道数据库加载完成，共 {len(db_channels)} 条。")

    matches = []
    for _, row in df_working.iterrows():
        ch_name = str(row.get("name", "")).strip()
        best_match, score = fuzzy_match_channel(ch_name, db_channels)
        if best_match:
            if score >= 90:
                info = f"高匹配: {best_match} ({score:.1f})"
            elif score >= 70:
                info = f"低匹配;拟匹配频道:{best_match}"
            else:
                info = f"未匹配"
        else:
            info = "未匹配"

        matches.append({
            "original_channel_name": ch_name,
            "match_info": info
        })

    df_match = pd.DataFrame(matches)
    df_match.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 匹配结果已保存到 {OUTPUT_TOTAL}")

    # 导出人工匹配文件
    export_unmatched_for_manual(df_match)


if __name__ == "__main__":
    main()