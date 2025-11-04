#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
standardize_channels.py
使用 iptv-org/database 自动标准化频道名，自动适配文件编码，生成总表和频道分组映射。
"""

import os, csv, pandas as pd, requests
from rapidfuzz import process

IPTV_DB_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/channels.csv"
IPTV_DB_FILE = "channels.csv"
OUTPUT_DIR = "output"
INPUT_CHANNEL_CSV = "input/channel.csv"
THRESHOLD = 85

def update_database():
    print("🔽 正在下载最新 channels.csv ...")
    try:
        r = requests.get(IPTV_DB_URL, timeout=30)
        r.raise_for_status()
        with open(IPTV_DB_FILE, "wb") as f:
            f.write(r.content)
        print("✅ 数据库下载完成")
    except Exception as e:
        print(f"⚠️ 下载失败: {e}")
        if not os.path.exists(IPTV_DB_FILE):
            raise SystemExit("❌ 没有可用的频道数据库")

def load_name_map():
    name_map = {}
    with open(IPTV_DB_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            std = row["name"].strip()
            name_map[std.lower()] = std
            aliases = row.get("aliases", "") or row.get("other_names", "")
            for alias in aliases.replace("|", ",").split(","):
                alias = alias.strip()
                if alias:
                    name_map[alias.lower()] = std
    print(f"📚 已加载 {len(name_map)} 个名称映射")
    return name_map

def read_csv_auto(path, encodings=None):
    """
    尝试多种编码读取 CSV 文件，返回 DataFrame。
    默认尝试 ['utf-8-sig', 'utf-8', 'gbk', 'latin1']。
    """
    if encodings is None:
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'latin1']

    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"✅ 使用编码 {enc} 读取文件成功: {path}")
            return df
        except UnicodeDecodeError:
            print(f"⚠️ 编码 {enc} 读取失败，尝试下一个...")
    # 全失败才抛异常
    raise UnicodeDecodeError(f"无法用备选编码读取文件: {path}")

def match_name(name, name_map):
    n = name.strip()
    if not n:
        return n, "空名"
    key = n.lower()
    if key in name_map:
        return name_map[key], "精确匹配"
    match, score, _ = process.extractOne(key, list(name_map.keys()))
    if score >= THRESHOLD:
        return name_map[match], f"模糊匹配({score:.0f})"
    return n, "未匹配"

def standardize_csv(path, name_map):
    print(f"📂 正在处理: {path}")
    df = read_csv_auto(path)
    unmatched = set()
    std_names, statuses = [], []

    for name in df.iloc[:, 0].astype(str):
        std, status = match_name(name, name_map)
        std_names.append(std)
        statuses.append(status)
        if status == "未匹配":
            unmatched.add(name)

    df.insert(0, "标准频道名", std_names)
    df.insert(1, "匹配状态", statuses)

    out_path = path.replace(".csv", "_standardized.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成: {out_path}")

    return df, unmatched

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    update_database()
    name_map = load_name_map()

    input_files = [
        "input/mysource/my_sum.csv",
        "output/working.csv"
    ]

    all_unmatched = set()
    dfs = []

    for f in input_files:
        if os.path.exists(f):
            df, unmatched = standardize_csv(f, name_map)
            all_unmatched |= unmatched
            dfs.append(df)
        else:
            print(f"⚠️ 文件不存在: {f}")

    if dfs:
        total_df = pd.concat(dfs, ignore_index=True)
        total_csv_path = os.path.join(OUTPUT_DIR, "total.csv")
        total_df.to_csv(total_csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ 已生成总表: {total_csv_path}")

        if "分组" in total_df.columns:
            channel_df = total_df[["标准频道名", "分组"]].drop_duplicates()
            os.makedirs(os.path.dirname(INPUT_CHANNEL_CSV), exist_ok=True)
            channel_df.to_csv(INPUT_CHANNEL_CSV, index=False, encoding="utf-8-sig")
            print(f"✅ 已生成频道分组映射: {INPUT_CHANNEL_CSV}")
        else:
            print("⚠️ 总表中未找到“分组”列，无法生成频道分组映射文件")

    if all_unmatched:
        report_path = os.path.join(OUTPUT_DIR, "unmatched_channels.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            for ch in sorted(all_unmatched):
                f.write(ch + "\n")
        print(f"⚠️ 未匹配频道 {len(all_unmatched)} 个，已保存至 {report_path}")
    else:
        print("🎉 所有频道均已匹配")

if __name__ == "__main__":
    main()