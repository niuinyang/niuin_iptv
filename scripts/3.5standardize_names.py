#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import pandas as pd
import requests
from rapidfuzz import process

IPTV_DB_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/channels.csv"
IPTV_DB_FILE = "channels.csv"
OUTPUT_DIR = "output"
INPUT_CHANNEL_CSV = "input/channel.csv"
THRESHOLD = 95  # 匹配阈值

match_cache = {}

def update_database():
    if os.path.exists(IPTV_DB_FILE):
        print(f"✅ 数据库文件 {IPTV_DB_FILE} 已存在，跳过下载")
        return
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
    """加载网络库频道名及别名映射"""
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
    if encodings is None:
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'latin1']
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"✅ 使用编码 {enc} 读取文件成功: {path}")
            return df
        except UnicodeDecodeError:
            print(f"⚠️ 编码 {enc} 读取失败，尝试下一个...")
    raise UnicodeDecodeError(f"无法用备选编码读取文件: {path}")

def build_local_name_map(df_my_sum):
    """从my_sum.csv构建本地频道名映射，key为小写，value为原名"""
    local_map = {}
    for name in df_my_sum.iloc[:, 0].astype(str):
        local_map[name.lower()] = name
    return local_map

def match_name_working(name, local_map, network_map):
    """
    对working.csv频道名匹配：
    1. 先和本地my_sum名模糊匹配
    2. 如果未达阈值，再和网络库模糊匹配
    """
    n = name.strip()
    if not n:
        return n, "空名"
    if n in match_cache:
        return match_cache[n]

    key = n.lower()

    # 1. 本地库模糊匹配
    local_candidates = list(local_map.keys())
    match_local, score_local, _ = process.extractOne(key, local_candidates)
    if score_local >= THRESHOLD:
        res = (local_map[match_local], f"本地库模糊匹配({score_local:.0f})")
        match_cache[n] = res
        return res

    # 2. 网络库匹配
    if key in network_map:
        res = (network_map[key], "网络库精确匹配")
        match_cache[n] = res
        return res

    network_candidates = list(network_map.keys())
    match_net, score_net, _ = process.extractOne(key, network_candidates)
    if score_net >= THRESHOLD:
        res = (network_map[match_net], f"网络库模糊匹配({score_net:.0f})")
    else:
        res = (n, f"匹配度低({max(score_local, score_net):.0f})，保留原名")

    match_cache[n] = res
    return res

def standardize_my_sum(path):
    """my_sum.csv不匹配，直接输出原名作为标准名"""
    print(f"📂 正在读取 my_sum.csv (不匹配): {path}")
    df = read_csv_auto(path)
    df.insert(0, "标准频道名", df.iloc[:, 0].astype(str))
    df.insert(1, "匹配状态", ["未匹配-跳过"] * len(df))
    out_path = path.replace(".csv", "_standardized.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成: {out_path}")
    return df

def standardize_working(path, local_map, network_map):
    """working.csv匹配处理，先本地后网络"""
    print(f"📂 正在处理 working.csv (匹配): {path}")
    df = read_csv_auto(path)
    unmatched = set()
    std_names, statuses = [], []

    for name in df.iloc[:, 0].astype(str):
        std, status = match_name_working(name, local_map, network_map)
        std_names.append(std)
        statuses.append(status)
        if status.startswith("匹配度低"):
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
    network_map = load_name_map()

    my_sum_path = "input/mysource/my_sum.csv"
    working_path = "output/working.csv"

    if not os.path.exists(my_sum_path):
        print(f"⚠️ 文件不存在: {my_sum_path}")
        return
    if not os.path.exists(working_path):
        print(f"⚠️ 文件不存在: {working_path}")
        return

    # 1. 读取并“标准化”my_sum.csv（不匹配）
    df_my_sum = standardize_my_sum(my_sum_path)
    # 2. 构建本地名称映射，用于匹配working.csv
    local_map = build_local_name_map(df_my_sum)
    # 3. 标准化working.csv，先匹配本地再匹配网络库
    df_working, unmatched_working = standardize_working(working_path, local_map, network_map)

    # 4. 合并两个结果生成总表
    total_df = pd.concat([df_my_sum, df_working], ignore_index=True)
    total_csv_path = os.path.join(OUTPUT_DIR, "total.csv")
    total_df.to_csv(total_csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成总表: {total_csv_path}")

    # 5. 生成频道名-分组映射表
    if "分组" in total_df.columns:
        channel_df = total_df[["标准频道名", "分组"]].drop_duplicates()
        os.makedirs(os.path.dirname(INPUT_CHANNEL_CSV), exist_ok=True)
        channel_df.to_csv(INPUT_CHANNEL_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ 已生成频道分组映射: {INPUT_CHANNEL_CSV}")
    else:
        print("⚠️ 总表中未找到“分组”列，无法生成频道分组映射文件")

    # 6. unmatched频道输出
    if unmatched_working:
        report_path = os.path.join(OUTPUT_DIR, "unmatched_channels.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            for ch in sorted(unmatched_working):
                f.write(ch + "\n")
        print(f"⚠️ working.csv 中匹配度低的频道 {len(unmatched_working)} 个，已保存至 {report_path}")
    else:
        print("🎉 working.csv 中所有频道匹配度达标")

if __name__ == "__main__":
    main()