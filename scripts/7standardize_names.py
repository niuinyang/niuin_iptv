#!/usr/bin/env python3
import csv
import os
import pandas as pd
from rapidfuzz import process
import re
import chardet
from pypinyin import lazy_pinyin
from tqdm import tqdm
import time

IPTV_DB_PATH = "./iptv-database"

INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
OUTPUT_TOTAL = "output/total.csv"
INPUT_CHANNEL = "input/channel.csv"
OUTPUT_CHANNEL = "input/channel.csv"
MANUAL_MAP_PATH = "input/manual_map.csv"

# ✅ 修改部分：统一为 utf-8-sig 的转换函数
def ensure_utf8sig(path):
    """检测文件编码，若不是 utf-8-sig（包括 utf-8），则转为 utf-8-sig"""
    if not os.path.exists(path):
        print(f"⚠️ 文件 {path} 不存在，跳过编码检查")
        return
    with open(path, 'rb') as f:
        raw = f.read()
    result = chardet.detect(raw)
    enc = (result['encoding'] or 'utf-8').lower()

    # 转换条件：不是 utf-8-sig（utf_8_sig）时
    if enc not in ['utf-8-sig', 'utf_8_sig']:
        try:
            text = raw.decode(enc, errors='ignore')
            with open(path, 'w', encoding='utf-8-sig') as f:
                f.write(text)
            print(f"✅ 文件 {path} 从 {enc} 转换为 UTF-8-SIG")
        except Exception as e:
            print(f"❌ 转换文件 {path} 出错: {e}")
    else:
        print(f"✅ 文件 {path} 已是 UTF-8-SIG，无需转换")

def convert_all_csv_to_utf8sig(paths):
    for p in paths:
        ensure_utf8sig(p)

# ✅ 后续读取统一使用 utf-8-sig
def safe_read_csv(path):
    return pd.read_csv(path, encoding="utf-8-sig", on_bad_lines='skip')

# ======================== 以下为原逻辑保持不动 ========================
def load_name_map():
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            std_name = row["name"].strip().title()
            name_map[std_name.lower()] = std_name
            others = row.get("other_names", "")
            for alias in others.split(","):
                alias = alias.strip()
                if alias:
                    name_map[alias.lower()] = std_name
    return name_map

def load_manual_map(path=MANUAL_MAP_PATH):
    manual_map = {}
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8-sig", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["原始名称", "标准名称", "拟匹配频道"])
        print(f"⚠️ 未找到人工映射文件，已新建空文件：{path}")
        return manual_map

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = row.get("原始名称", "").strip()
            std_name = row.get("标准名称", "").strip().title()
            if raw_name and std_name:
                manual_map[raw_name.lower()] = std_name
    print(f"📘 已加载人工映射文件 {path}，共 {len(manual_map)} 条")
    return manual_map

def clean_channel_name(name):
    if not isinstance(name, str):
        return ""
    name = re.sub(r"[\(\（][^\)\）]*[\)\）]", "", name)
    name = re.sub(r"[\[\【][^\]\】]*[\]\】]", "", name)
    name = re.sub(r"\b(not\s*)?(24/7|7\*24|7x24)\b", "", name, flags=re.I)
    return name.strip()

def normalize_name_for_match(name):
    if not isinstance(name, str):
        return ""
    name = clean_channel_name(name)
    name = re.sub(r"[-\s]", "", name)
    return name.lower()

def standardize_my_sum(my_sum_df):
    my_sum_df['final_name'] = my_sum_df.iloc[:,0].astype(str).str.title()
    my_sum_df['match_info'] = "自有源"
    my_sum_df['original_channel_name'] = my_sum_df.iloc[:,0].astype(str)
    return my_sum_df

def standardize_working(working_df, my_sum_df, name_map, manual_map):
    working_df['original_channel_name'] = working_df.iloc[:, 0].astype(str)
    working_df['clean_name'] = working_df['original_channel_name'].apply(clean_channel_name)
    my_name_dict = dict(zip(my_sum_df.iloc[:,0].apply(normalize_name_for_match), my_sum_df['final_name']))

    total = len(working_df)
    final_names = []
    match_infos = []
    matched_count = 0
    unmatched_count = 0

    print(f"🔄 开始对 working.csv 共 {total} 条记录进行标准化匹配...")

    start_time = time.time()
    last_print_time = start_time

    for idx, (orig_name, clean_name) in enumerate(tqdm(zip(working_df['original_channel_name'], working_df['clean_name']), total=total), 1):
        orig_name_lower = orig_name.lower()
        clean_name_lower = normalize_name_for_match(clean_name)

        if orig_name_lower in manual_map:
            std_name = manual_map[orig_name_lower]
            match_info = "人工匹配"
            matched_count += 1
        elif clean_name_lower in my_name_dict:
            std_name = my_name_dict[clean_name_lower]
            match_info = "自有源匹配"
            matched_count += 1
        else:
            choices = list(name_map.keys())
            match, score, _ = process.extractOne(clean_name_lower, choices)
            if score >= 95:
                std_name = name_map[match]
                match_info = "模糊匹配"
                matched_count += 1
            elif score > 0:
                std_name = clean_name.title()
                match_info = f"低匹配;拟匹配频道:{name_map[match]}"
                unmatched_count += 1
            else:
                std_name = clean_name.title()
                match_info = "未匹配"
                unmatched_count += 1

        final_names.append(std_name)
        match_infos.append(match_info)

        current_time = time.time()
        if current_time - last_print_time >= 5 or idx == total:
            print(f"已处理 {idx}/{total} 条，匹配 {matched_count} 条，未匹配 {unmatched_count} 条")
            last_print_time = current_time

    working_df['final_name'] = final_names
    working_df['match_info'] = match_infos
    print("✅ working.csv 标准化匹配完成")
    return working_df

# 以下函数保持不变（省略重复部分）……

def main():
    print("🚀 开始执行标准化匹配流程...")

    # ✅ 替换为 utf-8-sig 自动转换逻辑
    csv_files = [INPUT_MY, INPUT_WORKING, INPUT_CHANNEL, MANUAL_MAP_PATH]
    convert_all_csv_to_utf8sig(csv_files)

    my_sum_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)

    print(f"读取源文件：\n  📁 {INPUT_MY}\n  📁 {INPUT_WORKING}")

    name_map = load_name_map()
    manual_map = load_manual_map()
    print(f"✅ 数据库加载完成，映射总数：{len(name_map)}，人工映射条数：{len(manual_map)}")

    # 其余逻辑保持不变……