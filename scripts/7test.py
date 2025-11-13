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

def convert_file_to_utf8(path):
    if not os.path.exists(path):
        print(f"⚠️ 文件 {path} 不存在，跳过转换")
        return
    with open(path, 'rb') as f:
        raw = f.read()
    result = chardet.detect(raw)
    enc = result['encoding']
    if enc is None:
        enc = 'utf-8'
    if enc.lower() != 'utf-8-sig':
        try:
            text = raw.decode(enc, errors='ignore')
            with open(path, 'w', encoding='utf-8-sig') as f:
                f.write(text)
            print(f"✅ 文件 {path} 从 {enc} 转码为 UTF-8-SIG")
        except Exception as e:
            print(f"❌ 转码文件 {path} 出错: {e}")
    else:
        print(f"✅ 文件 {path} 已经是 UTF-8-SIG，无需转换")

def convert_all_csv_to_utf8(paths):
    for p in paths:
        convert_file_to_utf8(p)

def read_csv_auto_encoding(path, dtype=None):
    """
    先转为 utf-8-sig 编码文件，再用 utf-8-sig 读取。
    dtype 可选传入。
    """
    convert_file_to_utf8(path)
    return pd.read_csv(path, encoding='utf-8-sig', dtype=dtype)

def safe_read_csv(path):
    return read_csv_auto_encoding(path)

def load_name_map():
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    with open(path, encoding="utf-8") as f:
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

    df = read_csv_auto_encoding(path, dtype=str)
    for _, row in df.iterrows():
        raw_name = row.get("原始名称")
        std_name = row.get("标准名称")

        if pd.isna(raw_name) or pd.isna(std_name):
            continue

        raw_name_str = str(raw_name).strip()
        std_name_str = str(std_name).strip().title()

        if raw_name_str and std_name_str:
            manual_map[raw_name_str.lower()] = std_name_str
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

def export_unmatched_for_manual(working_df, manual_map_path=MANUAL_MAP_PATH):
    unmatched_mask = working_df['match_info'].fillna("").str.contains("未匹配|低匹配", na=False)
    unmatched_df = working_df[unmatched_mask].copy()

    def extract_candidate(info):
        if not isinstance(info, str):
            return ""
        m = re.search(r"拟匹配频道:([^\s,，]+)", info)
        if m:
            return m.group(1).strip()
        return ""

    export_df = pd.DataFrame({
        "原始名称": unmatched_df['original_channel_name'].astype(str).str.strip(),
        "标准名称": "",
        "拟匹配频道": unmatched_df['match_info'].apply(extract_candidate).astype(str).str.strip()
    }).drop_duplicates(subset=["原始名称"], keep="first")

    if export_df.empty:
        if not os.path.exists(manual_map_path):
            os.makedirs(os.path.dirname(manual_map_path), exist_ok=True)
            pd.DataFrame(columns=["原始名称", "标准名称", "拟匹配频道"]).to_csv(manual_map_path, index=False, encoding="utf-8-sig")
        print(f"🔔 无新增未匹配或低匹配频道，已确保 {manual_map_path} 存在。")
        return

    if os.path.exists(manual_map_path):
        existing = pd.read_csv(manual_map_path, encoding="utf-8-sig", dtype=str)
    else:
        existing = pd.DataFrame(columns=["原始名称", "标准名称", "拟匹配频道"])

    for col in ["原始名称", "标准名称", "拟匹配频道"]:
        if col not in existing.columns:
            existing[col] = ""

    existing = existing[["原始名称", "标准名称", "拟匹配频道"]].astype(str)

    combined = pd.concat([existing, export_df], ignore_index=True)
    combined.drop_duplicates(subset=["原始名称"], keep="first", inplace=True)

    os.makedirs(os.path.dirname(manual_map_path), exist_ok=True)
    combined.to_csv(manual_map_path, index=False, encoding="utf-8-sig")
    print(f"🔔 已更新 {manual_map_path}，共 {len(combined)} 条记录。")

def sort_by_name_pinyin(df, col_name):
    df['_sort_key'] = df[col_name].apply(lambda x: ''.join(lazy_pinyin(str(x).lower())))
    df = df.sort_values(by='_sort_key').drop(columns=['_sort_key'])
    return df.reset_index(drop=True)

def sort_channel_file(path=OUTPUT_CHANNEL):
    if not os.path.exists(path):
        print(f"⚠️ 文件 {path} 不存在，无法排序")
        return

    df = pd.read_csv(path, encoding="utf-8-sig")
    df['分组'] = df['分组'].fillna("").replace("", "未分类")

    group_order = [
        "央视频道",
        "4K频道",
        "卫视频道",
        "山东频道",
        "他省频道",
        "数字频道",
        "电台广播",
        "国际频道"
    ]

    def group_rank(g):
        if g == "未分类":
            return 9999
        try:
            return group_order.index(g)
        except ValueError:
            return 9998

    df['分组排序权重'] = df['分组'].apply(group_rank)
    df = df.sort_values(by=['分组排序权重']).reset_index(drop=True)

    result_frames = []
    for g, group_df in df.groupby('分组', sort=False):
        sorted_group = sort_by_name_pinyin(group_df, '频道名')
        result_frames.append(sorted_group)

    df_sorted = pd.concat(result_frames, ignore_index=True)
    df_sorted = df_sorted.drop(columns=['分组排序权重'])

    df_sorted.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ 已对 {path} 进行分组及频道名拼音排序")

def sort_manual_map_file(path=MANUAL_MAP_PATH):
    if not os.path.exists(path):
        print(f"⚠️ 文件 {path} 不存在，无法排序")
        return

    df = pd.read_csv(path, encoding="utf-8-sig")
    df['标准名称是否空'] = df['标准名称'].fillna("").apply(lambda x: 1 if x.strip() == "" else 0)
    df = df.sort_values(by=['标准名称是否空']).reset_index(drop=True)

    df_has_std = df[df['标准名称是否空'] == 0].copy()
    df_no_std = df[df['标准名称是否空'] == 1].copy()

    df_has_std = sort_by_name_pinyin(df_has_std, '原始名称')
    df_no_std = sort_by_name_pinyin(df_no_std, '原始名称')

    df_sorted = pd.concat([df_has_std, df_no_std], ignore_index=True)
    df_sorted = df_sorted.drop(columns=['标准名称是否空'])

    df_sorted.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ 已对 {path} 进行标准名称优先及原始名称拼音排序")

def build_total_df(df):
    def safe_col(name_list):
        for name in name_list:
            if name in df.columns:
                return df[name]
        return pd.Series([""] * len(df))

    # 这里新增视频编码、分辨率、帧率、音频、相似度列
    return pd.DataFrame({
        "频道名": df.get("final_name", df.iloc[:, 0]),
        "地址": safe_col(["地址"]),
        "来源": safe_col(["来源"]),
        "检测时间": safe_col(["检测时间", "检测时间(延迟)"]),
        "图标": safe_col(["图标"]),
        "分组": safe_col(["分组"]),
        "匹配信息": safe_col(["match_info"]),
        "原始频道名": safe_col(["original_channel_name"]),
        "视频编码": safe_col(["视频编码"]),
        "分辨率": safe_col(["分辨率"]),
        "帧率": safe_col(["帧率"]),
        "音频": safe_col(["音频"]),
        "相似度": safe_col(["相似度"]),
    })

def save_standardized_my_sum(df):
    def safe_col(name_list):
        for name in name_list:
            if name in df.columns:
                return df[name]
        return pd.Series([""] * len(df))

    out_df = pd.DataFrame({
        "频道名": df.get("final_name", df.iloc[:, 0]),
        "地址": safe_col(["地址"]),
        "来源": safe_col(["来源"]),
        "检测时间": safe_col(["检测时间", "检测时间(延迟)"]),
        "图标": safe_col(["图标"]),
        "分组": safe_col(["分组"]),
        "匹配信息": safe_col(["match_info"]),
        "原始频道名": safe_col(["original_channel_name"]),
        "视频编码": safe_col(["视频编码"]),
        "分辨率": safe_col(["分辨率"]),
        "帧率": safe_col(["帧率"]),
        "音频": safe_col(["音频"]),
        "相似度": safe_col(["相似度"]),
    })
    out_df.to_csv("input/mysource/my_sum_standardized.csv", index=False, encoding="utf-8-sig")
    print("✅ 已保存文件：input/mysource/my_sum_standardized.csv")

def main():
    print("🚀 开始执行标准化匹配流程...")

    csv_files = [INPUT_MY, INPUT_WORKING, INPUT_CHANNEL, MANUAL_MAP_PATH]
    convert_all_csv_to_utf8(csv_files)

    my_sum_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)

    print(f"读取源文件：\n  📁 {INPUT_MY}\n  📁 {INPUT_WORKING}")

    name_map = load_name_map()
    manual_map = load_manual_map()
    print(f"✅ 数据库加载完成，映射总数：{len(name_map)}，人工映射条数：{len(manual_map)}")

    my_sum_df = standardize_my_sum(my_sum_df)
    save_standardized_my_sum(my_sum_df)

    working_df = standardize_working(working_df, my_sum_df, name_map, manual_map)

    export_unmatched_for_manual(working_df)

    my_sum_out = build_total_df(my_sum_df)
    working_out = build_total_df(working_df)

    total_df = pd.concat([my_sum_out, working_out], ignore_index=True)

    # ===== 新增：根据频道名从 channel.csv 更新 total_df 的“分组”列 =====
    if os.path.exists(INPUT_CHANNEL):
        channel_df = pd.read_csv(INPUT_CHANNEL, encoding="utf-8-sig")
        channel_group_map = dict(zip(channel_df["频道名"].str.lower(), channel_df["分组"].fillna("").astype(str)))

        def update_group(row):
            name = row["频道名"]
            if not isinstance(name, str):
                return row["分组"]
            lower_name = name.lower()
            if lower_name in channel_group_map and channel_group_map[lower_name].strip() != "":
                return channel_group_map[lower_name]
            else:
                return row["分组"]

        total_df["分组"] = total_df.apply(update_group, axis=1)
    # ===============================================================

    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已保存文件：{OUTPUT_TOTAL}，共计 {len(total_df)} 条记录")

    if os.path.exists(INPUT_CHANNEL):
        existing_channel_df = pd.read_csv(INPUT_CHANNEL, encoding="utf-8-sig")
    else:
        existing_channel_df = pd.DataFrame(columns=["频道名", "分组"])

    manual_map_lower = {k.lower(): v for k, v in manual_map.items()}
    def replace_name(row):
        old_name_lower = row["频道名"].lower()
        if old_name_lower in manual_map_lower:
            return manual_map_lower[old_name_lower]
        return row["频道名"]

    existing_channel_df["频道名"] = existing_channel_df.apply(replace_name, axis=1)
    existing_channel_df.drop_duplicates(subset=["频道名"], keep="first", inplace=True)

    total_channels = total_df[["频道名", "分组"]]
    existing_names = set(existing_channel_df["频道名"])

    new_channels_df = total_channels[~total_channels["频道名"].isin(existing_names)].copy()
    new_channels_df["分组"] = "未分类"

    combined_channel_df = pd.concat([existing_channel_df, new_channels_df], ignore_index=True)
    combined_channel_df.drop_duplicates(subset=["频道名"], keep="first", inplace=True)

    combined_channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")

    added = len(new_channels_df)
    modified = len(existing_channel_df)
    print(f"✅ 更新 channel.csv 完成，新增频道数：{added}，现有频道数（去重后）：{modified}")

    sort_channel_file(OUTPUT_CHANNEL)
    sort_manual_map_file(MANUAL_MAP_PATH)
    print("✅ manual_map.csv 排序完成")

if __name__ == "__main__":
    main()