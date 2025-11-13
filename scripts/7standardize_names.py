#!/usr/bin/env python3
# standardize_iptv.py

import os
import re
import csv
import sys
import time
import chardet
import pandas as pd
from opencc import OpenCC
from rapidfuzz import fuzz, process
from tqdm import tqdm

# 配置路径，按需调整
MY_SUM_PATH = "input/mysource/my_sum.csv"
WORKING_PATH = "output/working.csv"
CHANNEL_DATA_PATH = "input/channel_data.xlsx"  # 改为Excel文件
NETWORK_CHANNELS_PATH = "input/iptv-org/database/data/channels.csv"

OUTPUT_TOTAL_FINAL = "output/total_final.csv"
OUTPUT_CHANNEL_DATA = CHANNEL_DATA_PATH  # 输出Excel文件路径

cc = OpenCC('t2s')

def read_csv_auto_encoding(filepath):
    with open(filepath, 'rb') as f:
        raw = f.read(10000)
        result = chardet.detect(raw)
        encoding = result['encoding'] or 'utf-8'
    return pd.read_csv(filepath, encoding=encoding)

def read_channel_data_excel(filepath):
    return pd.read_excel(filepath)

def mechanical_standardize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    s = cc.convert(s)
    s = s.lower()
    s = re.sub(r"\（.*?\）", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"\【.*?\】", "", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fa5\+\！]", "", s)
    return s

def clean_network_std_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = ' '.join([w.capitalize() if re.match(r'[a-zA-Z]+$', w) else w for w in name.split(" ")])
    return name

def main():
    print("开始读取文件...")
    my_sum = read_csv_auto_encoding(MY_SUM_PATH)
    working = read_csv_auto_encoding(WORKING_PATH)
    if not os.path.exists(CHANNEL_DATA_PATH):
        # Excel空表创建
        pd.DataFrame(columns=["原始名", "标准名", "拟匹配频道名", "分组"]).to_excel(CHANNEL_DATA_PATH, index=False)
    channel_data = read_channel_data_excel(CHANNEL_DATA_PATH)
    network_channels_df = read_csv_auto_encoding(NETWORK_CHANNELS_PATH)
    if "channel" in network_channels_df.columns:
        network_col = "channel"
    elif "name" in network_channels_df.columns:
        network_col = "name"
    else:
        print("网络数据库无频道名列，检查文件！")
        sys.exit(1)
    network_channels_df = network_channels_df.dropna(subset=[network_col])
    network_channels_df["std_key"] = network_channels_df[network_col].apply(mechanical_standardize)
    network_channels = dict(zip(network_channels_df["std_key"], network_channels_df[network_col]))

    for col in ["视频编码", "分辨率", "帧率", "音频", "相似度"]:
        if col not in my_sum.columns:
            my_sum[col] = ""
        if col not in working.columns:
            working[col] = ""

    total_before = pd.concat([my_sum, working], ignore_index=True, sort=False)
    for col in ["频道名","地址","来源","图标","检测时间","分组","视频编码","分辨率","帧率","音频","相似度"]:
        if col not in total_before.columns:
            total_before[col] = ""

    total_before["std_key"] = total_before["频道名"].apply(mechanical_standardize)

    channel_data["标准名_std_key"] = channel_data["标准名"].apply(mechanical_standardize)
    channel_data["原始名_std_key"] = channel_data["原始名"].apply(mechanical_standardize)

    std_name_dict = dict(zip(channel_data["标准名_std_key"], channel_data["标准名"]))

    # 新增：拟匹配频道名字典，用于判断精准匹配条件（标准名对应的）
    std_key_to_pending = dict(zip(channel_data["标准名_std_key"], channel_data["拟匹配频道名"]))

    # 新增：原始名对应的标准名字典
    orig_name_dict = dict(zip(channel_data["原始名_std_key"], channel_data["标准名"]))
    orig_key_to_pending = dict(zip(channel_data["原始名_std_key"], channel_data["拟匹配频道名"]))

    existing_orig_names = set(channel_data["原始名"].fillna("").unique())

    matched_standard_names = []
    matched_match_info = []
    matched_match_score = []

    precise_match_count = 0
    fuzzy_match_count = 0

    def add_channel_data_if_not_exists(orig_name, std_name, group_label):
        nonlocal channel_data, existing_orig_names
        if orig_name not in existing_orig_names:
            new_row = {
                "原始名": orig_name,
                "标准名": std_name,
                "拟匹配频道名": std_name,
                "分组": group_label
            }
            channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
            existing_orig_names.add(orig_name)

    # ======== 修改开始 ========
    def is_pending_empty(val):
        return pd.isna(val) or str(val).strip() == ""
    # ======== 修改结束 ========

    print("开始匹配标准化频道名，进度实时显示...")

    total_len = len(total_before)
    batch_size = 50  # 每批处理数量
    last_print_time = time.time()

    for start_idx in tqdm(range(0, total_len, batch_size), desc="匹配进度"):
        end_idx = min(start_idx + batch_size, total_len)
        batch = total_before.iloc[start_idx:end_idx]

        for idx, row in batch.iterrows():
            original_name = row["频道名"]
            key = row["std_key"]
            matched_name = None
            match_info = "未匹配"
            match_score = 0.0

            # ======== 修改开始 ========
            # 1. 先用 std_key 去匹配标准名
            if key in std_name_dict:
                pending_val = std_key_to_pending.get(key, "")
                if is_pending_empty(pending_val):
                    matched_name = std_name_dict[key]
                    match_info = "精准匹配-标准名"
                    match_score = 100.0
                    precise_match_count += 1
                else:
                    matched_name = None
            else:
                matched_name = None

            # 2. 如果标准名匹配失败，再用 std_key 去匹配原始名
            if matched_name is None:
                if key in orig_name_dict:
                    pending_val = orig_key_to_pending.get(key, "")
                    if is_pending_empty(pending_val):
                        matched_name = orig_name_dict[key]
                        match_info = "精准匹配-原始名"
                        match_score = 100.0
                        precise_match_count += 1
                    else:
                        matched_name = None
            # ======== 修改结束 ========

            if matched_name is None:
                choices = list(network_channels.keys())
                matches = process.extract(key, choices, scorer=fuzz.ratio, limit=1)
                if matches:
                    best_match_key, score, _ = matches[0]
                    if score > 90:
                        matched_name = network_channels[best_match_key]
                        match_info = f"模糊匹配（>90%）"
                        match_score = float(score)
                        fuzzy_match_count += 1
                        matched_name = clean_network_std_name(matched_name)
                        add_channel_data_if_not_exists(original_name, matched_name, "待确认分组")
                    else:
                        matched_name = original_name
                        match_info = "未匹配"
                        match_score = float(score)
                        add_channel_data_if_not_exists(original_name, matched_name, "待标准化")
                else:
                    matched_name = original_name
                    match_info = "未匹配"
                    match_score = 0.0
                    add_channel_data_if_not_exists(original_name, matched_name, "待标准化")

            matched_standard_names.append(matched_name)
            matched_match_info.append(match_info)
            matched_match_score.append(match_score)

        now = time.time()
        if now - last_print_time >= 5:
            print(f"已处理 {end_idx} / {total_len} 条，精准匹配数：{precise_match_count}，模糊匹配数：{fuzzy_match_count}")
            last_print_time = now

    total_before["频道名"] = matched_standard_names
    total_before["匹配信息"] = matched_match_info
    total_before["匹配值"] = matched_match_score

    # 关键修改：用 total_before["频道名"] 去匹配 channel_data["标准名"] 赋分组
    std_name_to_group = dict(zip(channel_data["标准名"], channel_data["分组"]))

    def get_group(name):
        return std_name_to_group.get(name, "未分类")

    total_before["分组"] = total_before["频道名"].apply(get_group)

    # ====== 新增：对 channel_data 按 “原始名” 去重，保留首次出现，防止覆盖 ======
    channel_data = channel_data.drop_duplicates(subset=["原始名"], keep='first')

    print(f"匹配完成，总精准匹配数：{precise_match_count}，总模糊匹配数：{fuzzy_match_count}")

    print("保存文件...")
    total_before.to_csv(OUTPUT_TOTAL_FINAL, index=False, encoding="utf-8-sig", columns=[
        "频道名","地址","来源","图标","检测时间","分组","视频编码","分辨率","帧率","音频","相似度","匹配信息","匹配值"
    ])

    # 保存 Excel 文件
    channel_data.to_excel(OUTPUT_CHANNEL_DATA, index=False,
                          columns=["原始名", "标准名", "拟匹配频道名", "分组"])

    # 额外保存一份同名 CSV，utf-8-sig 编码
    csv_path = OUTPUT_CHANNEL_DATA.rsplit('.', 1)[0] + ".csv"
    channel_data.to_csv(csv_path, index=False, encoding="utf-8-sig",
                        columns=["原始名", "标准名", "拟匹配频道名", "分组"])

    print("处理完成！")

if __name__ == "__main__":
    main()
