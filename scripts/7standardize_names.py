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
CHANNEL_DATA_PATH = "input/channel_data.csv"
NETWORK_CHANNELS_PATH = "input/iptv-org/database/data/channels.csv"

OUTPUT_TOTAL_FINAL = "output/total_final.csv"
OUTPUT_CHANNEL_DATA = CHANNEL_DATA_PATH  # 直接覆盖更新

cc = OpenCC('t2s')

def read_csv_auto_encoding(filepath):
    with open(filepath, 'rb') as f:
        raw = f.read(10000)
        result = chardet.detect(raw)
        encoding = result['encoding'] or 'utf-8'
    return pd.read_csv(filepath, encoding=encoding)

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
        pd.DataFrame(columns=["原始名", "标准名", "拟匹配频道名", "分组"]).to_csv(CHANNEL_DATA_PATH, index=False)
    channel_data = read_csv_auto_encoding(CHANNEL_DATA_PATH)
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
    orig_name_dict = dict(zip(channel_data["原始名_std_key"], channel_data["标准名"]))

    existing_orig_names = set(channel_data["原始名"].fillna("").unique())

    matched_standard_names = []
    matched_match_info = []
    matched_match_score = []

    precise_match_count = 0
    fuzzy_match_count = 0

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

            # 精确匹配第一轮
            if key in std_name_dict and std_name_dict[key]:
                matched_name = std_name_dict[key]
                match_info = "精准匹配"
                match_score = 100.0
                precise_match_count += 1
                if original_name not in existing_orig_names:
                    new_row = {
                        "原始名": original_name,
                        "标准名": matched_name,
                        "拟匹配频道名": matched_name,
                        "分组": "未分组"
                    }
                    channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
                    existing_orig_names.add(original_name)
            elif key in orig_name_dict and orig_name_dict[key]:
                matched_name = orig_name_dict[key]
                match_info = "精准匹配"
                match_score = 100.0
                precise_match_count += 1
                if original_name not in existing_orig_names:
                    new_row = {
                        "原始名": original_name,
                        "标准名": matched_name,
                        "拟匹配频道名": matched_name,
                        "分组": "未分组"
                    }
                    channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
                    existing_orig_names.add(original_name)
            else:
                # 第二轮模糊匹配网络库
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
                        if original_name not in existing_orig_names:
                            new_row = {
                                "原始名": original_name,
                                "标准名": matched_name,
                                "拟匹配频道名": matched_name,
                                "分组": "待确认分组"
                            }
                            channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
                            existing_orig_names.add(original_name)
                    else:
                        matched_name = original_name
                        match_info = "未匹配"
                        match_score = float(score)
                        if original_name not in existing_orig_names:
                            new_row = {
                                "原始名": original_name,
                                "标准名": "",
                                "拟匹配频道名": matched_name,
                                "分组": "待标准化"
                            }
                            channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
                            existing_orig_names.add(original_name)
                else:
                    matched_name = original_name
                    match_info = "未匹配"
                    match_score = 0.0
                    if original_name not in existing_orig_names:
                        new_row = {
                            "原始名": original_name,
                            "标准名": "",
                            "拟匹配频道名": matched_name,
                            "分组": "待标准化"
                        }
                        channel_data = pd.concat([channel_data, pd.DataFrame([new_row])], ignore_index=True)
                        existing_orig_names.add(original_name)

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

    std_name_to_group = dict(zip(channel_data["标准名"], channel_data["分组"]))

    def get_group(name):
        return std_name_to_group.get(name, "未分类")

    total_before["分组"] = total_before["频道名"].apply(get_group)

    print(f"匹配完成，总精准匹配数：{precise_match_count}，总模糊匹配数：{fuzzy_match_count}")

    print("保存文件...")
    total_before.to_csv(OUTPUT_TOTAL_FINAL, index=False, encoding="utf-8-sig", columns=[
        "频道名","地址","来源","图标","检测时间","分组","视频编码","分辨率","帧率","音频","相似度","匹配信息","匹配值"
    ])

    # 只保存原始的四列，防止写出多余的_std_key辅助列
    channel_data.to_csv(OUTPUT_CHANNEL_DATA, index=False, encoding="utf-8-sig",
                        columns=["原始名", "标准名", "拟匹配频道名", "分组"])

    print("处理完成！")

if __name__ == "__main__":
    main()