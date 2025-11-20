#!/usr/bin/env python3
# standardize_iptv.py (CSV 版本，无 Excel)

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

# =============================
# 配置路径，已经全部统一为 CSV
# =============================
MY_SUM_PATH = "output/middle/merge/mysource_total.csv"
WORKING_PATH = "output/middle/working.csv"

CHANNEL_DATA_PATH = "input/channel_data.csv"        # CSV 输出路径
NETWORK_CHANNELS_PATH = "input/iptv-org/database/data/channels.csv"

OUTPUT_TOTAL_FINAL = "output/total_final.csv"
OUTPUT_CHANNEL_DATA = CHANNEL_DATA_PATH

cc = OpenCC('t2s')


# =============================
# 自动编码识别读 CSV
# =============================
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
    name = ' '.join([
        w.capitalize() if re.match(r'[a-zA-Z]+$', w) else w
        for w in name.split(" ")
    ])
    return name


# =============================
#  主程序
# =============================
def main():
    print("开始读取文件...")

    # 读取 my_sum, working
    my_sum = read_csv_auto_encoding(MY_SUM_PATH)
    working = read_csv_auto_encoding(WORKING_PATH)

    # =============================
    #  CSV：如果不存在 channel_data，则创建
    # =============================
    if not os.path.exists(CHANNEL_DATA_PATH):
        pd.DataFrame(columns=["原始名", "标准名", "拟匹配频道名", "分组"]).to_csv(
            CHANNEL_DATA_PATH, index=False, encoding="utf-8-sig"
        )

    channel_data = read_csv_auto_encoding(CHANNEL_DATA_PATH)

    # 新增默认列：来源、输出顺序、是否已维护
    # 来源列填充逻辑：从 my_sum 和 working 取对应频道名的来源字段
    source_dict = {}
    for df in [my_sum, working]:
        for idx, row in df.iterrows():
            orig_name = row.get("频道名", "")
            src = row.get("来源", "")
            if orig_name and src:
                if orig_name not in source_dict:
                    source_dict[orig_name] = src

    # 赋默认值和映射
    if "来源" not in channel_data.columns:
        channel_data["来源"] = channel_data["原始名"].map(source_dict).fillna("")
    else:
        # 如果已存在来源列，更新映射但保留已有非空值
        channel_data["来源"] = channel_data.apply(
            lambda row: source_dict.get(row["原始名"], row["来源"]) if not row["来源"] else row["来源"],
            axis=1
        )

    if "输出顺序" not in channel_data.columns:
        channel_data["输出顺序"] = "未排序"

    if "是否已维护" not in channel_data.columns:
        channel_data["是否已维护"] = "否"

    # =============================
    # 网络频道库
    # =============================
    network_channels_df = read_csv_auto_encoding(NETWORK_CHANNELS_PATH)

    if "channel" in network_channels_df.columns:
        network_col = "channel"
    elif "name" in network_channels_df.columns:
        network_col = "name"
    else:
        print("网络数据库无频道名列！")
        sys.exit(1)

    network_channels_df = network_channels_df.dropna(subset=[network_col])
    network_channels_df["std_key"] = network_channels_df[network_col].apply(mechanical_standardize)
    network_channels = dict(zip(network_channels_df["std_key"], network_channels_df[network_col]))

    # =============================
    # 统一字段
    # =============================
    for df in [my_sum, working]:
        for col in ["视频编码", "分辨率", "帧率", "音频", "相似度"]:
            if col not in df.columns:
                df[col] = ""

    total_before = pd.concat([my_sum, working], ignore_index=True, sort=False)

    required_cols = ["频道名", "地址", "来源", "图标", "检测时间",
                     "分组", "视频编码", "分辨率", "帧率", "音频", "相似度"]

    for col in required_cols:
        if col not in total_before.columns:
            total_before[col] = ""

    total_before["std_key"] = total_before["频道名"].apply(mechanical_standardize)

    # channel_data 标准化 key
    channel_data["标准名_std_key"] = channel_data["标准名"].apply(mechanical_standardize)
    channel_data["原始名_std_key"] = channel_data["原始名"].apply(mechanical_standardize)

    existing_orig_names = set(channel_data["原始名"].fillna("").unique())

    std_name_dict = dict(zip(channel_data["标准名_std_key"], channel_data["标准名"]))
    std_key_to_pending = dict(zip(channel_data["标准名_std_key"], channel_data["拟匹配频道名"]))

    # 匹配结果
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
                "分组": group_label,
                "来源": source_dict.get(orig_name, ""),
                "输出顺序": "未排序",
                "是否已维护": "否"
            }
            channel_data = pd.concat(
                [channel_data, pd.DataFrame([new_row])],
                ignore_index=True
            )
            existing_orig_names.add(orig_name)

    # =============================
    #    逐条匹配
    # =============================
    print("开始匹配标准化频道名...")

    total_len = len(total_before)
    batch_size = 50
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

            # ——精准匹配：原始名且 是否已维护 == "是" —— 修改点
            if original_name in existing_orig_names:
                matched_std_name = channel_data.loc[
                    channel_data["原始名"] == original_name, "标准名"
                ].values
                maintained_val = channel_data.loc[
                    channel_data["原始名"] == original_name, "是否已维护"
                ].values

                if len(matched_std_name) > 0 and len(maintained_val) > 0:
                    mv = maintained_val[0]
                    if isinstance(mv, str) and mv.strip() == "是":
                        matched_name = matched_std_name[0]
                        match_info = "精准匹配"
                        match_score = 100.0
                        precise_match_count += 1
                    else:
                        matched_name = None
                else:
                    matched_name = None

            # ——模糊匹配——
            if matched_name is None:
                choices = list(network_channels.keys())
                matches = process.extract(key, choices, scorer=fuzz.ratio, limit=1)

                if matches:
                    best_key, score, _ = matches[0]
                    if score > 90:
                        matched_name = clean_network_std_name(network_channels[best_key])
                        match_info = "模糊匹配（>90%）"
                        match_score = float(score)
                        fuzzy_match_count += 1
                        add_channel_data_if_not_exists(original_name, matched_name, "待确认分组")
                    else:
                        matched_name = original_name
                        add_channel_data_if_not_exists(original_name, matched_name, "待标准化")
                else:
                    matched_name = original_name
                    add_channel_data_if_not_exists(original_name, matched_name, "待标准化")

            matched_standard_names.append(matched_name)
            matched_match_info.append(match_info)
            matched_match_score.append(match_score)

        if time.time() - last_print_time >= 5:
            print(f"已处理 {end_idx}/{total_len} 条，精准 {precise_match_count}，模糊 {fuzzy_match_count}")
            last_print_time = time.time()

    # 更新数据
    total_before["频道名"] = matched_standard_names
    total_before["匹配信息"] = matched_match_info
    total_before["匹配值"] = matched_match_score

    # 分组
    std_name_to_group = dict(zip(channel_data["标准名"], channel_data["分组"]))
    total_before["分组"] = total_before["频道名"].apply(lambda x: std_name_to_group.get(x, "未分类"))

    # 去重
    channel_data = channel_data.drop_duplicates(subset=["原始名"], keep='first')

    print("保存输出文件...")

    # 保存 total_final.csv
    total_before.to_csv(
        OUTPUT_TOTAL_FINAL, index=False, encoding="utf-8-sig",
        columns=[
            "频道名", "地址", "来源", "图标", "检测时间", "分组",
            "视频编码", "分辨率", "帧率", "音频", "相似度", "匹配信息", "匹配值"
        ]
    )

    # 保存 channel_data.csv，新增三列
    channel_data.to_csv(
        OUTPUT_CHANNEL_DATA, index=False, encoding="utf-8-sig",
        columns=["原始名", "标准名", "拟匹配频道名", "分组", "来源", "输出顺序", "是否已维护"]
    )

    print("🎉 CSV 标准化处理完成！")


if __name__ == "__main__":
    main()
