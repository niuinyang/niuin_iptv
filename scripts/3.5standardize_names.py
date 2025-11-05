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
OUTPUT_CHANNEL = "input/channel.csv"

def safe_read_csv(path):
    with open(path, "rb") as f:
        raw = f.read()
    result = chardet.detect(raw)
    enc = result["encoding"]
    if enc is None:
        enc = "utf-8"
    if enc.lower() != "utf-8":
        text = raw.decode(enc, errors="ignore")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    return pd.read_csv(path, encoding="utf-8")

def load_name_map():
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            std_name = row["name"].strip()
            name_map[std_name.lower()] = std_name
            others = row.get("other_names", "")
            for alias in others.split(","):
                alias = alias.strip()
                if alias:
                    name_map[alias.lower()] = std_name
    return name_map

def clean_channel_name(name):
    if not isinstance(name, str):
        return ""
    # 去除（）和【】及里面内容
    name = re.sub(r"[\(\（][^\)\）]*[\)\）]", "", name)
    name = re.sub(r"[\[\【][^\]\】]*[\]\】]", "", name)
    # 去除 24/7、7*24、7x24，以及带 not 的情况（不区分大小写）
    name = re.sub(r"\b(not\s*)?(24/7|7\*24|7x24)\b", "", name, flags=re.I)
    return name.strip()

def normalize_name_for_match(name):
    if not isinstance(name, str):
        return ""
    name = clean_channel_name(name)
    # 去除连字符和空格，方便匹配
    name = re.sub(r"[-\s]", "", name)
    return name.lower()

def get_std_name(name, name_map, threshold=95):
    name_lower = name.lower()
    if name_lower in name_map:
        return name_map[name_lower], 100.0, "精确匹配"
    choices = list(name_map.keys())
    match, score, _ = process.extractOne(name_lower, choices)
    if score >= threshold:
        return name_map[match], score, "模糊匹配"
    else:
        return name, score, "未匹配"

def standardize_my_sum(my_sum_df):
    my_sum_df['final_name'] = my_sum_df.iloc[:,0].astype(str)
    my_sum_df['match_info'] = "自有源"
    my_sum_df['original_channel_name'] = my_sum_df.iloc[:,0].astype(str)
    return my_sum_df

def standardize_working(working_df, my_sum_df, name_map):
    working_df['original_channel_name'] = working_df.iloc[:, 0].astype(str)
    working_df['clean_name'] = working_df['original_channel_name'].apply(clean_channel_name)
    my_name_dict = dict(zip(my_sum_df.iloc[:,0].apply(normalize_name_for_match), my_sum_df['final_name']))

    total = len(working_df)
    final_names = []
    match_infos = []
    matched_count = 0
    unmatched_count = 0

    print(f"🔄 开始对 working.csv 共 {total} 条记录进行标准化匹配...")

    for idx, (orig_name, clean_name) in enumerate(zip(working_df['original_channel_name'], working_df['clean_name']), 1):
        clean_name_lower = normalize_name_for_match(clean_name)
        if clean_name_lower in my_name_dict:
            std_name = my_name_dict[clean_name_lower]
            match_info = "自有源匹配"
            matched_count += 1
        else:
            std_name, score, info = get_std_name(clean_name, name_map)
            if score < 95:
                # 使用去除连接符和空格且首字母大写的规范名
                std_name = normalize_name_for_match(clean_name).title()
                match_info = "未匹配"
                unmatched_count += 1
            else:
                match_info = info
                matched_count += 1

        final_names.append(std_name)
        match_infos.append(match_info)

        if idx % 200 == 0 or idx == total:
            print(f"已处理 {idx}/{total} 条，已匹配 {matched_count} 条，未匹配 {unmatched_count} 条")

    working_df['final_name'] = final_names
    working_df['match_info'] = match_infos
    return working_df

def build_total_df(df):
    def safe_col(name_list):
        for name in name_list:
            if name in df.columns:
                return df[name]
        return pd.Series([""] * len(df))

    return pd.DataFrame({
        "频道名": df.get("final_name", df.iloc[:, 0]),
        "地址": safe_col(["地址"]),
        "来源": safe_col(["来源"]),
        "检测时间": safe_col(["检测时间", "检测时间(延迟)"]),
        "图标": safe_col(["图标"]),
        "分组": safe_col(["分组"]),
        "匹配信息": safe_col(["match_info"]),
        "原始频道名": safe_col(["original_channel_name"])
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
        "原始频道名": safe_col(["original_channel_name"])
    })
    out_df.to_csv("input/mysource/my_sum_standardized.csv", index=False, encoding="utf-8-sig")
    print("✅ 已生成标准化自有源文件: input/mysource/my_sum_standardized.csv")

def main():
    print("🚀 开始执行标准化匹配流程...")

    my_sum_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)

    print(f"读取源文件：\n  📁 {INPUT_MY}\n  📁 {INPUT_WORKING}")

    name_map = load_name_map()
    print(f"✅ 数据库加载完成，映射总数：{len(name_map)}")

    my_sum_df = standardize_my_sum(my_sum_df)
    save_standardized_my_sum(my_sum_df)

    working_df = standardize_working(working_df, my_sum_df, name_map)

    my_sum_out = build_total_df(my_sum_df)
    working_out = build_total_df(working_df)

    total_df = pd.concat([my_sum_out, working_out], ignore_index=True)

    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成合并文件: {OUTPUT_TOTAL}")

    # 先取 my_sum_df 的频道名和分组，去重
    my_channels = my_sum_out.loc[:, ["频道名", "分组"]].drop_duplicates()
    my_channel_names = set(my_channels["频道名"].tolist())

    # 再取 working_out 中不在 my_sum_df 的频道
    working_channels = working_out.loc[~working_out["频道名"].isin(my_channel_names), ["频道名", "分组"]].drop_duplicates()

    # 合并两个 DataFrame
    channel_df = pd.concat([my_channels, working_channels], ignore_index=True)

    # 保存频道列表文件
    channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成频道列表文件: {OUTPUT_CHANNEL}")

if __name__ == "__main__":
    main()