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
    return name.strip()

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
    my_name_dict = dict(zip(my_sum_df.iloc[:,0].str.lower(), my_sum_df['final_name']))

    total = len(working_df)
    final_names = []
    match_infos = []
    matched_count = 0
    unmatched_count = 0

    print(f"🔄 开始对 working.csv 共 {total} 条记录进行标准化匹配...")

    for idx, (orig_name, clean_name) in enumerate(zip(working_df['original_channel_name'], working_df['clean_name']), 1):
        clean_name_lower = clean_name.lower()
        if clean_name_lower in my_name_dict:
            std_name = my_name_dict[clean_name_lower]
            match_info = "自有源匹配"
            matched_count += 1
        else:
            std_name, score, info = get_std_name(clean_name, name_map)
            if score < 95:
                std_name = orig_name
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

def main():
    print("🚀 开始执行标准化匹配流程...")

    my_sum_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)

    print(f"读取源文件：\n  📁 {INPUT_MY}\n  📁 {INPUT_WORKING}")

    name_map = load_name_map()
    print(f"✅ 数据库加载完成，映射总数：{len(name_map)}")

    my_sum_df = standardize_my_sum(my_sum_df)
    working_df = standardize_working(working_df, my_sum_df, name_map)

    def build_total_df(df):
        cols = df.columns.tolist()
        addr = df.iloc[:,1] if len(cols) > 1 else pd.Series([""]*len(df))
        source = df.iloc[:,2] if len(cols) > 2 else pd.Series([""]*len(df))
        check_time = df.iloc[:,3] if len(cols) > 3 else pd.Series([""]*len(df))
        icon = df.iloc[:,4] if len(cols) > 4 else pd.Series([""]*len(df))
        group = df.iloc[:,5] if len(cols) > 5 else pd.Series([""]*len(df))

        if 'original_channel_name' in df.columns:
            original_channel_name = df['original_channel_name']
        else:
            original_channel_name = df.iloc[:,0].astype(str)

        match_info = df['match_info'] if 'match_info' in df.columns else pd.Series(["自有源"]*len(df))

        return pd.DataFrame({
            "频道名": df['final_name'],
            "地址": addr,
            "来源": source,
            "检测时间": check_time,
            "图标": icon,
            "分组": group,
            "匹配信息": match_info,
            "原始频道名": original_channel_name
        })

    my_sum_out = build_total_df(my_sum_df)
    working_out = build_total_df(working_df)

    total_df = pd.concat([my_sum_out, working_out], ignore_index=True)
    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成合并文件: {OUTPUT_TOTAL}")

    # 输出 channel.csv 两列：final_name 和 分组
    channel_list = []
    for df in [my_sum_df, working_df]:
        for _, row in df.iterrows():
            final_name = row['final_name']
            group = ""
            if len(row) > 5:
                group = row.iloc[5] if isinstance(row, pd.Series) else (row[5] if len(row) > 5 else "")
            channel_list.append((final_name, group))
    channel_df = pd.DataFrame(channel_list, columns=["final_name", "分组"])
    channel_df.drop_duplicates(inplace=True)
    channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成频道列表文件: {OUTPUT_CHANNEL}")

if __name__ == "__main__":
    main()