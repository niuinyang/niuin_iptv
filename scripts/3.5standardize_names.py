import os
import re
import pandas as pd
import csv
import chardet
from rapidfuzz import process

IPTV_DB_PATH = "./iptv-database"
INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
OUTPUT_TOTAL = "output/total.csv"
OUTPUT_CHANNEL = "input/channel.csv"

def detect_encoding_and_convert_utf8(filepath):
    """检测文件编码，非utf-8时转为utf-8并覆盖"""
    with open(filepath, "rb") as f:
        rawdata = f.read()
    result = chardet.detect(rawdata)
    enc = result['encoding']
    if enc is None:
        enc = 'utf-8'
    if enc.lower() != 'utf-8':
        print(f"🔄 检测到 {filepath} 编码为 {enc}，正在转换为 UTF-8...")
        text = rawdata.decode(enc)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"✅ 已转码并覆盖保存为 UTF-8: {filepath}")
    else:
        print(f"✅ 文件 {filepath} 已是 UTF-8 编码")

def load_name_map():
    """加载iptv-org数据库频道名和别名映射"""
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
    print(f"📚 已加载 {len(name_map)} 个名称映射")
    return name_map

def clean_channel_name(name: str) -> str:
    """去除频道名中括号()和中括号[]及里面内容"""
    if not isinstance(name, str):
        return ""
    name = re.sub(r'\([^)]*?\)', '', name)  # 去除()
    name = re.sub(r'\[[^\]]*?\]', '', name)  # 去除[]
    return name.strip()

def get_std_name_with_score(name, name_map, threshold=80):
    name_lower = name.lower()
    if name_lower in name_map:
        return name_map[name_lower], 100
    choices = list(name_map.keys())
    match, score, _ = process.extractOne(name_lower, choices)
    if score >= threshold:
        return name_map[match], score
    else:
        return name, score

def standardize_my_sum(file_path):
    """my_sum.csv 不匹配，标准名即为原名，保留检测时间"""
    df = pd.read_csv(file_path, encoding="utf-8")
    original_names = df.iloc[:, 0].astype(str).str.strip()
    df.insert(0, 'final_name', original_names)
    # 不改变检测时间列
    df.to_csv(file_path.replace(".csv", "_standardized.csv"), index=False, encoding="utf-8")
    print(f"✅ {file_path} 标准化完成，输出到 {file_path.replace('.csv', '_standardized.csv')}")
    return df

def standardize_working(file_path, name_map, my_sum_names_set):
    """working.csv 先清理名字，再匹配（优先匹配my_sum.csv名字），保留检测时间"""
    df = pd.read_csv(file_path, encoding="utf-8")
    original_names = df.iloc[:, 0].astype(str).str.strip()
    clean_names = original_names.apply(clean_channel_name)

    std_names = []
    remarks = []
    for orig_name, clean_name in zip(original_names, clean_names):
        if orig_name in my_sum_names_set:
            std_names.append(orig_name)
            remarks.append("自有源优先")
        else:
            std_name, score = get_std_name_with_score(clean_name, name_map)
            if score < 95:
                std_names.append(orig_name)
                remarks.append(f"模糊匹配({score:.0f})低于95")
            else:
                std_names.append(std_name)
                remarks.append(f"模糊匹配({score:.0f})")
    df.insert(0, "final_name", std_names)
    df["match_remark"] = remarks
    # 保留检测时间原列，不做修改
    df.to_csv(file_path.replace(".csv", "_standardized.csv"), index=False, encoding="utf-8")
    print(f"✅ {file_path} 标准化完成，输出到 {file_path.replace('.csv', '_standardized.csv')}")
    return df

def save_channel_csv(my_sum_df, working_df):
    """提取标准化名和分组两列合并输出到 channel.csv"""
    dfs = []
    for df in [my_sum_df, working_df]:
        cols = df.columns.tolist()
        # 频道名在final_name，分组列可能是“分组”或者“group”，尝试识别
        group_col = None
        for c in ["分组", "group"]:
            if c in df.columns:
                group_col = c
                break
        if group_col is None:
            raise ValueError("无法找到分组列")
        dfs.append(df[["final_name", group_col]].rename(columns={group_col:"分组"}))
    combined = pd.concat(dfs, ignore_index=True)
    combined.drop_duplicates(inplace=True)
    combined.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8")
    print(f"✅ 已输出频道名和分组到 {OUTPUT_CHANNEL}")

def save_total_csv(my_sum_df, working_df):
    """合并两个df，输出total.csv"""
    combined = pd.concat([my_sum_df, working_df], ignore_index=True)
    combined.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8")
    print(f"✅ 已输出合并总表到 {OUTPUT_TOTAL}")

def main():
    print("🚀 开始执行标准化匹配流程...")

    # 先确保输入文件编码正确
    for f in [INPUT_MY, INPUT_WORKING]:
        detect_encoding_and_convert_utf8(f)

    name_map = load_name_map()

    # 处理my_sum.csv，直接标准化为原名
    my_sum_df = standardize_my_sum(INPUT_MY)
    my_sum_names_set = set(my_sum_df.iloc[:, 0].astype(str).str.strip())

    # 处理working.csv，先清理再匹配，优先匹配my_sum名
    working_df = standardize_working(INPUT_WORKING, name_map, my_sum_names_set)

    # 生成频道名和分组的channel.csv
    save_channel_csv(my_sum_df, working_df)

    # 合并输出total.csv
    save_total_csv(my_sum_df, working_df)

if __name__ == "__main__":
    main()