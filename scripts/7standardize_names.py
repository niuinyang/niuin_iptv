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
INPUT_CHANNEL = "input/channel.csv"   # 作为输入的channel.csv
OUTPUT_CHANNEL = "input/channel.csv"  # 覆盖写回channel.csv
MANUAL_MAP_PATH = "input/manual_map.csv"    # 人工映射文件路径
UNMATCHED_PATH = "unmatched_channels.csv"  # 导出未匹配频道列表（备用）

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
    if enc.lower() != 'utf-8':
        try:
            text = raw.decode(enc, errors='ignore')
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text)
            print(f"✅ 文件 {path} 从 {enc} 转码为 UTF-8")
        except Exception as e:
            print(f"❌ 转码文件 {path} 出错: {e}")
    else:
        print(f"✅ 文件 {path} 已经是 UTF-8，无需转换")

def convert_all_csv_to_utf8(paths):
    for p in paths:
        convert_file_to_utf8(p)

def safe_read_csv(path):
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

def load_manual_map(path=MANUAL_MAP_PATH):
    manual_map = {}
    if not os.path.exists(path):
        # 文件不存在时，创建带表头的空文件
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8-sig", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["原始名称", "标准名称", "拟匹配频道"])
        return manual_map

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = row.get("原始名称", "").strip()
            std_name = row.get("标准名称", "").strip()
            if raw_name and std_name:
                manual_map[raw_name.lower()] = std_name
    return manual_map

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

    for idx, (orig_name, clean_name) in enumerate(zip(working_df['original_channel_name'], working_df['clean_name']), 1):
        orig_name_lower = orig_name.lower()
        clean_name_lower = normalize_name_for_match(clean_name)

        # 优先检查人工映射
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

        if idx % 200 == 0 or idx == total:
            print(f"已处理 {idx}/{total} 条，已匹配 {matched_count} 条，未匹配 {unmatched_count} 条")

    working_df['final_name'] = final_names
    working_df['match_info'] = match_infos
    return working_df

def export_unmatched_for_manual(working_df, manual_map_path=MANUAL_MAP_PATH):
    """
    导出未匹配或低匹配频道，用于人工补全标准名称
    输出列：原始名称, 标准名称(空), 拟匹配频道
    """

    import re

    unmatched_mask = working_df['match_info'].fillna("").str.contains("未匹配|低匹配", na=False)
    unmatched_df = working_df[unmatched_mask].copy()

    def extract_candidate(info):
        if not isinstance(info, str):
            return ""
        # 尝试用正则提取“拟匹配频道:”后面的内容，直到逗号或结尾
        m = re.search(r"拟匹配频道:([^\s,，]+)", info)
        if m:
            return m.group(1).strip()
        return ""

    # 构造导出 DataFrame
    export_df = pd.DataFrame({
        "原始名称": unmatched_df['original_channel_name'].astype(str).str.strip(),
        "标准名称": "",  # 统一空
        "拟匹配频道": unmatched_df['match_info'].apply(extract_candidate).astype(str).str.strip()
    }).drop_duplicates(subset=["原始名称"], keep="first")

    # 如果没有未匹配，确保文件存在表头
    if export_df.empty:
        if not os.path.exists(manual_map_path):
            os.makedirs(os.path.dirname(manual_map_path), exist_ok=True)
            pd.DataFrame(columns=["原始名称", "标准名称", "拟匹配频道"]).to_csv(manual_map_path, index=False, encoding="utf-8-sig")
        print(f"🔔 无新增未匹配或低匹配频道，已确保 {manual_map_path} 存在。")
        return

    # 读取已有文件，合并，去重
    if os.path.exists(manual_map_path):
        existing = pd.read_csv(manual_map_path, encoding="utf-8-sig", dtype=str)
    else:
        existing = pd.DataFrame(columns=["原始名称", "标准名称", "拟匹配频道"])

    # 确保列存在
    for col in ["原始名称", "标准名称", "拟匹配频道"]:
        if col not in existing.columns:
            existing[col] = ""

    existing = existing[["原始名称", "标准名称", "拟匹配频道"]].astype(str)

    # 合并，优先保留已有标准名称
    combined = pd.concat([existing, export_df], ignore_index=True)
    combined.drop_duplicates(subset=["原始名称"], keep="first", inplace=True)

    os.makedirs(os.path.dirname(manual_map_path), exist_ok=True)
    combined.to_csv(manual_map_path, index=False, encoding="utf-8-sig")

    print(f"🔔 已更新 {manual_map_path}，共 {len(combined)} 条记录。")

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
    print("✅ 已保存文件：input/mysource/my_sum_standardized.csv")

def main():
    print("🚀 开始执行标准化匹配流程...")

    # 先检测并统一编码，避免 utf-8 解码错误
    csv_files = [
        INPUT_MY,
        INPUT_WORKING,
        INPUT_CHANNEL,
        MANUAL_MAP_PATH
    ]
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

    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已保存文件：{OUTPUT_TOTAL}")

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
    print(f"✅ 已保存文件：{OUTPUT_CHANNEL}")

if __name__ == "__main__":
    main()