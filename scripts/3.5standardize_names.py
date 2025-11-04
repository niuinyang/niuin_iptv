import os
import re
import pandas as pd
from rapidfuzz import process

IPTV_DB_PATH = "./iptv-database"

INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
OUTPUT_TOTAL = "output/total.csv"
OUTPUT_CHANNEL = "input/channel.csv"

def load_name_map():
    """加载iptv-org数据库频道名和别名映射"""
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.startswith("name,"):
                continue
            parts = line.strip().split(",")
            if len(parts) < 1:
                continue
            std_name = parts[0].strip()
            name_map[std_name.lower()] = std_name
    # 这里简化处理，实际可以解析 other_names 列映射别名
    # 你可根据之前代码完善
    return name_map

def clean_channel_name(name):
    """去除频道名中括号（）和【】及其内容"""
    # 例如： "3ABN Kids (1080p) [Geo-blocked]" => "3ABN Kids"
    name = re.sub(r"[\(\[][^\)\]]*[\)\]]", "", name)
    return name.strip()

def safe_read_csv(path):
    """尝试用utf-8打开，失败用gbk打开，返回DataFrame"""
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="gbk")
    return df

def get_std_name(name, name_map, threshold=95):
    """
    只做模糊匹配，匹配度>=threshold返回标准名，否则返回原名
    """
    name_lower = name.lower()
    if name_lower in name_map:
        return name_map[name_lower], 100.0, "精准匹配"
    # 模糊匹配
    choices = list(name_map.keys())
    match, score, _ = process.extractOne(name_lower, choices)
    if score >= threshold:
        return name_map[match], score, f"模糊匹配({score:.0f})"
    else:
        return name, score, f"匹配不足({score:.0f})"

def standardize_my_sum(my_sum_df):
    # 自有源不做匹配，标准名就是原名
    my_sum_df['final_name'] = my_sum_df.iloc[:, 0].astype(str).str.strip()
    my_sum_df['match_info'] = "自有源"
    return my_sum_df

def standardize_working(working_df, my_sum_df, name_map):
    # 先清理名字括号内容
    working_df['clean_name'] = working_df.iloc[:, 0].astype(str).apply(clean_channel_name)

    # 用 my_sum 的 final_name 做匹配字典（key是频道名小写，value是final_name）
    my_name_dict = dict(zip(my_sum_df.iloc[:,0].str.lower(), my_sum_df['final_name']))

    final_names = []
    match_infos = []
    for name, clean_name in zip(working_df.iloc[:,0], working_df['clean_name']):
        # 先在 my_sum 里找匹配
        clean_name_lower = clean_name.lower()
        if clean_name_lower in my_name_dict:
            final_names.append(my_name_dict[clean_name_lower])
            match_infos.append("自有源匹配")
        else:
            # 否则用网络名映射库模糊匹配
            std_name, score, info = get_std_name(clean_name, name_map)
            if score < 95:
                std_name = name  # 匹配度低时用原名
            final_names.append(std_name)
            match_infos.append(info)

    working_df['final_name'] = final_names
    working_df['match_info'] = match_infos
    # 保留检测时间列
    # 这里假设检测时间列是原表的第4列(索引3)
    # 若有不同，请根据实际调整
    return working_df

def save_channel_csv(my_sum_df, working_df):
    # 标准化名和对应分组两列输出到 input/channel.csv
    # 取final_name和分组列(假设分组列名是'分组'，若无请替换为正确列名)
    my_channel = my_sum_df[['final_name', '分组']].copy()
    working_channel = working_df[['final_name', '分组']].copy()
    combined = pd.concat([my_channel, working_channel], ignore_index=True).drop_duplicates()
    combined.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")

def save_total_csv(my_sum_df, working_df):
    # 合并两个df，保留所有列，新增来源列表示自有源或网络源
    my_sum_df['来源_标识'] = '自有源'
    working_df['来源_标识'] = '网络源'

    combined = pd.concat([my_sum_df, working_df], ignore_index=True)
    combined.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")

def main():
    print("🚀 开始执行标准化匹配流程...")

    # 加载iptv-org数据库映射
    name_map = load_name_map()

    print(f"📁 读取源文件：\n  {INPUT_MY}\n  {INPUT_WORKING}")

    # 读取两个CSV，自动编码尝试
    my_sum_df = safe_read_csv(INPUT_MY)
    working_df = safe_read_csv(INPUT_WORKING)

    # 自有源标准化（不匹配，直接用原频道名）
    my_sum_df = standardize_my_sum(my_sum_df)

    # 网络源标准化（先用my_sum匹配，没匹配的用iptv数据库模糊匹配）
    working_df = standardize_working(working_df, my_sum_df, name_map)

    # 保存标准化结果（可选中间文件）
    my_sum_df.to_csv(INPUT_MY.replace(".csv", "_standardized.csv"), index=False, encoding="utf-8-sig")
    working_df.to_csv(INPUT_WORKING.replace(".csv", "_standardized.csv"), index=False, encoding="utf-8-sig")

    # 生成频道名和分组对应表
    save_channel_csv(my_sum_df, working_df)

    # 生成合并总表 total.csv
    save_total_csv(my_sum_df, working_df)

    print(f"✅ 处理完成，结果保存到：\n  {OUTPUT_CHANNEL}\n  {OUTPUT_TOTAL}")

if __name__ == "__main__":
    main()