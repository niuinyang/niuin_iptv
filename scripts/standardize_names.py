import csv
import os
import pandas as pd
from rapidfuzz import process

IPTV_DB_PATH = "./iptv-database"

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
    return name_map

def get_std_name(name, name_map, threshold=80):
    """
    先尝试精确匹配，找不到时用模糊匹配：
    返回匹配度大于阈值的最相似标准名，否则返回原名
    """
    name_lower = name.lower()
    if name_lower in name_map:
        return name_map[name_lower]

    # 模糊匹配
    choices = list(name_map.keys())
    match, score, _ = process.extractOne(name_lower, choices)
    if score >= threshold:
        return name_map[match]
    else:
        return name

def standardize_csv(file_path, name_map):
    """标准化CSV第一列频道名，新增final_name列为第一列，原列后移"""
    df = pd.read_csv(file_path)
    original_names = df.iloc[:, 0].astype(str).str.strip()
    std_names = original_names.apply(lambda x: get_std_name(x, name_map))

    if 'final_name' in df.columns:
        df.drop(columns=['final_name'], inplace=True)

    df.insert(0, 'final_name', std_names)
    df.to_csv(file_path, index=False)

def main():
    name_map = load_name_map()
    standardize_csv("input/mysource/my_sum.csv", name_map)
    standardize_csv("output/working.csv", name_map)

if __name__ == "__main__":
    main()