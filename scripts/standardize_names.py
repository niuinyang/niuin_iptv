import csv
import os
import pandas as pd

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

def standardize_csv(file_path, has_header=True):
    """
    标准化CSV文件第一列频道名，新增标准名为第一列，所有列后移
    """
    name_map = load_name_map()

    if has_header:
        df = pd.read_csv(file_path)
    else:
        df = pd.read_csv(file_path, header=None)

    # 取第一列为原始频道名
    original_names = df.iloc[:, 0].astype(str).str.strip()

    # 获取标准名（不区分大小写）
    def get_std_name(name):
        return name_map.get(name.lower(), name)

    std_names = original_names.apply(get_std_name)

    # 插入新第一列为标准名
    df.insert(0, 'standard_name', std_names)

    # 保存回文件，保留header状态
    df.to_csv(file_path, index=False, header=has_header)

def main():
    # 自有源 my_sum.csv 无表头
    standardize_csv("input/mysource/my_sum.csv", has_header=False)
    # 网络源 working.csv 有表头
    standardize_csv("output/working.csv", has_header=True)

if __name__ == "__main__":
    main()