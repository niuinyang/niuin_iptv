import os
import glob
import pandas as pd
import json

def merge_csv_files(input_folder, output_file):
    # 找到所有csv文件
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return
    # 读取所有csv合并
    df_list = []
    for file in csv_files:
        df = pd.read_csv(file)
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)
    # 保存合并文件
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False)
    print(f"Merged {len(csv_files)} CSV files into {output_file}")

def merge_json_files(input_folder, existing_file):
    # 读取已有的缓存json，如果不存在则初始化为空字典
    if os.path.exists(existing_file):
        with open(existing_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    else:
        existing_data = {}

    # 读取所有chunk目录下的json文件
    json_files = glob.glob(os.path.join(input_folder, "*.json"))
    if not json_files:
        print(f"No JSON files found in {input_folder}")
        return

    # 合并新json到已有json，假设都是字典结构，后面的覆盖前面的同名key
    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            new_data = json.load(f)
        existing_data.update(new_data)

    # 保存合并后的缓存文件
    os.makedirs(os.path.dirname(existing_file), exist_ok=True)
    with open(existing_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)
    print(f"Merged {len(json_files)} JSON files into {existing_file}")

if __name__ == "__main__":
    merge_csv_files("output/middle/final/ok", "output/working.csv")
    merge_json_files("output/cache/chunk", "output/cache/cache_hashes.json")
