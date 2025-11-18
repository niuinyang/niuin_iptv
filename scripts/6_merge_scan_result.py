import os
import glob
import pandas as pd

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

if __name__ == "__main__":
    merge_csv_files("output/middle/final/ok", "output/working.csv")