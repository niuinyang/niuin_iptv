#!/usr/bin/env python3
import csv
import os
import sys

def split_deep_scan(input_path="output/middle/merge/networksource_total.csv",
                    chunk_size=2000,
                    output_dir="output/middle/chunk"):
    """
    读取指定 CSV 文件，将其按指定大小分割成多个小文件，保存到目标目录。
    每个分片文件命名为 chunk-N.csv，N 从1开始递增。
    """

    # 打印当前工作目录和文件路径信息
    print("当前工作目录:", os.getcwd())
    print(f"输入文件路径: {input_path}")
    print(f"输出目录路径: {output_dir}")

    # 检查输入文件是否存在，若不存在则退出程序
    if not os.path.exists(input_path):
        print(f"错误：输入文件不存在 - {input_path}")
        sys.exit(1)

    # 创建输出目录（若不存在则新建）
    os.makedirs(output_dir, exist_ok=True)

    # 删除输出目录中已有的旧分片文件，确保干净环境
    print("清理旧的分片文件...")
    for filename in os.listdir(output_dir):
        if filename.startswith("chunk") and filename.endswith(".csv"):
            os.remove(os.path.join(output_dir, filename))
            print(f"已删除旧文件: {filename}")

    # 尝试使用 UTF-8 编码读取 CSV 文件内容，若失败则尝试自动检测编码
    try:
        with open(input_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
    except UnicodeDecodeError:
        print("UTF-8 解码失败，尝试自动检测文件编码...")
        import chardet
        with open(input_path, 'rb') as f:
            data = f.read()
            detected = chardet.detect(data)
            encoding = detected.get('encoding', 'utf-8')
        print(f"检测到文件编码: {encoding}")
        text = data.decode(encoding, errors='ignore')
        rows = list(csv.DictReader(text.splitlines()))
        headers = rows[0].keys() if rows else []

    # 统计读取的总数据行数（不含表头）
    total_rows = len(rows)
    print(f"读取到总数据行数: {total_rows}")

    # 计算需要拆分的文件数量
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    print(f"预计生成分片数量: {total_chunks}")

    # 按块大小循环写入分片文件
    for start_index in range(0, total_rows, chunk_size):
        chunk_rows = rows[start_index:start_index + chunk_size]
        chunk_number = start_index // chunk_size + 1
        chunk_filename = f"chunk-{chunk_number}.csv"
        chunk_filepath = os.path.join(output_dir, chunk_filename)

        with open(chunk_filepath, "w", newline='', encoding='utf-8') as cf:
            writer = csv.DictWriter(cf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(chunk_rows)

        print(f"写入分片文件: {chunk_filepath}，行数: {len(chunk_rows)}")

    print("所有分片文件写入完成。")


if __name__ == "__main__":
    split_deep_scan()
