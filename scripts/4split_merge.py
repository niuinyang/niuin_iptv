#!/usr/bin/env python3
import csv
import os
import sys

def split_deep_scan(input_path="output/merge_total.csv",
                    chunk_size=1000,
                    output_dir="output/middle/chunk"):
    """
    将 merge_total.csv 按 chunk_size 行分割到 output/middle/chunk 目录。
    输出文件命名格式：chunk(总文件数)-1.csv, chunk(总文件数)-2.csv ...
    """
    print("🔍 当前工作目录:", os.getcwd())
    print(f"📄 输入文件: {input_path}")
    print(f"📂 输出目录: {output_dir}")

    # 确认输入文件是否存在
    if not os.path.exists(input_path):
        print(f"❌ 未找到输入文件: {input_path}")
        sys.exit(1)

    # 自动创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 清理旧的 chunk 文件
    print("🧹 清理旧的 chunk 文件...")
    for f in os.listdir(output_dir):
        if f.endswith(".csv") and f.startswith("chunk"):
            path = os.path.join(output_dir, f)
            os.remove(path)
            print(f"删除旧文件: {f}")

    # 读取 CSV 内容
    try:
        with open(input_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
    except UnicodeDecodeError:
        print("⚠️ UTF-8 解码失败，尝试使用自动检测编码...")
        import chardet
        with open(input_path, 'rb') as f:
            data = f.read()
            result = chardet.detect(data)
            encoding = result['encoding'] or 'utf-8'
        print(f"📘 检测到编码: {encoding}")
        text = data.decode(encoding, errors='ignore')
        rows = list(csv.DictReader(text.splitlines()))
        headers = rows[0].keys() if rows else []

    total = len(rows)
    print(f"✅ 读取到 {total} 行数据")

    # 计算总块数
    total_chunks = (total + chunk_size - 1) // chunk_size
    print(f"📦 将分成 {total_chunks} 个文件")

    # 按块分割
    for i in range(0, total, chunk_size):
        chunk_rows = rows[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        chunk_filename = f"chunk{total_chunks}-{chunk_num}.csv"
        chunk_path = os.path.join(output_dir, chunk_filename)

        with open(chunk_path, "w", newline='', encoding='utf-8') as cf:
            writer = csv.DictWriter(cf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(chunk_rows)

        print(f"🧩 已写入 {chunk_path} ({len(chunk_rows)} 行)")

    print("✅ 所有分片文件写入完成。")


if __name__ == "__main__":
    split_deep_scan()