#!/usr/bin/env python3
import csv
import os
import sys

# === 获取脚本所在目录，确保路径永远正确 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def split_deep_scan(
        input_path=os.path.join(BASE_DIR, "output/middle/merge/networksource_total.csv"),
        chunk_size=1000,
        output_dir=os.path.join(BASE_DIR, "output/middle/chunk")
    ):
    """
    读取 CSV，将其按指定大小分割成多个分片文件 chunk-N.csv。
    自动清理旧分片文件，路径基于脚本实际位置，避免 GitHub Actions 路径错乱。
    """

    print("=== 路径检查 ===")
    print("脚本所在目录 BASE_DIR:", BASE_DIR)
    print("当前工作目录 os.getcwd():", os.getcwd())
    print("输入文件绝对路径:", os.path.abspath(input_path))
    print("chunk 输出目录绝对路径:", os.path.abspath(output_dir))

    # 检查输入文件是否存在
    if not os.path.exists(input_path):
        print(f"错误：输入文件不存在 - {input_path}")
        sys.exit(1)

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # === 删除 output/middle/chunk 中旧的分片文件 ===
    print("\n=== 清理旧的分片文件 ===")
    for filename in os.listdir(output_dir):
        full_path = os.path.join(output_dir, filename)
        print(f"发现文件: {full_path}")

        # 删除 chunk-*.csv
        if filename.startswith("chunk") and filename.endswith(".csv"):
            os.remove(full_path)
            print(f"👉 已删除: {full_path}")
        else:
            print(f"❌ 跳过（不是 chunk*.csv）: {full_path}")

    # === 读取 CSV 文件 ===
    print("\n=== 读取 CSV 文件 ===")
    try:
        with open(input_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
    except UnicodeDecodeError:
        print("UTF-8 解码失败，尝试自动检测编码...")
        import chardet
        with open(input_path, "rb") as f:
            data = f.read()
            detected = chardet.detect(data)
            encoding = detected.get("encoding", "utf-8")

        print(f"检测到编码: {encoding}")

        text = data.decode(encoding, errors="ignore")
        rows = list(csv.DictReader(text.splitlines()))
        headers = rows[0].keys() if rows else []

    total_rows = len(rows)
    print(f"读取行数: {total_rows}")

    # === 开始拆分 ===
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    print(f"预计生成 {total_chunks} 个分片文件")

    for start in range(0, total_rows, chunk_size):
        chunk_rows = rows[start:start + chunk_size]
        chunk_index = start // chunk_size + 1
        chunk_name = f"chunk-{chunk_index}.csv"
        chunk_path = os.path.join(output_dir, chunk_name)

        with open(chunk_path, "w", newline='', encoding="utf-8") as cf:
            writer = csv.DictWriter(cf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(chunk_rows)

        print(f"✔ 已生成: {chunk_path}（行数 {len(chunk_rows)}）")

    print("\n🎉 所有分片文件已完成")


if __name__ == "__main__":
    split_deep_scan()
