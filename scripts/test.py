import csv

INPUT_FILE = "output/merge_total.csv"  # 你大文件路径

with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    print(f"表头列数: {len(headers)}, 表头内容: {headers}")

    for i, row in enumerate(reader, start=2):  # 从2开始，因为1是表头
        if len(row) != len(headers):
            print(f"第{i}行列数异常: {len(row)}列，内容：{row}")
            continue
        if not row[0].strip():
            print(f"第{i}行频道名为空，内容：{row}")