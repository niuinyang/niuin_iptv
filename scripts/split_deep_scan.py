import csv
import os

def split_deep_scan(input_path="output/middle/deep_scan.csv",
                    chunk_size=1000,
                    output_dir="input_chunks"):
    os.makedirs(output_dir, exist_ok=True)
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    print(f"Total rows: {total}")

    for i in range(0, total, chunk_size):
        chunk_rows = rows[i:i+chunk_size]
        chunk_num = i // chunk_size + 1
        chunk_filename = f"chunk_{chunk_num}.csv"
        chunk_path = os.path.join(output_dir, chunk_filename)

        with open(chunk_path, "w", newline='', encoding='utf-8') as cf:
            writer = csv.DictWriter(cf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(chunk_rows)

        print(f"Wrote {chunk_path} with {len(chunk_rows)} rows")

if __name__ == "__main__":
    split_deep_scan()