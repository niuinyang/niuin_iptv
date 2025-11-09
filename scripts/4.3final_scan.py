def ensure_dirs(input_path):
    # 确保输入文件所在目录存在，和输出文件夹也放在该目录下
    input_dir = os.path.dirname(input_path)
    os.makedirs(input_dir, exist_ok=True)
    # 输出文件都放同目录
    os.makedirs(input_dir, exist_ok=True)


def write_final(results, input_path, working_out=None, final_out=None, final_invalid_out=None, generate_working_gbk=False):
    final_map = {r["url"]: r for r in results}

    # 自动检测输入文件编码，修复working.csv乱码问题
    with open(input_path, "rb") as fb:
        raw = fb.read(20000)
        detected_enc = chardet.detect(raw)["encoding"] or "utf-8"

    # 如果没有传入输出路径，则根据输入文件名构造输出路径，且都放input文件同目录
    input_dir = os.path.dirname(input_path)
    input_name = os.path.splitext(os.path.basename(input_path))[0]  # 例如 chunk_1

    if working_out is None:
        working_out = os.path.join(input_dir, f"{input_name}_working.csv")
    if final_out is None:
        final_out = os.path.join(input_dir, f"{input_name}_final.csv")
    if final_invalid_out is None:
        final_invalid_out = os.path.join(input_dir, f"{input_name}_final_invalid.csv")

    with open(input_path, newline='', encoding=detected_enc, errors='ignore') as fin, \
         open(working_out, "w", newline='', encoding='utf-8') as fworking, \
         open(final_out, "w", newline='', encoding='utf-8') as fvalid, \
         open(final_invalid_out, "w", newline='', encoding='utf-8') as finvalid:

        reader = csv.DictReader(fin)

        working_fields = ["频道名","地址","来源","图标","检测时间","分组","视频信息"]
        valid_fields = working_fields + ["相似度"]
        invalid_fields = working_fields + ["未通过信息", "相似度"]

        w_working = csv.DictWriter(fworking, fieldnames=working_fields)
        w_valid = csv.DictWriter(fvalid, fieldnames=valid_fields)
        w_invalid = csv.DictWriter(finvalid, fieldnames=invalid_fields)

        w_working.writeheader()
        w_valid.writeheader()
        w_invalid.writeheader()

        # 预备GBK写入（如果需要）
        if generate_working_gbk:
            working_gbk_path = working_out.rsplit(".",1)[0] + "_gbk.csv"
            fworking_gbk = open(working_gbk_path, "w", newline='', encoding='gbk', errors='ignore')
            w_working_gbk = csv.DictWriter(fworking_gbk, fieldnames=working_fields)
            w_working_gbk.writeheader()
        else:
            fworking_gbk = None
            w_working_gbk = None

        for row in reader:
            url = (row.get("地址") or row.get("url") or "").strip()
            if not url:
                continue
            r = final_map.get(url)
            passed = False
            similarity = ""
            fail_reason = ""

            if r:
                if r.get("status") == "ok" and not r.get("is_fake", False):
                    passed = True
                    similarity = round(r.get("similarity", 0), 4)
                else:
                    fail_reason = r.get("status", "")
                    if r.get("is_fake", False):
                        fail_reason += "; 伪源相似度: {:.4f}".format(r.get("similarity", 0))
                    similarity = round(r.get("similarity", 0), 4)

            if passed:
                working_row = {k: row.get(k, "") for k in working_fields}
                w_working.writerow(working_row)
                if w_working_gbk:
                    try:
                        w_working_gbk.writerow(working_row)
                    except UnicodeEncodeError:
                        fixed_row = {k: (v.encode('gbk', errors='ignore').decode('gbk') if isinstance(v, str) else v) for k,v in working_row.items()}
                        w_working_gbk.writerow(fixed_row)

                valid_row = {k: row.get(k, "") for k in working_fields}
                valid_row["相似度"] = similarity
                w_valid.writerow(valid_row)
            else:
                invalid_row = {k: row.get(k, "") for k in working_fields}
                invalid_row["未通过信息"] = fail_reason or "未知错误"
                invalid_row["相似度"] = similarity
                w_invalid.writerow(invalid_row)

        if fworking_gbk:
            fworking_gbk.close()
            print(f"✔️ 生成 GBK 编码的 working 文件: {working_gbk_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=DEEP_INPUT)
    # 不再默认用固定路径，留空让 write_final 函数自己根据输入文件路径生成
    p.add_argument("--final", default=None)
    p.add_argument("--working", default=None)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--working_gbk", action="store_true", help="是否生成 GBK 编码的 working.csv 版本，兼容Windows Excel")
    args = p.parse_args()

    ensure_dirs(args.input)

    urls = read_deep_input(args.input)
    print(f"Final-stage checking {len(urls)} urls")
    cache = load_cache()
    results = asyncio.run(run_all(urls, concurrency=args.concurrency, cache=cache, timeout=args.timeout))
    save_cache(cache)
    write_final(results, input_path=args.input, working_out=args.working, final_out=args.final, generate_working_gbk=args.working_gbk)
    fake_count = sum(1 for r in results if r.get("is_fake"))
    print(f"Final scan finished. Fake found: {fake_count}/{len(results)}. Wrote final and working files in {os.path.dirname(args.input)}")
