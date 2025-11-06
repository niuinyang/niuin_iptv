import aiohttp
import asyncio
import csv
import argparse
import os
import time
from aiohttp import ClientTimeout

# =====================================
# 配置区
# =====================================
DEFAULT_INPUT = "output/merge_total.csv"
DEFAULT_OUTPUT = "output/middle/fast_scan.csv"
DEFAULT_INVALID = "output/middle/fast_scan_invalid.csv"

# 宽松模式参数
DEFAULT_CONCURRENCY = 100
DEFAULT_TIMEOUT = 15  # 超时时间放宽
RETRY_TIMES = 2       # 失败重试次数


async def fetch_url(session, url, timeout):
    """
    执行一次 HTTP 请求检测
    """
    start_time = time.time()
    try:
        async with session.get(url, timeout=ClientTimeout(total=timeout)) as response:
            status = response.status
            elapsed = int((time.time() - start_time) * 1000)
            if status == 200:
                content = await response.read()
                # 放宽判断标准，只要不是太短就算可疑有效
                if len(content) > 50:
                    return True, elapsed, ""
                else:
                    return False, elapsed, "内容太短"
            else:
                return False, elapsed, f"HTTP状态码{status}"
    except asyncio.TimeoutError:
        return False, int((time.time() - start_time) * 1000), "超时"
    except aiohttp.ClientError as e:
        return False, int((time.time() - start_time) * 1000), f"连接错误: {e}"
    except Exception as e:
        return False, int((time.time() - start_time) * 1000), f"其他错误: {e}"


async def check_stream(semaphore, session, row, timeout):
    """
    检测单个流地址（带重试）
    """
    async with semaphore:
        url = row[1]
        for attempt in range(RETRY_TIMES):
            success, elapsed, reason = await fetch_url(session, url, timeout)
            if success:
                return True, elapsed, ""
        return False, elapsed, reason


async def process_file(input_file, output_file, invalid_file, concurrency, timeout):
    """
    读取输入文件并检测所有源
    """
    # 确保目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(invalid_file), exist_ok=True)

    with open(input_file, "r", encoding="utf-8-sig") as infile:
        reader = csv.reader(infile)
        header = next(reader, None)
        if header is None or len(header) < 2:
            print("输入文件格式不正确，至少需要包含频道名和地址。")
            return

        rows = [row for row in reader if len(row) >= 2 and row[1].startswith("http")]
        total = len(rows)
        print(f"待检测源数量: {total}")

        # 初始化输出文件
        with open(output_file, "w", newline="", encoding="utf-8") as valid_f, \
             open(invalid_file, "w", newline="", encoding="utf-8") as invalid_f:
            valid_writer = csv.writer(valid_f)
            invalid_writer = csv.writer(invalid_f)

            # 写入表头
            valid_writer.writerow(["频道名", "地址", "来源", "图标", "检测时间(ms)", "分组", "视频信息"])
            invalid_writer.writerow(["频道名", "地址", "来源", "图标", "检测时间(ms)", "失败原因"])

            semaphore = asyncio.Semaphore(concurrency)
            connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)

            async with aiohttp.ClientSession(connector=connector) as session:
                completed = 0
                last_percent = -1

                for row in rows:
                    success, elapsed, reason = await check_stream(semaphore, session, row, timeout)
                    channel = row[0] if len(row) > 0 else ""
                    source = row[2] if len(row) > 2 else ""
                    logo = row[3] if len(row) > 3 else ""

                    if success:
                        valid_writer.writerow([channel, row[1], source, logo, elapsed, "未分组", ""])
                    else:
                        invalid_writer.writerow([channel, row[1], source, logo, elapsed, reason])

                    completed += 1
                    percent = int((completed / total) * 100)
                    if percent % 5 == 0 and percent != last_percent:
                        last_percent = percent
                        print(f"检测进度: {percent}% ({completed}/{total})")

    print("检测完成。")
    print(f"✅ 有效源已保存到: {output_file}")
    print(f"❌ 无效源已保存到: {invalid_file}")


def main():
    parser = argparse.ArgumentParser(description="宽松模式 IPTV 快速检测器 (Fast Scan Loose Mode)")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="输入 CSV 文件路径")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="有效源输出文件路径")
    parser.add_argument("--invalid", default=DEFAULT_INVALID, help="无效源输出文件路径")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="并发数")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="超时时间(秒)")
    args = parser.parse_args()

    asyncio.run(process_file(args.input, args.output, args.invalid, args.concurrency, args.timeout))


if __name__ == "__main__":
    main()