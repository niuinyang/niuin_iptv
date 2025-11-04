#!/usr/bin/env python3
# download_m3u.py
# 用法: python download_m3u.py
# 读取 input/network/networksource.txt，下载 M3U 文件到 input/network/network_sources/
# 覆盖旧文件，清理未在列表中的旧文件

import os
import sys
import re
import time
import random
import logging
from urllib.parse import urlparse, unquote
import requests

# ==============================
# 配置
# ==============================
SOURCE_LIST = "input/network/networksource.txt"
OUTPUT_DIR = "input/network/network_sources"
LOG_FILE = "download_m3u.log"
ERROR_LOG = "download_errors.log"

RETRIES = 3
BACKOFF_BASE = 2
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
MIN_SIZE_BYTES = 200
M3U_KEYWORDS = ["#EXTM3U", ".m3u", ".m3u8"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

# ==============================
# 日志设置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==============================
# 工具函数
# ==============================
def sanitize_filename(s: str) -> str:
    s = unquote(s)
    s = re.sub(r"[:/?#\[\]@!$&'()*+,;=\"<>\\|]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:200]

def guess_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    if not name:
        name = parsed.netloc
    name = sanitize_filename(name)
    if not os.path.splitext(name)[1]:
        name = name + ".m3u"
    return name

def looks_like_m3u(content_bytes: bytes) -> bool:
    try:
        txt = content_bytes[:1024].decode("utf-8", errors="ignore").lower()
    except Exception:
        return False
    for kw in M3U_KEYWORDS:
        if kw.lower() in txt:
            return True
    return False

def download_url(url: str, out_path: str) -> (bool, str):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://{urlparse(url).netloc}/",
        "Connection": "keep-alive",
    }

    temp_path = out_path + ".tmp"
    for attempt in range(1, RETRIES+1):
        try:
            logging.info(f"Downloading ({attempt}/{RETRIES}): {url}")
            with requests.get(url, headers=headers, stream=True,
                              timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True) as r:
                if r.status_code != 200:
                    raise Exception(f"HTTP {r.status_code}")
                with open(temp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                size = os.path.getsize(temp_path)
                if size < MIN_SIZE_BYTES:
                    raise Exception(f"File too small ({size} bytes)")
                with open(temp_path, "rb") as f:
                    head = f.read(2048)
                if not looks_like_m3u(head):
                    logging.warning("Content does not look like M3U")
                os.replace(temp_path, out_path)
                return True, "OK"
        except Exception as e:
            wait = BACKOFF_BASE ** (attempt - 1)
            logging.warning(f"Attempt {attempt} failed for {url}: {e}. Backoff {wait}s")
            time.sleep(wait + random.random())
    if os.path.exists(temp_path):
        os.remove(temp_path)
    return False, f"Failed after {RETRIES} attempts"

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)

# ==============================
# 主程序
# ==============================
def main():
    ensure_dirs()
    if not os.path.exists(SOURCE_LIST):
        logging.error(f"Source list not found: {SOURCE_LIST}")
        sys.exit(2)

    # 读取所有 URL
    urls = []
    with open(SOURCE_LIST, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line.split()[0])
    total = len(urls)
    logging.info(f"Total URLs to process: {total}")

    # 下载每个 URL
    downloaded_files = []
    failed = []
    for url in urls:
        try:
            fname = guess_filename_from_url(url)
            out_path = os.path.join(OUTPUT_DIR, fname)
            success, msg = download_url(url, out_path)
            if success:
                logging.info(f"Saved: {out_path} ({msg})")
                downloaded_files.append(fname)
            else:
                logging.error(f"Failed: {url} -> {msg}")
                failed.append((url, msg))
        except Exception as e:
            logging.exception(f"Unhandled error for {url}: {e}")
            failed.append((url, str(e)))

    # 清理未在源列表里的旧文件
    for f in os.listdir(OUTPUT_DIR):
        if f not in downloaded_files:
            path = os.path.join(OUTPUT_DIR, f)
            try:
                os.remove(path)
                logging.info(f"Removed old file: {path}")
            except Exception as e:
                logging.warning(f"Failed to remove {path}: {e}")

    # 写失败日志
    if failed:
        with open(ERROR_LOG, "a", encoding="utf-8") as ef:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            ef.write(f"\n# {ts} - failed {len(failed)}/{total}\n")
            for u, m in failed:
                ef.write(f"{u}    # {m}\n")

    logging.info(f"Done. Total URLs: {total}. Failed: {len(failed)}.")

if __name__ == "__main__":
    main()
