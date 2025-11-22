#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一编码版 IPTV 合并脚本（加入：频道名乱码修复）
支持多语言文件（中文 / 英文 / 俄语 / 西葡语）
输出文件 UTF-8 无 BOM，兼容 Excel / GitHub / Windows / macOS
"""

import os
import re
import csv
import chardet
import platform

# ================================
# 乱码修复函数：核心增强功能
# ================================
def fix_garbled(text):
    """
    修复 UTF-8 被误当作 Latin-1 解码导致的乱码，例如：
    æ°‘è§† → 民视
    å¤©æ´‹ → 天洋
    """
    if not text:
        return text

    # 判断字符是否异常多非中文字符（检测乱码）
    def looks_garbled(s):
        bad = sum(1 for c in s if ord(c) > 128 and not ('\u4e00' <= c <= '\u9fff'))
        return bad > len(s) * 0.4

    if not looks_garbled(text):
        return text

    try:
        return text.encode("latin1").decode("utf-8")
    except Exception:
        return text


# ================================
# 目录和配置
# ================================
networksource_dir = "input/download/network"
mysource_dir = "input/download/my"

OUTPUT_DIR = "output"
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
MERGE_DIR = "output/middle/merge"
LOG_MERGE_DIR = os.path.join(LOG_DIR, "merge")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MERGE_DIR, exist_ok=True)
os.makedirs(LOG_MERGE_DIR, exist_ok=True)

NETWORK_M3U = os.path.join(MERGE_DIR, "networksource_total.m3u")
NETWORK_CSV = os.path.join(MERGE_DIR, "networksource_total.csv")
NETWORK_LOG = os.path.join(LOG_MERGE_DIR, "networksource_skipped.log")

MYSOURCE_M3U = os.path.join(MERGE_DIR, "mysource_total.m3u")
MYSOURCE_CSV = os.path.join(MERGE_DIR, "mysource_total.csv")
MYSOURCE_LOG = os.path.join(LOG_MERGE_DIR, "mysource_skipped.log")

SOURCE_MAPPING = {
    "1sddxzb.m3u": "济南电信组播",
    "2sddxdb.m3u": "济南电信单播",
    "3jnltzb.m3u": "济南联通组播",
    "4sdqdlt.m3u": "青岛联通单播",
    "5sdyd_ipv6.m3u": "山东移动单播",
    "6shyd_ipv6.m3u": "上海移动单播",
}

# ================================
# 自动检测编码读取文件
# ================================
def safe_open(file_path):
    with open(file_path, 'rb') as f:
        raw = f.read()
        enc = chardet.detect(raw)['encoding'] or 'utf-8'

    try:
        text = raw.decode(enc, errors='ignore')
    except:
        text = raw.decode('utf-8', errors='ignore')

    text = text.replace('\x00', '')
    return text.splitlines()


# ================================
# 读取 M3U 文件
# ================================
def read_m3u_file(file_path: str):
    channels = []
    try:
        lines = safe_open(file_path)
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXTINF:"):
                info_line = line
                url_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                # 去掉前缀
                if info_line.startswith("#EXTINF:-1 "):
                    content = info_line[len("#EXTINF:-1 "):]
                else:
                    content = info_line[len("#EXTINF:"):]

                # 去掉属性
                attributes = re.findall(r'\w+="[^"]*"', content)
                for attr in attributes:
                    content = content.replace(attr, '')

                # 取频道名
                if ',' in content:
                    display_name = content.split(',')[-1].strip()
                else:
                    display_name = content.strip()

                # 修复乱码
                display_name = fix_garbled(display_name)

                # 提取 logo
                logo_match = re.search(r'tvg-logo="([^"]+)"', info_line)
                tvg_logo_url = logo_match.group(1).strip() if logo_match else ""

                channels.append({
                    "display_name": display_name,
                    "url": url_line,
                    "logo": tvg_logo_url
                })
                i += 2
            else:
                i += 1

        print(f"📡 已加载 {os.path.basename(file_path)}: {len(channels)} 条频道")
        return channels

    except Exception as e:
        print(f"⚠️ 读取 {file_path} 失败: {e}")
        return []


# ================================
# 读取 TXT/CSV 多段格式
# ================================
def read_txt_multi_section_csv(file_path: str):
    channels = []
    try:
        lines = safe_open(file_path)
        for line in lines:
            line = line.strip()
            if not line or "#genre#" in line:
                continue

            parts = line.split(",", 1)
            if len(parts) != 2:
                continue

            display_name, url = parts[0].strip(), parts[1].strip()

            if not (url.startswith("http://") or url.startswith("https://") or url.startswith("rtsp://")):
                continue

            display_name = fix_garbled(display_name)

            channels.append({
                "display_name": display_name,
                "url": url,
                "logo": ""
            })

        print(f"📡 已加载 {os.path.basename(file_path)}: {len(channels)} 条频道")
        return channels

    except Exception as e:
        print(f"⚠️ 读取 {file_path} 失败: {e}")
        return []


# ================================
# 合并目录内所有源
# ================================
def merge_all_sources(source_dir):
    all_channels = []
    if not os.path.exists(source_dir):
        print(f"⚠️ 源目录不存在: {source_dir}")
        return []

    print(f"📂 扫描目录: {source_dir}")

    for file in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file)
        if file.endswith(".m3u"):
            chs = read_m3u_file(file_path)
            if file == "1sddxzb.m3u":
                for ch in chs:
                    ch["url"] = ch["url"].replace("192.168.50.1:20231", "192.168.31.2:4022")
        elif file.endswith(".txt"):
            chs = read_txt_multi_section_csv(file_path)
        else:
            continue

        for ch in chs:
            ch["source_file"] = file

        all_channels.extend(chs)

    print(f"\n📊 合并所有频道，共 {len(all_channels)} 条")
    return all_channels


# ================================
# 输出文件
# ================================
def write_output_files(channels, output_m3u, output_csv, skipped_log):
    seen_urls = set()
    valid_channels = []
    skipped_channels = []

    for ch in channels:
        url = ch["url"]

        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("rtsp://")):
            skipped_channels.append({
                "display_name": ch["display_name"],
                "url": url,
                "reason": "无效URL（非 http/https/rtsp 开头）"
            })
            continue

        if url in seen_urls:
            skipped_channels.append({
                "display_name": ch["display_name"],
                "url": url,
                "reason": "重复URL"
            })
            continue

        seen_urls.add(url)

        source_file = ch.get("source_file", "")
        source_desc = SOURCE_MAPPING.get(source_file, "网络源")

        valid_channels.append({
            "display_name": ch["display_name"],
            "url": url,
            "logo": ch.get("logo", ""),
            "source": source_desc,
        })

    print(f"\n✅ 有效频道: {len(valid_channels)} 条（去重后）")
    print(f"🚫 跳过无效或重复: {len(skipped_channels)} 条")

    # 写 M3U
    with open(output_m3u, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")
        for ch in valid_channels:
            f.write(f'#EXTINF:-1,{ch["display_name"]}\n{ch["url"]}\n')

    # 写 CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标"])
        for ch in valid_channels:
            writer.writerow([ch["display_name"], ch["url"], ch["source"], ch.get("logo", "")])

    # 写跳过日志
    with open(skipped_log, "w", encoding="utf-8") as f:
        f.write("频道名,地址,跳过原因\n")
        for ch in skipped_channels:
            f.write(f"{ch['display_name']},{ch['url']},{ch['reason']}\n")

    print(f"📁 输出文件：{output_m3u} 和 {output_csv}")
    print(f"📁 跳过日志：{skipped_log}")


# ================================
# 主程序
# ================================
if __name__ == "__main__":
    print(f"🔧 当前系统: {platform.system()}，输出统一为 UTF-8 无 BOM")

    channels = merge_all_sources(networksource_dir)
    if channels:
        write_output_files(
            channels,
            output_m3u=NETWORK_M3U,
            output_csv=NETWORK_CSV,
            skipped_log=NETWORK_LOG
        )

    channels_my = merge_all_sources(mysource_dir)
    if channels_my:
        write_output_files(
            channels_my,
            output_m3u=MYSOURCE_M3U,
            output_csv=MYSOURCE_CSV,
            skipped_log=MYSOURCE_LOG
        )