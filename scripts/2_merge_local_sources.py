#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一编码版 IPTV 合并脚本
支持多语言文件（中文 / 英文 / 俄语 / 西葡语）
输出文件 UTF-8 无 BOM，兼容 Excel / GitHub / Windows / macOS
"""

import os
import re
import csv
import chardet
import platform

# 输入目录，分别是网络源和我的源目录
networksource_dir = "input/download/network"
mysource_dir = "input/download/my"

# 输出目录及日志目录配置
OUTPUT_DIR = "output"
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
MERGE_DIR = "output/middle/merge"
LOG_MERGE_DIR = os.path.join(LOG_DIR, "merge")

# 创建输出和日志目录（如果不存在）
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MERGE_DIR, exist_ok=True)
os.makedirs(LOG_MERGE_DIR, exist_ok=True)

# 输出合并后文件的路径配置
NETWORK_M3U = os.path.join(MERGE_DIR, "networksource_total.m3u")
NETWORK_CSV = os.path.join(MERGE_DIR, "networksource_total.csv")
NETWORK_LOG = os.path.join(LOG_MERGE_DIR, "networksource_skipped.log")

MYSOURCE_M3U = os.path.join(MERGE_DIR, "mysource_total.m3u")
MYSOURCE_CSV = os.path.join(MERGE_DIR, "mysource_total.csv")
MYSOURCE_LOG = os.path.join(LOG_MERGE_DIR, "mysource_skipped.log")

# 来源文件名到中文描述的映射，方便区分不同来源
SOURCE_MAPPING = {
    "1sddxzb.m3u": "济南电信组播",
    "2sddxdb.m3u": "济南电信单播",
    "3jnltzb.m3u": "济南联通组播",
    "4sdqdlt.m3u": "青岛联通单播",
    "5sdyd_ipv6.m3u": "山东移动单播",
    "6shyd_ipv6.m3u": "上海移动单播",
}

def safe_open(file_path):
    """
    自动检测文件编码并读取内容为行列表，去除隐藏字符。
    兼容各种编码格式，避免因编码错误导致读取失败。
    """
    with open(file_path, 'rb') as f:
        raw = f.read()
        enc = chardet.detect(raw)['encoding'] or 'utf-8'
    try:
        text = raw.decode(enc, errors='ignore')
    except Exception:
        text = raw.decode('utf-8', errors='ignore')
    text = text.replace('\x00', '')  # 清理空字符
    return text.splitlines()

def read_m3u_file(file_path: str):
    """
    读取 M3U 文件，解析频道信息。
    支持解析EXTINF行，提取频道名、播放地址、图标URL。
    返回频道字典列表。
    """
    channels = []
    try:
        lines = safe_open(file_path)
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXTINF:"):
                info_line = line
                url_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                if info_line.startswith("#EXTINF:-1 "):
                    content = info_line[len("#EXTINF:-1 "):]
                else:
                    content = info_line[len("#EXTINF:"):]

                attributes = re.findall(r'\w+="[^"]*"', content)
                for attr in attributes:
                    content = content.replace(attr, '')

                if ',' in content:
                    display_name = content.split(',')[-1].strip()
                else:
                    display_name = content.strip()

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

def read_txt_multi_section_csv(file_path: str):
    """
    读取TXT或CSV格式文件（多段标题格式），
    过滤无效行和注释，提取频道名和播放地址。
    返回频道字典列表。
    """
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

def merge_all_sources(source_dir):
    """
    遍历指定目录，读取所有M3U和TXT文件，合并频道列表。
    针对特定文件进行地址替换处理。
    返回合并后的频道列表。
    """
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

def write_output_files(channels, output_m3u, output_csv, skipped_log):
    """
    将合并后的频道列表写入M3U和CSV文件。
    同时生成跳过的频道日志，过滤无效或重复URL。
    输出全部文件均为UTF-8无BOM编码。
    """
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

    with open(output_m3u, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")
        for ch in valid_channels:
            f.write(f'#EXTINF:-1,{ch["display_name"]}\n{ch["url"]}\n')

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标"])
        for ch in valid_channels:
            writer.writerow([ch["display_name"], ch["url"], ch["source"], ch.get("logo", "")])

    with open(skipped_log, "w", encoding="utf-8") as f:
        f.write("频道名,地址,跳过原因\n")
        for ch in skipped_channels:
            f.write(f"{ch['display_name']},{ch['url']},{ch['reason']}\n")

    print(f"📁 输出文件：{output_m3u} 和 {output_csv}")
    print(f"📁 跳过日志：{skipped_log}")

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
    else:
        print("⚠️ 没有读取到任何频道")

    channels_my = merge_all_sources(mysource_dir)
    if channels_my:
        write_output_files(
            channels_my,
            output_m3u=MYSOURCE_M3U,
            output_csv=MYSOURCE_CSV,
            skipped_log=MYSOURCE_LOG
        )
    else:
        print(f"⚠️ 没有读取到任何频道：{mysource_dir}")
