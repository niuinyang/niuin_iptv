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
import unicodedata
import chardet
import platform

# ==============================
# 配置区
# ==============================
SOURCE_DIR = "input/network/network_sources"  # M3U 和 TXT 文件所在目录
OUTPUT_DIR = "output"
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
ICON_DIR = "png"  # 保留目录，不下载图标

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ICON_DIR, exist_ok=True)

OUTPUT_M3U = os.path.join(OUTPUT_DIR, "merge_total.m3u")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "merge_total.csv")
SKIPPED_LOG = os.path.join(LOG_DIR, "skipped.log")

# ==============================
# 工具函数
# ==============================

def safe_open(file_path):
    """自动检测文件编码并返回按行列表"""
    with open(file_path, 'rb') as f:
        raw = f.read()
        enc = chardet.detect(raw)['encoding'] or 'utf-8'
    try:
        text = raw.decode(enc, errors='ignore')
    except Exception:
        text = raw.decode('utf-8', errors='ignore')
    # 清理隐藏字符
    text = text.replace('\x00', '')
    return text.splitlines()

def normalize_channel_name(name: str) -> str:
    """标准化频道名（内部使用）"""
    if not name:
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\s\[\]（）()【】]", "", name)
    name = re.sub(r"[-_\.]", "", name)
    return name.strip().lower()

def get_icon_path(standard_name, tvg_logo_url):
    # 不下载图标，仅返回 URL
    return tvg_logo_url or ""

def read_m3u_file(file_path: str):
    """读取 M3U 文件"""
    channels = []
    try:
        lines = safe_open(file_path)
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXTINF:"):
                info_line = line
                url_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                # 提取频道名为逗号后的所有内容，避免属性内逗号干扰
                m = re.match(r'#EXTINF:-?\d+\s*(?:.*?),\s*(.*)', info_line)
                if m:
                    display_name = m.group(1).strip()
                else:
                    display_name = "未知频道"

                logo_match = re.search(r'tvg-logo=[\'"]([^\'"]+)[\'"]', info_line)
                tvg_logo_url = logo_match.group(1).strip() if logo_match else ""

                icon_path = get_icon_path(display_name, tvg_logo_url)

                channels.append({
                    "display_name": display_name,
                    "url": url_line,
                    "logo": icon_path
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
    """读取多段标题 TXT/CSV 文件"""
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
            if not url.startswith("http"):
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

def write_output_files(channels):
    """统一输出 UTF-8 无 BOM"""
    seen_urls = set()
    valid_channels = []
    skipped_channels = []

    for ch in channels:
        url = ch["url"]
        if not url.startswith("http"):
            skipped_channels.append(ch)
            continue
        if url in seen_urls:
            skipped_channels.append(ch)
            continue
        seen_urls.add(url)
        valid_channels.append(ch)

    print(f"\n✅ 有效频道: {len(valid_channels)} 条（去重后）")
    print(f"🚫 跳过无效或重复: {len(skipped_channels)} 条")

    # 写 M3U（UTF-8 无 BOM）
    with open(OUTPUT_M3U, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")
        for ch in valid_channels:
            display_name = ch["display_name"]
            url = ch["url"]
            f.write(f'#EXTINF:-1,{display_name}\n{url}\n')

    # 写 CSV（UTF-8 无 BOM）
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标"])
        for ch in valid_channels:
            writer.writerow([ch["display_name"], ch["url"], "网络源", ch.get("logo", "")])

    # 写跳过日志（UTF-8 无 BOM）
    with open(SKIPPED_LOG, "w", encoding="utf-8") as f:
        for ch in skipped_channels:
            f.write(f"{ch['display_name']},{ch['url']}\n")

    print(f"📁 输出文件：{OUTPUT_M3U} 和 {OUTPUT_CSV}")
    print(f"📁 跳过日志：{SKIPPED_LOG}")

def merge_all_sources():
    """合并目录内所有 M3U / TXT 源"""
    all_channels = []
    if not os.path.exists(SOURCE_DIR):
        print(f"⚠️ 源目录不存在: {SOURCE_DIR}")
        return []

    print(f"📂 扫描目录: {SOURCE_DIR}")
    for file in os.listdir(SOURCE_DIR):
        file_path = os.path.join(SOURCE_DIR, file)
        if file.endswith(".m3u"):
            chs = read_m3u_file(file_path)
        elif file.endswith(".txt"):
            chs = read_txt_multi_section_csv(file_path)
        else:
            continue
        all_channels.extend(chs)

    print(f"\n📊 合并所有频道，共 {len(all_channels)} 条")
    return all_channels

if __name__ == "__main__":
    print(f"🔧 当前系统: {platform.system()}，输出统一为 UTF-8 无 BOM")
    channels = merge_all_sources()
    if channels:
        write_output_files(channels)
    else:
        print("⚠️ 没有读取到任何频道")