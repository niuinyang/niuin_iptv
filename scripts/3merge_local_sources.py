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

# 新增：mysource 来源映射
SOURCE_MAPPING = {
    "1sddxzb.m3u": "济南电信组播",
    "2sddxdb.m3u": "济南电信单播",
    "3jnltzb.m3u": "济南联通组播",
    "4sdqdlt.m3u": "青岛联通单播",
    "5sdyd_ipv6.m3u": "山东移动单播",
    "6shyd_ipv6.m3u": "上海移动单播",
}

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

                # 去掉开头的 '#EXTINF:-1 ' 或 '#EXTINF:'
                if info_line.startswith("#EXTINF:-1 "):
                    content = info_line[len("#EXTINF:-1 "):]
                else:
                    content = info_line[len("#EXTINF:"):]

                # 匹配所有 key="value" 属性
                attributes = re.findall(r'\w+="[^"]*"', content)

                # 从 content 中删除所有属性
                for attr in attributes:
                    content = content.replace(attr, '')

                # content 中剩余部分，频道名为最后一个逗号后内容
                if ',' in content:
                    display_name = content.split(',')[-1].strip()
                else:
                    display_name = content.strip()

                # 提取 tvg-logo 用于图标
                logo_match = re.search(r'tvg-logo="([^"]+)"', info_line)
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
    """合并目录内所有 M3U / TXT 源，传入目录路径"""
    all_channels = []
    if not os.path.exists(source_dir):
        print(f"⚠️ 源目录不存在: {source_dir}")
        return []

    print(f"📂 扫描目录: {source_dir}")
    for file in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file)
        if file.endswith(".m3u"):
            chs = read_m3u_file(file_path)
            # 这里是新增替换逻辑
            if file == "1sddxzb.m3u":
                for ch in chs:
                    ch["url"] = ch["url"].replace("192.168.50.1:20231", "192.168.31.2:4022")
        elif file.endswith(".txt"):
            chs = read_txt_multi_section_csv(file_path)
        else:
            continue
        # 给每条数据增加来源字段，方便后续区分
        for ch in chs:
            ch["source_file"] = file
        all_channels.extend(chs)

    print(f"\n📊 合并所有频道，共 {len(all_channels)} 条")
    return all_channels

def write_output_files(channels, output_m3u, output_csv, skipped_log):
    """统一输出 UTF-8 无 BOM，支持自定义输出路径"""
    seen_urls = set()
    valid_channels = []
    skipped_channels = []

    for ch in channels:
        url = ch["url"]
        # ✅ 支持 http / https / rtsp 三种协议
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

        # 根据文件名映射中文来源
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

    # 写 M3U（UTF-8 无 BOM）
    with open(output_m3u, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")
        for ch in valid_channels:
            display_name = ch["display_name"]
            url = ch["url"]
            f.write(f'#EXTINF:-1,{display_name}\n{url}\n')

    # 写 CSV（UTF-8 无 BOM）
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标"])
        for ch in valid_channels:
            writer.writerow([ch["display_name"], ch["url"], ch["source"], ch.get("logo", "")])

    # 写跳过日志（UTF-8 无 BOM）
    with open(skipped_log, "w", encoding="utf-8") as f:
        f.write("频道名,地址,跳过原因\n")
        for ch in skipped_channels:
            f.write(f"{ch['display_name']},{ch['url']},{ch['reason']}\n")

    print(f"📁 输出文件：{output_m3u} 和 {output_csv}")
    print(f"📁 跳过日志：{skipped_log}")

if __name__ == "__main__":
    print(f"🔧 当前系统: {platform.system()}，输出统一为 UTF-8 无 BOM")

    # 原有目录合并
    channels = merge_all_sources(SOURCE_DIR)
    if channels:
        write_output_files(
            channels,
            output_m3u=OUTPUT_M3U,
            output_csv=OUTPUT_CSV,
            skipped_log=SKIPPED_LOG
        )
    else:
        print("⚠️ 没有读取到任何频道")

    # 新增：合并 input/mysource/m3u，输出指定文件名
    mysource_dir = "input/mysource/m3u"
    my_m3u = os.path.join(OUTPUT_DIR, "merge_my_sum.m3u")
    my_csv = os.path.join(OUTPUT_DIR, "merge_my_sum.csv")
    my_log = os.path.join(LOG_DIR, "merge_my_sum_skipped.log")

    channels_my = merge_all_sources(mysource_dir)
    if channels_my:
        write_output_files(
            channels_my,
            output_m3u=my_m3u,
            output_csv=my_csv,
            skipped_log=my_log
        )
    else:
        print(f"⚠️ 没有读取到任何频道：{mysource_dir}")
