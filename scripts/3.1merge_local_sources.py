import os
import re
import csv
import unicodedata

# ==============================
# 配置区
# ==============================
SOURCE_DIR = "input/network/network_sources"  # M3U 和 TXT 文件所在目录
OUTPUT_DIR = "output"
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
ICON_DIR = "png"  # 保留，暂不下载图标

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ICON_DIR, exist_ok=True)

OUTPUT_M3U = os.path.join(OUTPUT_DIR, "merge_total.m3u")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "merge_total.csv")
SKIPPED_LOG = os.path.join(LOG_DIR, "skipped.log")

# ==============================
# 工具函数
# ==============================

def normalize_channel_name(name: str) -> str:
    """标准化频道名（仍保留，用于内部匹配，但不写入CSV）"""
    if not name:
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\s\[\]（）()【】]", "", name)
    name = re.sub(r"[-_\.]", "", name)
    return name.strip().lower()

def get_icon_path(standard_name, tvg_logo_url):
    # 不下载图标，直接返回 URL
    return tvg_logo_url or ""

def read_m3u_file(file_path: str):
    """
    读取 M3U 文件，返回频道列表，每项是 dict
    """
    channels = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXTINF:"):
                info_line = line
                url_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                tvg_match = re.search(r'tvg-name=[\'"]([^\'"]+)[\'"]', info_line)
                tvg_name = tvg_match.group(1).strip() if tvg_match else None

                logo_match = re.search(r'tvg-logo=[\'"]([^\'"]+)[\'"]', info_line)
                tvg_logo_url = logo_match.group(1).strip() if logo_match else ""

                if "," in info_line:
                    display_name = info_line.split(",", 1)[1].strip()
                else:
                    display_name = "未知频道"

                icon_path = get_icon_path(tvg_name or display_name, tvg_logo_url)

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
    """
    读取多段标题的CSV格式TXT文件，跳过空行和包含 #genre# 的标题行
    返回频道列表，每项是 dict
    """
    channels = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "#genre#" in line:
                    continue  # 跳过空行和标题行
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

    print(f"\n✅ 过滤有效频道: {len(valid_channels)} 条，有效 URL 去重后")
    print(f"跳过无效或重复频道: {len(skipped_channels)} 条")

    # 写 M3U
    with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in valid_channels:
            display_name = ch["display_name"]
            url = ch["url"]
            f.write(f'#EXTINF:-1,{display_name}\n{url}\n')

    # 写 CSV，表头为中文，utf-8无BOM
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["频道名", "地址", "来源", "图标"])
        for ch in valid_channels:
            writer.writerow([ch["display_name"], ch["url"], "网络源", ch.get("logo", "")])

    # 写跳过日志
    with open(SKIPPED_LOG, "w", encoding="utf-8") as f:
        for ch in skipped_channels:
            f.write(f"{ch['display_name']},{ch['url']}\n")

    print(f"📁 输出文件：{OUTPUT_M3U} 和 {OUTPUT_CSV}")
    print(f"📁 跳过日志：{SKIPPED_LOG}")

def merge_all_sources():
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
    channels = merge_all_sources()
    if channels:
        write_output_files(channels)
    else:
        print("⚠️ 没有读取到任何频道")