#!/usr/bin/env python3
import csv
import os
import re
import sys
import asyncio
from collections import defaultdict
from urllib.parse import urlparse, urlunparse

# --- 配置 ---
CSV_INPUT = "output/total_final.csv"
LOCAL_PNG_DIR = "depend/png"
OUTPUT_DXL = "output/dxl.m3u"
OUTPUT_SJMZ = "output/sjmz.m3u"

# 分组优先级
GROUP_PRIORITY = [
    "央视频道",
    "4K频道",
    "卫视频道",
    "国际频道",
    "山东频道",
    "他省频道",
    "数字频道",
    "国际频道（小语种）",
    "待确认分组",
    "待标准化"
]

# 来源优先级
SOURCE_PRIORITY_DXL = ["济南联通", "电信组播", "济南联通组播", "电信单播", "青岛联通", "网络源"]
SOURCE_PRIORITY_SJMZ = ["济南移动", "济南联通", "电信组播", "济南联通组播", "电信单播", "青岛联通", "网络源"]

# 过滤来源
FILTER_SOURCE = []  # 取消过滤上海移动，保持全部来源

# 默认tv-logo模板链接
TVG_LOGO_TEMPLATE = "https://raw.githubusercontent.com/plsy1/iptv/main/logo/{channel}.png"

# 用于拼音排序
try:
    from pypinyin import lazy_pinyin
except ImportError:
    print("缺少 pypinyin 库，请运行: pip install pypinyin")
    sys.exit(1)


def get_pinyin_key(name: str):
    """获取拼音排序关键字"""
    if re.search(r'[\u4e00-\u9fff]', name):
        return ''.join(lazy_pinyin(name)).lower()
    else:
        return name.lower()


def split_alpha_num(text: str):
    """将文本分成字母部分和数字部分，方便排序"""
    match = re.match(r'^([a-zA-Z]+)(\d*)', text)
    if match:
        alpha = match.group(1).lower()
        num = int(match.group(2)) if match.group(2) else 0
        return (alpha, num)
    return (text.lower(), 0)


def load_local_png_set():
    """扫描本地png目录，返回所有文件名(无后缀小写)集合"""
    if not os.path.exists(LOCAL_PNG_DIR):
        return set()
    files = os.listdir(LOCAL_PNG_DIR)
    pngs = set()
    for f in files:
        if f.lower().endswith(".png"):
            pngs.add(os.path.splitext(f)[0].lower())
    return pngs


def find_local_logo(channel_name, local_png_set):
    """根据频道名找本地图标文件名，忽略大小写"""
    name_lower = channel_name.lower()
    if name_lower in local_png_set:
        return os.path.join(LOCAL_PNG_DIR, channel_name + ".png")
    # 模糊匹配：忽略非字母数字的比较
    pattern = re.compile(r'[^a-z0-9]')
    norm_name = pattern.sub('', name_lower)
    for png_name in local_png_set:
        norm_png = pattern.sub('', png_name)
        if norm_png == norm_name:
            return os.path.join(LOCAL_PNG_DIR, png_name + ".png")
    return None


def make_tvg_logo(channel_name, local_png_set):
    """生成 tvg-logo 字段，先尝试远程模板URL，若本地有则用本地"""
    url = TVG_LOGO_TEMPLATE.format(channel=channel_name)
    local_logo = find_local_logo(channel_name, local_png_set)
    if local_logo:
        return local_logo.replace("\\", "/")
    return url


def construct_catchup(url):
    """
    构造 catchup-source URL，严格按照范例逻辑：
    - IP 地址保持和播放地址一致
    - 去掉路径中的 /import 片段
    - 截断路径，保留到第一个以 .rsc 结尾的位置
    - 在URL尾部添加时间参数 ?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT
    """
    try:
        parsed = urlparse(url)
        ip = parsed.hostname
        if not ip:
            return None
        if ':' in ip:  # IPv6 不处理
            return None

        path = parsed.path
        # 去除 /import 段
        if path.startswith("/iptv/import"):
            path = path.replace("/iptv/import", "/iptv", 1)

        # 截断路径到第一个 .rsc 结尾（包含.rsc）
        rsc_pos = path.find(".rsc")
        if rsc_pos != -1:
            path = path[:rsc_pos + 4]  # 保留 .rsc 结尾部分

        time_params = "?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT"
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            '',  # params
            time_params[1:],  # query 不带 '?'
            ''   # fragment
        ))
        return new_url
    except Exception:
        return None


def build_extinf_line(csv_row, local_png_set):
    """
    构造 #EXTINF 行，严格对应范例：
    - 仅对来源为“济南联通组播”添加 catchup 参数
    - tvg-logo 优先使用本地，无则用远程模板
    """
    channel = csv_row.get("频道名", "").strip()
    url = csv_row.get("地址", "").strip()
    group = csv_row.get("分组", "").strip()
    source = csv_row.get("来源", "").strip()

    tvg_logo = make_tvg_logo(channel, local_png_set)
    extinf = f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}" tvg-logo="{tvg_logo}"'

    catchup_str = ""
    if source == "济南联通组播":  # 仅此来源添加回看参数
        catchup_url = construct_catchup(url)
        if catchup_url:
            catchup_str = f' catchup="default" catchup-source="{catchup_url}"'

    extinf += catchup_str + f", {channel}"
    return extinf


def parse_csv():
    """读取CSV，返回列表(dict)"""
    rows = []
    with open(CSV_INPUT, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("来源", "") in FILTER_SOURCE:
                continue
            rows.append(row)
    return rows


def channel_sort_key(channel_name):
    """频道名排序键：英文字母顺序，中文拼音首字母顺序，数字按数值大小"""
    pinyin_key = get_pinyin_key(channel_name)
    alpha_num = split_alpha_num(pinyin_key)
    return alpha_num


def group_sort_key(group_name):
    """分组排序键，优先级列表顺序，未匹配放最后"""
    try:
        return GROUP_PRIORITY.index(group_name)
    except ValueError:
        return len(GROUP_PRIORITY)


def source_sort_key(source_name, dxl=True):
    """来源排序键，dxl/sjmz两种不同优先级"""
    if dxl:
        try:
            return SOURCE_PRIORITY_DXL.index(source_name)
        except ValueError:
            return len(SOURCE_PRIORITY_DXL)
    else:
        try:
            return SOURCE_PRIORITY_SJMZ.index(source_name)
        except ValueError:
            return len(SOURCE_PRIORITY_SJMZ)


def resolution_area(row):
    """计算分辨率面积"""
    m = re.match(r'(\d+)[xX](\d+)', row.get("分辨率", ""))
    if m:
        return int(m.group(1)) * int(m.group(2))
    return 0


def resolution_area_detecttime_key(row):
    """网络源排序键：分辨率面积降序，检测时间升序"""
    area = resolution_area(row)
    try:
        detect = float(row.get("检测时间", 0))
    except Exception:
        detect = 0
    return (-area, detect)


def sort_rows(rows, dxl=True):
    """
    综合排序：
    1. 分组优先
    2. 频道名排序
    3. 多来源排序（优先级）
    4. 同一来源内部网络源排序（分辨率面积+检测时间）
    5. 网络源放最后
    """
    group_dict = defaultdict(list)
    for r in rows:
        group_dict[r.get("分组", "待标准化")].append(r)

    sorted_groups = sorted(group_dict.items(), key=lambda x: group_sort_key(x[0]))

    final_sorted = []
    for group, group_rows in sorted_groups:
        channel_dict = defaultdict(list)
        for r in group_rows:
            channel_dict[r.get("频道名", "")].append(r)

        sorted_channels = sorted(channel_dict.items(), key=lambda x: channel_sort_key(x[0]))

        for channel, ch_rows in sorted_channels:
            source_dict = defaultdict(list)
            for rr in ch_rows:
                source_dict[rr.get("来源", "网络源")].append(rr)

            network_source_rows = source_dict.get("网络源", [])
            others_sources = [(s, lst) for s, lst in source_dict.items() if s != "网络源"]

            others_sources_sorted = sorted(
                others_sources,
                key=lambda x: source_sort_key(x[0], dxl=dxl)
            )

            sorted_rows = []
            for s, lst in others_sources_sorted:
                sorted_rows.extend(lst)
            if network_source_rows:
                network_source_rows_sorted = sorted(network_source_rows, key=resolution_area_detecttime_key)
                sorted_rows.extend(network_source_rows_sorted)

            final_sorted.extend(sorted_rows)

    return final_sorted


def generate_m3u(rows, output_file, dxl=True, local_png_set=None):
    """生成m3u文件"""
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for row in rows:
            extinf_line = build_extinf_line(row, local_png_set)
            f.write(extinf_line + "\n")
            f.write(row.get("地址", "") + "\n")


async def main():
    print("读取CSV文件...")
    rows = parse_csv()
    print(f"CSV条目数(过滤来源后)：{len(rows)}")

    print("加载本地图标文件...")
    local_png_set = load_local_png_set()

    print("开始排序...")
    rows_dxl = sort_rows(rows, dxl=True)
    rows_sjmz = sort_rows(rows, dxl=False)

    print(f"生成{OUTPUT_DXL}...")
    generate_m3u(rows_dxl, OUTPUT_DXL, dxl=True, local_png_set=local_png_set)
    print(f"生成{OUTPUT_SJMZ}...")
    generate_m3u(rows_sjmz, OUTPUT_SJMZ, dxl=False, local_png_set=local_png_set)

    print("完成。")


if __name__ == "__main__":
    asyncio.run(main())
