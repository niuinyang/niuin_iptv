#!/usr/bin/env python3
import csv
import os
import re
import sys
import aiohttp
import asyncio
import math
from collections import defaultdict
from urllib.parse import urlparse, urlunparse
from pathlib import Path

# --- 配置 ---
CSV_INPUT = "output/total_final.csv"
UNISTREAM_URL = "https://raw.githubusercontent.com/plsy1/iptv/refs/heads/main/unicast.m3u"
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
SOURCE_PRIORITY_DXL = ["济南联通", "电信组播", "电信单播", "青岛联通", "网络源"]
SOURCE_PRIORITY_SJMZ = ["济南移动", "济南联通", "电信组播", "电信单播", "青岛联通", "网络源"]

# 过滤来源
FILTER_SOURCE = ["上海移动"]

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
        # 中文转拼音
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


def parse_resolution(res: str):
    """解析分辨率字符串，返回宽高及面积，格式如 '1920x1080' """
    if not res:
        return 0, 0, 0
    m = re.match(r'(\d+)[xX](\d+)', res)
    if m:
        w, h = int(m.group(1)), int(m.group(2))
        return w, h, w * h
    return 0, 0, 0


async def fetch_unicast():
    """异步下载远程unicast.m3u并解析为映射 address -> extinf_line"""
    async with aiohttp.ClientSession() as session:
        async with session.get(UNISTREAM_URL) as resp:
            if resp.status != 200:
                print(f"无法下载unicast.m3u，HTTP状态码: {resp.status}")
                return {}
            text = await resp.text(encoding='utf-8')

    lines = text.splitlines()
    mapping = {}
    prev_line = ""
    for line in lines:
        if line.startswith("#EXTINF"):
            prev_line = line
        elif line.strip() and not line.startswith("#"):
            # 播放地址
            mapping[line.strip()] = prev_line
    return mapping


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
    """生成tvg-logo字段，先尝试远程模板URL，若远程文件不存在，则用本地图标"""
    # 注意：此处不实时检测远程文件是否存在（耗时严重），实际生成m3u时先用远程模板链接
    # 可以后续加缓存或其他判断逻辑。此处为简化，只做本地文件检测替代

    # 优先远程模板链接
    url = TVG_LOGO_TEMPLATE.format(channel=channel_name)
    # 判断本地是否有图标，优先用本地文件路径（相对路径）
    local_logo = find_local_logo(channel_name, local_png_set)
    if local_logo:
        return local_logo.replace("\\", "/")  # 保证路径格式
    return url


def construct_catchup(url):
    """构造带时间参数的catchup-source URL，基于传入地址"""
    try:
        parsed = urlparse(url)
        ip = parsed.hostname
        if not ip:
            return None
        if ':' in ip:  # IPv6不处理
            return None
        ip_parts = ip.split('.')
        if len(ip_parts) == 4:
            ip_parts[-1] = '39'  # 替换最后一节为39
            new_ip = '.'.join(ip_parts)
            netloc = new_ip
            if parsed.port:
                netloc += f":{parsed.port}"
            path = parsed.path
            # 构造时间参数模板
            time_params = "?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT"
            new_url = urlunparse((
                parsed.scheme,
                netloc,
                path,
                '',  # params
                time_params[1:],  # query不含问号
                ''  # fragment
            ))
            return new_url
    except Exception:
        return None
    return None


def build_extinf_line(csv_row, local_png_set, unicast_map=None):
    channel = csv_row.get("频道名", "").strip()
    url = csv_row.get("地址", "").strip()
    group = csv_row.get("分组", "").strip()
    source = csv_row.get("来源", "").strip()

    # 远程匹配优先
    if source == "济南联通" and unicast_map:
        matched_extinf = unicast_map.get(url)
        if matched_extinf:
            # 替换分组和频道名，保持远程行完整格式（包括catchup-source）
            replaced_extinf = re.sub(r'group-title="[^"]*"', f'group-title="{group}"', matched_extinf)
            replaced_extinf = re.sub(r',\s*[^,]*$', f', {channel}', replaced_extinf)
            return replaced_extinf

    # 自定义构造
    tvg_logo = make_tvg_logo(channel, local_png_set)
    extinf = f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}" tvg-logo="{tvg_logo}"'

    catchup_str = ""
    if source == "济南联通":
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
            # 过滤上海移动
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
    _, _, area = parse_resolution(row.get("分辨率", ""))
    return area


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
    # 先分组、频道名聚合
    group_dict = defaultdict(list)
    for r in rows:
        group_dict[r.get("分组", "待标准化")].append(r)

    sorted_groups = sorted(group_dict.items(), key=lambda x: group_sort_key(x[0]))

    final_sorted = []
    for group, group_rows in sorted_groups:
        # 按频道名排序
        # 先归类相同频道
        channel_dict = defaultdict(list)
        for r in group_rows:
            channel_dict[r.get("频道名", "")].append(r)

        # 频道名排序键
        sorted_channels = sorted(channel_dict.items(), key=lambda x: channel_sort_key(x[0]))

        for channel, ch_rows in sorted_channels:
            # 按来源优先级排序
            # 多来源先按来源优先级排，网络源放最后
            # 按来源拆分
            source_dict = defaultdict(list)
            for rr in ch_rows:
                source_dict[rr.get("来源", "网络源")].append(rr)

            # 网络源单独处理
            network_source_rows = source_dict.get("网络源", [])
            others_sources = [(s, lst) for s, lst in source_dict.items() if s != "网络源"]

            # 按来源优先级排序others
            others_sources_sorted = sorted(
                others_sources, 
                key=lambda x: source_sort_key(x[0], dxl=dxl)
            )

            # 同一来源内，其他来源保持原顺序，网络源内部排序
            sorted_rows = []
            for s, lst in others_sources_sorted:
                sorted_rows.extend(lst)
            if network_source_rows:
                # 网络源排序
                network_source_rows_sorted = sorted(network_source_rows, key=resolution_area_detecttime_key)
                sorted_rows.extend(network_source_rows_sorted)

            final_sorted.extend(sorted_rows)

    return final_sorted


def generate_m3u(rows, output_file, dxl=True, unicast_map=None, local_png_set=None):
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for row in rows:
            extinf_line = build_extinf_line(row, local_png_set, unicast_map)
            f.write(extinf_line + "\n")
            f.write(row.get("地址", "") + "\n")


async def main():
    print("开始下载并解析远程unicast.m3u...")
    unicast_map = await fetch_unicast()
    print(f"远程unicast.m3u解析完成，条目数：{len(unicast_map)}")

    print("读取CSV文件...")
    rows = parse_csv()
    print(f"CSV条目数(过滤上海移动后)：{len(rows)}")

    print("加载本地图标文件...")
    local_png_set = load_local_png_set()

    print("开始排序...")
    rows_dxl = sort_rows(rows, dxl=True)
    rows_sjmz = sort_rows(rows, dxl=False)

    print(f"生成{OUTPUT_DXL}...")
    generate_m3u(rows_dxl, OUTPUT_DXL, dxl=True, unicast_map=unicast_map, local_png_set=local_png_set)
    print(f"生成{OUTPUT_SJMZ}...")
    generate_m3u(rows_sjmz, OUTPUT_SJMZ, dxl=False, unicast_map=unicast_map, local_png_set=local_png_set)

    print("完成。")


if __name__ == "__main__":
    asyncio.run(main())
