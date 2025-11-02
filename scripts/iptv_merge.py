import csv
import re
import os
from collections import defaultdict
from functools import cmp_to_key

# === 参数 ===
MY_SUM_PATH = 'input/mysource/my_sum.csv'
NETWORK_PATH = 'output/working.csv'
CHANNEL_CACHE_PATH = 'input/channel.csv'

# 输出文件
DXL_PATH = 'output/dxl.m3u'
SJMZ_PATH = 'output/sjmz.m3u'
TOTAL_PATH = 'output/total.m3u'

# IPTV-org数据库示例（实际需用API或数据库文件替换）
# 这里只是示范：key=标准化频道名，值为字典
iptv_org_db = {
    'cctv1': {'country': '中国', 'region': '', 'category': '央视频道', 'logo': 'https://raw.githubusercontent.com/plsy1/iptv/main/logo/CCTV1.png'},
    'cctv2': {'country': '中国', 'region': '', 'category': '央视频道', 'logo': 'https://raw.githubusercontent.com/plsy1/iptv/main/logo/CCTV2.png'},
    'cctv12': {'country': '中国', 'region': '', 'category': '央视频道', 'logo': 'https://raw.githubusercontent.com/plsy1/iptv/main/logo/CCTV12.png'},
    'cctv16': {'country': '中国', 'region': '', 'category': '央视频道', 'logo': 'https://raw.githubusercontent.com/plsy1/iptv/main/logo/CCTV16.png'},
    # 更多频道...
}

# === 工具函数 ===
def normalize_channel_name(name):
    # 标准化频道名为小写、去空格、去特殊字符等
    if not name:
        return ''
    return re.sub(r'[^a-z0-9]', '', name.lower())

def load_csv_dict(filepath, key_index=0):
    """加载csv，返回dict，key是key_index列，value是整行字典"""
    data = {}
    if not os.path.exists(filepath):
        return data
    with open(filepath, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = list(row.values())[key_index]
            data[key] = row
    return data

def load_channel_cache():
    """加载频道缓存csv，返回 dict 标准化频道名 => info dict"""
    if not os.path.exists(CHANNEL_CACHE_PATH):
        return {}
    result = {}
    with open(CHANNEL_CACHE_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            std_name = normalize_channel_name(row.get('standard_name', ''))
            if std_name:
                result[std_name] = row
    return result

def save_channel_cache(channel_cache):
    """保存频道缓存csv"""
    if not channel_cache:
        return
    keys = set()
    for v in channel_cache.values():
        keys.update(v.keys())
    keys = sorted(keys)
    with open(CHANNEL_CACHE_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        for v in channel_cache.values():
            writer.writerow(v)

# === 频道分组逻辑 ===
def determine_group(row):
    # 优先使用自有源的group字段
    g = (row.get('group') or "").strip()
    valid_groups = {
        "央视频道": "央视频道",
        "4K频道": "4K频道",
        "卫视频道": "卫视频道",
        "山东频道": "山东频道",
        "数字频道": "数字频道",
        "电台广播": "广播频道",
        "广播频道": "广播频道"
    }
    if g in valid_groups:
        return valid_groups[g]

    country = (row.get('country') or "").lower()
    region = (row.get('region') or "").lower()

    if any(x in country for x in ["taiwan", "台湾"]) or any(x in region for x in ["台湾"]):
        return "台湾频道"
    if any(x in country for x in ["hong kong", "hongkong", "香港"]) or any(x in region for x in ["香港"]):
        return "香港频道"
    if any(x in country for x in ["macau", "macao", "澳门"]) or any(x in region for x in ["澳门"]):
        return "澳门频道"
    
    internationals = ["us", "usa", "united states", "uk", "united kingdom", "canada", "japan", "jp", "korea", "kr", "australia"]
    if any(x in country for x in internationals):
        return "国际频道"

    if "china" in country or country in ("cn", "中国") or "china" in region:
        return "他省频道"

    return "其他频道"

# === 自有源加载 ===
def load_my_sum():
    result = []
    if not os.path.exists(MY_SUM_PATH):
        print(f"警告：找不到自有源文件 {MY_SUM_PATH}")
        return result
    with open(MY_SUM_PATH, encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            # 名字、分组、url、来源
            name, group, url, source = row[0], row[1], row[2], row[3]
            std_name = normalize_channel_name(name)
            result.append({
                'original_name': name,
                'group': group,
                'url': url,
                'source': source,
                'standard_name': std_name,
                'channel_type': 'my_sum',
            })
    return result

# === 网络源加载 ===
def load_network():
    result = []
    if not os.path.exists(NETWORK_PATH):
        print(f"警告：找不到网络源文件 {NETWORK_PATH}")
        return result
    with open(NETWORK_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('standard_name') or ''
            url = row.get('url') or ''
            source = row.get('source') or ''
            original_name = row.get('original_name') or ''
            logo = row.get('logo') or ''
            std_name = normalize_channel_name(name)
            result.append({
                'original_name': original_name,
                'group': '',
                'url': url,
                'source': source,
                'standard_name': std_name,
                'logo': logo,
                'channel_type': 'network',
            })
    return result

# === 频道信息标准化 ===
def enrich_channel_info(channels, channel_cache):
    """为channels列表里每个频道尝试补充国家、地区、类别、logo，优先用channel_cache，否则用iptv_org_db"""
    for ch in channels:
        std_name = ch.get('standard_name')
        info = channel_cache.get(std_name)
        if info:
            ch['country'] = info.get('country', '')
            ch['region'] = info.get('region', '')
            ch['category'] = info.get('category', '')
            ch['logo'] = ch.get('logo') or info.get('logo', '')
        else:
            iptv_info = iptv_org_db.get(std_name)
            if iptv_info:
                ch['country'] = iptv_info.get('country', '')
                ch['region'] = iptv_info.get('region', '')
                ch['category'] = iptv_info.get('category', '')
                ch['logo'] = ch.get('logo') or iptv_info.get('logo', '')
            else:
                # 没有找到，字段空
                ch['country'] = ''
                ch['region'] = ''
                ch['category'] = ''
                ch['logo'] = ch.get('logo') or ''
    return channels

# === 频道排序规则 ===

# 央视频道自定义排序顺序 (cctv1-cctv17)
CCTV_ORDER = {f'cctv{i}': i for i in range(1, 18)}

def channel_name_cmp(a, b):
    # CCTV频道按顺序排序
    a_std = a['standard_name']
    b_std = b['standard_name']
    if a_std in CCTV_ORDER and b_std in CCTV_ORDER:
        return CCTV_ORDER[a_std] - CCTV_ORDER[b_std]
    if a_std in CCTV_ORDER:
        return -1
    if b_std in CCTV_ORDER:
        return 1
    # 其他按英文名或拼音首字母排序
    a_name = a.get('original_name') or a_std
    b_name = b.get('original_name') or b_std
    return (a_name > b_name) - (a_name < b_name)

def sort_channels(channels):
    return sorted(channels, key=cmp_to_key(channel_name_cmp))

# === 分组排序 ===
GROUP_ORDER = [
    "央视频道",
    "4K频道",
    "卫视频道",
    "山东频道",
    "他省频道",
    "台湾频道",
    "香港频道",
    "澳门频道",
    "国际频道",
    "数字频道",
    "广播频道",
    "其他频道",
]

def group_sort_key(group_name):
    try:
        return GROUP_ORDER.index(group_name)
    except ValueError:
        return len(GROUP_ORDER)

# === m3u写入函数 ===

def format_extinf(ch, catchup_special=False):
    # catchup_special: 对济南联通特殊处理，时移格式
    # 例：
    # #EXTINF:-1 tvg-name="CCTV1" group-title="央视频道" tvg-logo="..." catchup="default" catchup-source="rtsp://xxx?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT", CCTV1
    # url
    name = ch.get('original_name') or ch.get('standard_name') or ''
    tvg_name = ch.get('standard_name') or ''
    group_title = ch.get('group') or ''
    logo = ch.get('logo') or ''
    url = ch.get('url') or ''
    if catchup_special and ch.get('source', '') == '济南联通':
        catchup_source = url
        # 替换成时移格式url模板，去掉url最后的某些参数部分，示范处理，这里假设rtsp类型url
        # 你的格式例子是：rtsp://.../iptv/Tvod/iptv/xxx.rsc?tvdr=...GMT-...GMT
        # 但实际url末尾你给的是.rsc/xxxx_Uni.sdp，可能需要改写成带时移参数格式
        # 简单示范用替换，确保url基础部分
        base_url = url.split('.rsc')[0] + '.rsc?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT'
        extinf = f'#EXTINF:-1 tvg-name="{tvg_name}" group-title="{group_title}" tvg-logo="{logo}" catchup="default" catchup-source="{base_url}", {name}'
        return extinf + '\n' + url
    else:
        extinf = f'#EXTINF:-1 tvg-name="{tvg_name}" group-title="{group_title}" tvg-logo="{logo}", {name}'
        return extinf + '\n' + url

# === 输出m3u文件 ===

def output_m3u(channels, filename, order_rule=None):
    """
    channels: list of dicts
    order_rule: None or list of sources排序规则列表, 用于指定频道源的顺序，如['济南联通', '电信组播', ...]
    """
    # 按频道分组排序
    channels.sort(key=lambda x: (group_sort_key(x.get('group')), x.get('standard_name')))

    # 按频道名排序分组内排序
    grouped = defaultdict(list)
    for ch in channels:
        grouped[ch.get('group')].append(ch)
    # 每组内排序
    for g in grouped:
        grouped[g] = sort_channels(grouped[g])

    with open(filename, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        # 按分组顺序输出
        for g in GROUP_ORDER:
            ch_list = grouped.get(g, [])
            # 如果有 order_rule，优先根据order_rule排序源
            if order_rule:
                def source_cmp(a, b):
                    try:
                        return order_rule.index(a['source']) - order_rule.index(b['source'])
                    except ValueError:
                        # 不在规则里的放后面
                        return 999
                ch_list.sort(key=cmp_to_key(source_cmp))
            for ch in ch_list:
                catchup_special = (ch.get('source') == '济南联通' and filename == DXL_PATH)
                f.write(format_extinf(ch, catchup_special))
                f.write('\n')

# === 主流程 ===

def main():
    # 加载频道缓存
    channel_cache = load_channel_cache()

    # 加载数据
    my_sum_channels = load_my_sum()
    network_channels = load_network()

    # 合并所有频道
    all_channels = my_sum_channels + network_channels

    # 尝试补全频道信息
    all_channels = enrich_channel_info(all_channels, channel_cache)

    # 补充分组字段
    for ch in all_channels:
        ch['group'] = determine_group(ch)

    # 保存更新后的频道缓存（含新增的标准化频道信息）
    for ch in all_channels:
        std_name = ch.get('standard_name')
        if std_name and std_name not in channel_cache:
            # 新增缓存，填充简单字段
            channel_cache[std_name] = {
                'standard_name': std_name,
                'country': ch.get('country', ''),
                'region': ch.get('region', ''),
                'category': ch.get('category', ''),
                'logo': ch.get('logo', ''),
            }
    save_channel_cache(channel_cache)

    # 分别输出3个m3u文件

    # dxl.m3u 排序规则：“济南联通、电信组播、上海移动、电信单播、青岛联通、网络源”
    dxl_order = ["济南联通", "电信组播", "上海移动", "电信单播", "青岛联通", "网络源"]
    output_m3u(all_channels, DXL_PATH, order_rule=dxl_order)

    # sjmz.m3u 排序规则：“济南联通、济南移动、上海移动、电信组播、电信单播、青岛联通、网络源”
    sjmz_order = ["济南联通", "济南移动", "上海移动", "电信组播", "电信单播", "青岛联通", "网络源"]
    output_m3u(all_channels, SJMZ_PATH, order_rule=sjmz_order)

    # total.m3u 不排序
    output_m3u(all_channels, TOTAL_PATH, order_rule=None)

    print("处理完成，生成文件：", DXL_PATH, SJMZ_PATH, TOTAL_PATH)

if __name__ == '__main__':
    main()