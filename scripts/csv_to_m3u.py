#!/usr/bin/env python3
"""
iptv_merge.py
合并并生成 dxl.m3u, sjmz.m3u, total.m3u

输入：
- input/mysource/my_sum.csv    （自有源，无表头，列：name, group, url, source）
- output/working.csv           （网络源，包含 original_name, url, source, logo 等）
- input/channel.csv           （可选的本地标准化缓存）

输出：
- output/dxl.m3u
- output/sjmz.m3u
- output/total.m3u
- 更新 input/channel.csv（若有新增匹配）
"""

import os
import re
import json
import sys
import csv
import shutil
from pathlib import Path
from collections import defaultdict, OrderedDict

# external deps
import pandas as pd
from slugify import slugify
from pypinyin import lazy_pinyin

ROOT = Path('.')
INPUT_MY = ROOT / 'input' / 'mysource' / 'my_sum.csv'
INPUT_CHANNEL_CACHE = ROOT / 'input' / 'channel.csv'
WORKING_NET = ROOT / 'output' / 'working.csv'
IPTV_REPO_DIR = ROOT / 'iptv-org-database'  # clone target
IPTV_DATA_DIR = IPTV_REPO_DIR / 'data' / 'channels'

OUTPUT_DIR = ROOT / 'output'
DXL_OUT = OUTPUT_DIR / 'dxl.m3u'
SJMZ_OUT = OUTPUT_DIR / 'sjmz.m3u'
TOTAL_OUT = OUTPUT_DIR / 'total.m3u'

M3U_HEADER = '#EXTM3U url-tvg="https://raw.githubusercontent.com/plsy1/epg/main/e/seven-days.xml.gz"'

# group order you specified (11 groups)
GROUP_ORDER = [
    "央视频道", "4K频道", "卫视频道", "山东频道", "他省频道",
    "台湾频道", "香港频道", "澳门频道", "国际频道", "数字频道", "广播频道", "其他频道"
]

# CCTV order mapping for special sorting inside 央视频道
CCTV_ORDER = {f"cctv{n}": n for n in range(1, 100)}  # cctv1..cctv99 mapping

def normalize_key(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    s = slugify(s, lowercase=True)
    return s

def chinese_sort_key(s: str):
    # return pinyin join for sorting Chinese strings roughly by pinyin
    if not isinstance(s, str):
        return s
    return ''.join(lazy_pinyin(s))

def read_my_sum():
    if not INPUT_MY.exists():
        print(f"⚠️ Missing {INPUT_MY}; exiting.")
        return pd.DataFrame(columns=["name","group","url","source"])
    # my_sum.csv has no header
    df = pd.read_csv(INPUT_MY, header=None, names=["name","group","url","source"], dtype=str, encoding='utf-8', keep_default_na=False)
    return df

def read_working():
    if not WORKING_NET.exists():
        print(f"⚠️ Missing {WORKING_NET}; creating empty.")
        return pd.DataFrame(columns=["standard_name","", "url","source","original_name","logo"])
    # try to read with pandas; handle various column orders
    df = pd.read_csv(WORKING_NET, dtype=str, encoding='utf-8', keep_default_na=False)
    # ensure expected columns exist
    expected = ["original_name","url","source","logo","standard_name"]
    for c in expected:
        if c not in df.columns:
            df[c] = ""
    return df

def load_channel_cache():
    if not INPUT_CHANNEL_CACHE.exists():
        return pd.DataFrame(columns=["standard_key","standard_name","country","region","category","logo","aliases"])
    df = pd.read_csv(INPUT_CHANNEL_CACHE, dtype=str, encoding='utf-8', keep_default_na=False)
    # expected columns: standard_key, standard_name, country, region, category, logo, aliases
    for c in ["standard_key","standard_name","country","region","category","logo","aliases"]:
        if c not in df.columns:
            df[c] = ""
    return df

def save_channel_cache(df):
    dirp = INPUT_CHANNEL_CACHE.parent
    dirp.mkdir(parents=True, exist_ok=True)
    df.to_csv(INPUT_CHANNEL_CACHE, index=False, encoding='utf-8')

def fetch_iptv_org():
    """
    Attempt to clone or update iptv-org/database into IPTV_REPO_DIR.
    We'll not fail if network not allowed; this is best-effort.
    """
    if IPTV_REPO_DIR.exists():
        print("🔁 Updating iptv-org/database (if available)...")
        os.system(f"git -C {IPTV_REPO_DIR} pull --quiet || true")
    else:
        print("⬇️ Cloning iptv-org/database (best-effort)...")
        os.system(f"git clone --depth 1 https://github.com/iptv-org/database.git {IPTV_REPO_DIR} || true")

def build_iptv_lookup():
    """
    Build a lookup dict from iptv-org data:
    key -> info dict
    key = normalized slug of channel name
    We'll try to read data/channels.csv if present, else parse data/channels/*.json
    """
    lookup = {}
    csv_path = IPTV_REPO_DIR / 'data' / 'channels.csv'
    json_dir = IPTV_REPO_DIR / 'data' / 'channels'
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding='utf-8', keep_default_na=False)
            for _, r in df.iterrows():
                name = r.get('name') or ''
                key = normalize_key(name)
                lookup[key] = {
                    "standard_name": name,
                    "country": r.get('country', ''),
                    "region": r.get('region', ''),
                    "category": r.get('category', ''),
                    "logo": r.get('logo', '')
                }
        except Exception as e:
            print("⚠️ Failed to load channels.csv:", e)
    elif json_dir.exists():
        for jf in json_dir.glob('*.json'):
            try:
                j = json.loads(jf.read_text(encoding='utf-8'))
                name = j.get('name','')
                key = normalize_key(name)
                lookup[key] = {
                    "standard_name": name,
                    "country": j.get('country', ''),
                    "region": j.get('region', ''),
                    "category": j.get('category', ''),
                    "logo": j.get('logo', '')
                }
                # also add aliases
                for alias in j.get('alternate_names', []) + j.get('aliases', []):
                    lookup[normalize_key(alias)] = lookup[key]
            except Exception:
                continue
    else:
        print("ℹ️ iptv-org data not found locally.")
    return lookup

def standardize(name: str, cache_df: pd.DataFrame, iptv_lookup: dict):
    """
    Return a dict with keys:
    standard_key, standard_name, country, region, category, logo
    """
    nk = normalize_key(name)
    # check cache by standard_key or standard_name or aliases
    if not cache_df.empty:
        # direct key match
        m = cache_df[cache_df['standard_key'] == nk]
        if not m.empty:
            r = m.iloc[0]
            return {
                "standard_key": r['standard_key'] or nk,
                "standard_name": r['standard_name'] or name,
                "country": r.get('country',''),
                "region": r.get('region',''),
                "category": r.get('category',''),
                "logo": r.get('logo','')
            }
        # match by alias substring
        ali = cache_df[cache_df['aliases'].str.contains(nk, na=False)]
        if not ali.empty:
            r = ali.iloc[0]
            return {
                "standard_key": r['standard_key'] or nk,
                "standard_name": r['standard_name'] or name,
                "country": r.get('country',''),
                "region": r.get('region',''),
                "category": r.get('category',''),
                "logo": r.get('logo','')
            }
    # try iptv-org lookup
    if iptv_lookup:
        if nk in iptv_lookup:
            info = iptv_lookup[nk]
            return {
                "standard_key": nk,
                "standard_name": info.get('standard_name', name),
                "country": info.get('country',''),
                "region": info.get('region',''),
                "category": info.get('category',''),
                "logo": info.get('logo','')
            }
    # fallback: return normalized key only
    return {
        "standard_key": nk,
        "standard_name": name,
        "country": "",
        "region": "",
        "category": "",
        "logo": ""
    }

def detect_jinan_unicom(url: str, source_field: str) -> bool:
    if not isinstance(url, str):
        return False
    if not isinstance(source_field, str):
        source_field = ""
    flags = ["济南联通", "津南联通", "jinan unicom", "jinan"]
    if any(x in source_field for x in flags):
        # require rtsp import pattern
        if url.startswith("rtsp://") and "/import/Tvod/" in url and re.search(r"ch\d+\.rsc", url):
            return True
    # additionally, sometimes source_field empty; detect by url pattern but be conservative
    if url.startswith("rtsp://") and "/import/Tvod/" in url and re.search(r"ch\d+\.rsc/\d+_Uni\.sdp", url):
        # treat as Jinan Unicom-like
        return True
    return False

def build_catchup_source(url: str) -> str:
    """
    Convert:
    rtsp://IP:1554/iptv/import/Tvod/iptv/001/001/ch<ID>.rsc/<file>_Uni.sdp
    =>
    rtsp://IP:1554/iptv/Tvod/iptv/001/001/ch<ID>.rsc?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT
    """
    m = re.match(r"(rtsp://[^/]+)(/iptv)/import/Tvod(.*?/ch\d+\.rsc)(?:/[^/]+_Uni\.sdp)?", url)
    if not m:
        # try alternate simpler match
        m2 = re.match(r"(rtsp://[^/]+)(/iptv/import/Tvod/.*?/ch\d+\.rsc)", url)
        if m2:
            prefix = m2.group(1)
            chpath = m2.group(2).replace("/import/Tvod", "/Tvod", 1)
            return f"{prefix}{chpath}?tvdr={{utc:YmdHMS}}GMT-{{utcend:YmdHMS}}GMT"
        return ""
    prefix = m.group(1)
    chpath = m.group(3)
    return f"{prefix}/iptv/Tvod{chpath}?tvdr={{utc:YmdHMS}}GMT-{{utcend:YmdHMS}}GMT"

def determine_group(row):
    # 1) if local group specified and in our mapping priority, use it
    g = (row.get('group') or "").strip()
    # direct mapping: if contains recognized words -> map to same
    direct = ["央视频道","4K频道","卫视频道","山东频道","数字频道","电台广播"]
    if g in direct:
        if g == "电台广播":
            return "广播频道"
        return g
    # 2) if country/region fields exist from standardization, use them
    country = (row.get('country') or "").lower()
    region = (row.get('region') or "").lower()
    # specific region based groups
    if "hong" in country or "hong kong" in country or "香港" in country or "hongkong" in country:
        return "香港频道"
    if "taiwan" in country or "台湾" in country:
        return "台湾频道"
    if "macao" in country or "macau" in country or "澳门" in country:
        return "澳门频道"
    # international countries -> 国际频道 (common list)
    internationals = ["us","uk","usa","united kingdom","united states","canada","japan","jp","korea","kr","australia"]
    if any(x in country for x in internationals):
        return "国际频道"
    # if country indicates China but not matched in local, treat as 他省频道
    if "china" in country or "china" in region or country in ("cn","中国"):
        return "他省频道"
    # default
    return "其他频道"

def source_sort_key(source_name: str, mode: str):
    # source_name is the 'source' field like '济南联通','电信组播','电信单播','上海移动','青岛联通','网络源','济南移动'
    order_dxl = ["济南联通", "电信组播", "上海移动", "电信单播", "青岛联通", "网络源"]
    order_sjmz = ["济南联通", "济南移动", "上海移动", "电信组播", "电信单播", "青岛联通", "网络源"]
    order = order_dxl if mode == 'dxl' else order_sjmz
    # try to find best match by keyword
    if not isinstance(source_name, str):
        return len(order)
    for idx, k in enumerate(order):
        if k and k in source_name:
            return idx
    # fallback: network source last
    if "网络" in source_name or "net" in source_name.lower():
        return len(order) - 1
    return len(order)

def cctv_sort_key(standard_key: str, display_name: str):
    """
    For CCTV channels, try to put cctv1..cctv17 in order.
    standard_key like 'cctv1' or display_name 'CCTV1' or '央视一套'
    """
    k = standard_key.lower()
    # try to find cctvN in key or display_name
    m = re.search(r"cctv(\d+)", k)
    if not m:
        m = re.search(r"cctv(\d+)", display_name.lower())
    if m:
        return (0, int(m.group(1)))
    # else use textual pinyin key
    return (1, chinese_sort_key(display_name))

def group_sorter_key(row):
    # returns tuple for sorting groups and then within group
    group_idx = GROUP_ORDER.index(row.get('final_group')) if row.get('final_group') in GROUP_ORDER else len(GROUP_ORDER)
    # within group: use CCTV order specially
    cctv_key = cctv_sort_key(row.get('standard_key',''), row.get('standard_name',''))
    # fallback name sort (pinyin)
    name_key = chinese_sort_key(row.get('standard_name') or row.get('name') or "")
    return (group_idx, cctv_key, name_key)

def build_entries(df_all):
    """
    df_all: list of dict records with fields:
    name, group, url, source, standard_key, standard_name, country, region, category, logo
    """
    # group them by final_group then by standard_key then by display name
    grouped = OrderedDict()
    # create a stable order by sorting by group_sorter_key at channel-level (we'll use a DataFrame)
    tmp = pd.DataFrame(df_all)
    tmp['final_group'] = tmp.apply(determine_group, axis=1)
    tmp['standard_key'] = tmp['standard_key'].fillna(tmp['name'].apply(normalize_key))
    # sort so that channels of same standard_key are adjacent
    tmp = tmp.sort_values(by=['final_group', 'standard_key', 'name'], key=lambda col: col.map(lambda v: chinese_sort_key(v) if isinstance(v,str) else v))
    # now produce entries list preserving adjacency for same standard_key
    entries = []
    for _, r in tmp.iterrows():
        rec = r.to_dict()
        entries.append(rec)
    return entries

def write_m3u(entries, mode, path_out):
    """
    entries: list of records, already sorted so same channel entries adjacent
    mode: 'dxl', 'sjmz', 'total'
    """
    lines = [M3U_HEADER]
    # We need to ensure grouping order as GROUP_ORDER; we'll build a stable ordering using final_group
    df = pd.DataFrame(entries)
    # ensure final_group exists
    if 'final_group' not in df.columns:
        df['final_group'] = df.apply(determine_group, axis=1)
    # iterate groups in order
    for group in GROUP_ORDER:
        sub = df[df['final_group'] == group]
        if sub.empty:
            continue
        # within each group, we need channels adjacent by standard_key; group by standard_key preserving order
        # preserve order by using the order in sub (which should be sorted earlier)
        for sk, block in sub.groupby('standard_key', sort=False):
            # block is multiple sources for same channel; sort sources per mode priority
            block_list = list(block.to_dict('records'))
            block_list.sort(key=lambda r: source_sort_key(r.get('source',''), mode))
            for r in block_list:
                name = r.get('standard_name') or r.get('name') or ""
                logo = r.get('logo') or ""
                url = r.get('url') or ""
                source_field = r.get('source') or ""
                ext = f'#EXTINF:-1 tvg-name="{name}" group-title="{group}" tvg-logo="{logo}"'
                # Jinan Unicom special handling: add catchup attribute if detected
                if detect_jinan_unicom(url, source_field):
                    catchup = build_catchup_source(url)
                    if catchup:
                        ext += f' catchup="default" catchup-source="{catchup}"'
                ext += f', {name}'
                # For dxl mode do not output entries whose source contains '济南移动' (as per your rule)
                if mode == 'dxl' and isinstance(source_field, str) and '济南移动' in source_field:
                    # skip
                    continue
                lines.append(ext)
                lines.append(url)
    # write file
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path_out, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"✅ Wrote {path_out}")

def main():
    print("▶ Starting iptv merge process...")
    # Ensure inputs
    df_local = read_my_sum()
    df_net = read_working()
    cache_df = load_channel_cache()

    # optionally fetch iptv-org
    fetch_iptv_org()
    iptv_lookup = build_iptv_lookup()

    # build combined records
    records = []
    # from local self-owned source: fields name, group, url, source
    for _, r in df_local.iterrows():
        rec_name = r.get('name', '') or ''
        rec_group = r.get('group', '') or ''
        rec_url = r.get('url', '') or ''
        rec_source = r.get('source', '') or ''
        std = standardize(rec_name, cache_df, iptv_lookup)
        records.append({
            "name": rec_name,
            "group": rec_group,
            "url": rec_url,
            "source": rec_source,
            "standard_key": std.get('standard_key'),
            "standard_name": std.get('standard_name'),
            "country": std.get('country'),
            "region": std.get('region'),
            "category": std.get('category'),
            "logo": std.get('logo')
        })

    # from network source: try to get original_name and url and source
    for _, r in df_net.iterrows():
        orig = ""
        if 'original_name' in r.index:
            orig = r.get('original_name','') or r.get('original','') or ''
        else:
            # fallback try some names
            orig = r.get('original', '') or r.get('name','') or ''
        url = r.get('url','') or ''
        source_field = r.get('source','') or ''
        logo = r.get('logo','') or ''
        std = standardize(orig or r.get('standard_name','') or url, cache_df, iptv_lookup)
        records.append({
            "name": orig or std.get('standard_name') or url,
            "group": "",  # network doesn't have local group hint
            "url": url,
            "source": source_field or "网络源",
            "standard_key": std.get('standard_key'),
            "standard_name": std.get('standard_name'),
            "country": std.get('country'),
            "region": std.get('region'),
            "category": std.get('category'),
            "logo": logo or std.get('logo','')
        })

    # update cache_df with any new entries discovered from records (best-effort)
    # add if standard_key not in cache
    existing_keys = set(cache_df['standard_key'].tolist()) if not cache_df.empty else set()
    new_rows = []
    for r in records:
        sk = r.get('standard_key')
        if sk and sk not in existing_keys:
            new_rows.append({
                "standard_key": sk,
                "standard_name": r.get('standard_name') or r.get('name'),
                "country": r.get('country',''),
                "region": r.get('region',''),
                "category": r.get('category',''),
                "logo": r.get('logo',''),
                "aliases": ""  # left empty for manual enrichment
            })
            existing_keys.add(sk)
    if new_rows:
        cache_df = pd.concat([cache_df, pd.DataFrame(new_rows)], ignore_index=True)
        save_channel_cache(cache_df)
        print(f"ℹ️ Updated channel cache with {len(new_rows)} new keys -> {INPUT_CHANNEL_CACHE}")

    # prepare entries (determine groups and ordering)
    # annotate final_group
    for r in records:
        r['final_group'] = determine_group(r)

    # produce entries list sorted properly: first by group order then CCTV order then pinyin
    entries_df = pd.DataFrame(records)
    # add sort keys
    entries_df['_group_idx'] = entries_df['final_group'].apply(lambda g: GROUP_ORDER.index(g) if g in GROUP_ORDER else len(GROUP_ORDER))
    # for CCTV special order
    def cctv_key_row(row):
        return cctv_sort_key(row.get('standard_key',''), row.get('standard_name',''))
    entries_df['_cctv_key'] = entries_df.apply(cctv_key_row, axis=1)
    entries_df['_name_key'] = entries_df['standard_name'].apply(lambda x: chinese_sort_key(x or ""))
    # final sort
    entries_df = entries_df.sort_values(by=['_group_idx', '_cctv_key', '_name_key'])
    entries = entries_df.to_dict('records')

    # generate m3u files
    write_m3u(entries, 'dxl', DXL_OUT)
    write_m3u(entries, 'sjmz', SJMZ_OUT)
    write_m3u(entries, 'total', TOTAL_OUT)

    print("✅ Completed all outputs.")

if __name__ == '__main__':
    main()