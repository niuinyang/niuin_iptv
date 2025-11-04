import os
import re
import pandas as pd
import chardet
from rapidfuzz import process

# 文件路径配置
INPUT_MY = "input/mysource/my_sum.csv"
INPUT_WORKING = "output/working.csv"
OUTPUT_TOTAL = "output/total.csv"
OUTPUT_CHANNEL = "input/channel.csv"

IPTV_DB_PATH = "./iptv-database"

def safe_read_csv(path):
    """自动检测编码读取csv，并统一保存为utf-8编码"""
    if not os.path.exists(path):
        print(f"文件不存在: {path}")
        return None

    with open(path, "rb") as f:
        data = f.read()
        result = chardet.detect(data)
        enc = result["encoding"]

    try:
        df = pd.read_csv(path, encoding=enc)
        if enc.lower() != "utf-8":
            # 转码为utf-8覆盖原文件
            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"✅ 转码并覆盖保存为 UTF-8: {path}")
        return df
    except Exception as e:
        print(f"读取文件失败: {path}, 错误: {e}")
        return None

def load_name_map():
    """加载iptv-org数据库频道名和别名映射"""
    name_map = {}
    path = os.path.join(IPTV_DB_PATH, "data", "channels.csv")
    if not os.path.exists(path):
        print(f"iptv数据库文件不存在: {path}")
        return name_map

    with open(path, encoding="utf-8") as f:
        for row in pd.read_csv(f).itertuples():
            std_name = getattr(row, "name").strip()
            name_map[std_name.lower()] = std_name
            others = getattr(row, "other_names", "")
            if pd.isna(others):
                continue
            for alias in others.split(","):
                alias = alias.strip()
                if alias:
                    name_map[alias.lower()] = std_name
    return name_map

def clean_channel_name(name):
    """去除频道名中括号内的说明，如 (1080p)、[Geo-blocked]、[Not 24/7]"""
    # 去除中英文括号及里面内容
    cleaned = re.sub(r"[\(\[（【][^\)\]）】]*[\)\]）】]", "", name)
    return cleaned.strip()

def get_std_name(name, name_map, threshold=95):
    """先尝试精确匹配，失败则模糊匹配，匹配度低于阈值返回原名并标注"""
    name_lower = name.lower()
    if name_lower in name_map:
        return name_map[name_lower], 100.0, "精确匹配"

    choices = list(name_map.keys())
    match, score, _ = process.extractOne(name_lower, choices)
    if score >= threshold:
        return name_map[match], score, f"模糊匹配({score:.1f})"
    else:
        return name, score, f"匹配度低({score:.1f})"

def standardize_my_sum(file_path):
    """my_sum.csv不做匹配，标准化名即原名"""
    df = safe_read_csv(file_path)
    if df is None:
        return pd.DataFrame()
    df["标准频道名"] = df.iloc[:, 0].astype(str).str.strip()
    df["匹配信息"] = "自有源原名"
    return df

def standardize_working(file_path, my_df, name_map):
    """working.csv先去除括号信息，然后优先匹配my_sum，再匹配iptv-org库"""
    df = safe_read_csv(file_path)
    if df is None:
        return pd.DataFrame()

    # 去除括号字段
    df["处理频道名"] = df.iloc[:, 0].astype(str).apply(clean_channel_name)

    # 用 my_sum.csv 标准名映射，构建快速匹配字典（key是my_sum的原名，value是标准名）
    my_name_map = {name.lower(): std_name for name, std_name in zip(my_df.iloc[:, 0].str.lower(), my_df["标准频道名"])}

    std_names = []
    scores = []
    notes = []

    choices_my = list(my_name_map.keys())
    choices_iptv = list(name_map.keys())

    for ch_name in df["处理频道名"]:
        ch_name_lower = ch_name.lower()

        # 优先尝试自有源my_sum匹配（精确+模糊）
        if ch_name_lower in my_name_map:
            std_names.append(my_name_map[ch_name_lower])
            scores.append(100.0)
            notes.append("自有源精确匹配")
            continue

        match_my = process.extractOne(ch_name_lower, choices_my)
        if match_my and match_my[1] >= 95:
            std_names.append(my_name_map[match_my[0]])
            scores.append(match_my[1])
            notes.append(f"自有源模糊匹配({match_my[1]:.1f})")
            continue

        # 自有源没匹配上，再匹配iptv-org库
        match_iptv = process.extractOne(ch_name_lower, choices_iptv)
        if match_iptv and match_iptv[1] >= 95:
            std_names.append(name_map[match_iptv[0]])
            scores.append(match_iptv[1])
            notes.append(f"iptv-org模糊匹配({match_iptv[1]:.1f})")
        else:
            std_names.append(df.iloc[:, 0].astype(str).values[len(std_names)])  # 保留原名
            scores.append(match_iptv[1] if match_iptv else 0)
            notes.append(f"匹配度低({match_iptv[1]:.1f})" if match_iptv else "无匹配")

    df["标准频道名"] = std_names
    df["匹配得分"] = scores
    df["匹配信息"] = notes

    return df

def main():
    print("🚀 开始执行标准化匹配流程...\n")
    print("读取源文件：")
    print(f"  📁 {INPUT_MY}")
    print(f"  📁 {INPUT_WORKING}\n")

    my_df = standardize_my_sum(INPUT_MY)
    name_map = load_name_map()
    print(f"📚 已加载 {len(name_map)} 个频道映射\n")

    working_df = standardize_working(INPUT_WORKING, my_df, name_map)

    # 合并两个数据框，my_sum优先，后面是working
    total_df = pd.concat([my_df, working_df], ignore_index=True)
    total_df.to_csv(OUTPUT_TOTAL, index=False, encoding="utf-8-sig")
    print(f"✅ 已生成汇总文件: {OUTPUT_TOTAL}")

    # 输出频道名和分组两列到 input/channel.csv
    # 尽量兼容分组列，默认是第6列或叫“分组”
    group_col = None
    for col_name in total_df.columns:
        if col_name in ["分组", "group"]:
            group_col = col_name
            break
    if not group_col:
        group_col = total_df.columns[5] if len(total_df.columns) > 5 else None

    if group_col:
        channel_df = total_df[["标准频道名", group_col]]
        channel_df.to_csv(OUTPUT_CHANNEL, index=False, encoding="utf-8-sig")
        print(f"✅ 已提取频道映射: {OUTPUT_CHANNEL}")
    else:
        print("⚠️ 未找到分组列，未生成频道映射文件")

    print("\n🎉 全部处理完成！")

if __name__ == "__main__":
    main()