#!/usr/bin/env python3
import os
import shutil
import sys

def move_folder(src, dst):
    if not os.path.exists(src):
        print(f"❌ 源文件夹不存在: {src}")
        sys.exit(1)

    # 目标路径可能不存在，先创建父目录
    dst_parent = os.path.dirname(dst)
    if dst_parent and not os.path.exists(dst_parent):
        os.makedirs(dst_parent)

    # 如果目标已存在，先删除
    if os.path.exists(dst):
        print(f"⚠️ 目标文件夹已存在，删除: {dst}")
        shutil.rmtree(dst)

    try:
        shutil.move(src, dst)
        print(f"✅ 成功移动: {src} -> {dst}")
    except Exception as e:
        print(f"❌ 移动失败: {e}")
        sys.exit(2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: move_folder.py <source_folder> <destination_folder>")
        sys.exit(1)

    source = sys.argv[1]
    destination = sys.argv[2]
    move_folder(source, destination)
