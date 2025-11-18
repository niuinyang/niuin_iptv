#!/usr/bin/env python3
import os
import shutil
import json

def move_folder(src, dst):
    if not os.path.exists(src):
        print(f"源文件夹不存在：{src}")
        return False
    dest_folder = os.path.join(dst, os.path.basename(src))
    if os.path.exists(dest_folder):
        shutil.rmtree(dest_folder)
    shutil.move(src, dst)
    print(f"成功移动：{src} -> {dest_folder}")
    return True

def main():
    # 你想移动的目录列表，改这里即可
    folders_to_move = [
        "png",
        "iptv-database",
        "input/network"
    ]
    target_parent = "depend"
    if not os.path.exists(target_parent):
        os.makedirs(target_parent)

    moved = []
    for folder in folders_to_move:
        if move_folder(folder, target_parent):
            moved.append(folder)

    # 写移动成功列表，供 workflow 读取
    with open("moved_folders.json", "w") as f:
        json.dump(moved, f)

if __name__ == "__main__":
    main()
