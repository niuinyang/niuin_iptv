#!/usr/bin/env python3
import os
import shutil

def move_folder(src, dst):
    if not os.path.exists(src):
        print(f"源文件夹不存在：{src}")
        return False
    if not os.path.exists(dst):
        os.makedirs(dst)
    try:
        # 如果目标已经有同名文件夹，先删除（可根据需求改）
        if os.path.exists(dst):
            # dst 是depend/，完整目标路径是 depend/png 或 depend/iptv-database
            dest_folder = os.path.join(dst, os.path.basename(src))
            if os.path.exists(dest_folder):
                shutil.rmtree(dest_folder)

        shutil.move(src, dst)
        print(f"成功移动：{src} -> {dst}")
        return True
    except Exception as e:
        print(f"移动失败：{src} -> {dst}，错误：{e}")
        return False

def main():
    folders_to_move = ["png", "iptv-database"]
    target_parent = "depend"

    for folder in folders_to_move:
        src_path = folder
        dst_path = target_parent
        move_folder(src_path, dst_path)

if __name__ == "__main__":
    main()
