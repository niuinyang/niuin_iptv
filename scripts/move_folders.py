#!/usr/bin/env python3
import os
import shutil

def move_folder(src, dst):
    if not os.path.exists(src):
        print(f"源文件夹不存在：{src}")
        return False

    # 目标完整路径是 dst + basename(src)
    dest_folder = os.path.join(dst, os.path.basename(src))
    
    # 如果目标已存在，删除它
    if os.path.exists(dest_folder):
        try:
            shutil.rmtree(dest_folder)
        except Exception as e:
            print(f"删除已存在目标文件夹失败：{dest_folder}，错误：{e}")
            return False

    try:
        shutil.move(src, dst)
        print(f"成功移动：{src} -> {dest_folder}")
        return True
    except Exception as e:
        print(f"移动失败：{src} -> {dest_folder}，错误：{e}")
        return False

def main():
    # 你想移动 input/network 到 else/network
    src_path = "input/network"
    dst_path = "else"

    # 确保目标父目录存在
    if not os.path.exists(dst_path):
        os.makedirs(dst_path)

    move_folder(src_path, dst_path)

if __name__ == "__main__":
    main()
