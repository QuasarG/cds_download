import os
import time
from urllib.parse import quote

# 配置
idm_path = r"D:\Internet Download Manager\IDMan.exe"  # IDM 安装路径
links_file = "merra2.txt"  # 包含下载链接的文本文件
download_folder = r"F:\merra2"  # 下载文件保存路径

# 检查 IDM 路径是否存在
if not os.path.exists(idm_path):
    print(f"错误：未找到 IDM 可执行文件，请检查路径：{idm_path}")
    exit()

# 读取链接文件
if not os.path.exists(links_file):
    print(f"错误：未找到链接文件：{links_file}")
    exit()

with open(links_file, "r") as file:
    links = file.read().splitlines()

# 遍历链接并添加到 IDM
for link in links:
    if not link.strip():  # 跳过空行
        continue
    # 对 URL 进行编码处理
    encoded_link = quote(link, safe=":/?=&%")
    # 构造 IDM 命令行
    command = f'"{idm_path}" /d "{encoded_link}" /p "{download_folder}" /n'
    print(f"添加下载任务：{link}")
    os.system(command)
    time.sleep(1)  # 避免过快添加任务

print("所有链接已添加到 IDM 下载队列。")