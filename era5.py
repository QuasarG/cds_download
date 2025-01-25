import cdsapi
import requests
from tqdm import tqdm
import os
import subprocess
from datetime import datetime, timedelta
import time

# CDS API客户端配置
dataset = "reanalysis-era5-land"
request_template = {
    "format": "zip",  # 明确指定zip格式
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure"
    ],
    "time": [f"{hour:02d}:00" for hour in range(24)],
}

# 路径配置
install_directory = "G:\\data_from_era5"
downloaded_file = os.path.join(install_directory, "downloaded_dates.txt")
idm_path = r"D:\Internet Download Manager\idman.exe"

# 初始化CDS客户端
client = cdsapi.Client()


def load_downloaded_dates():
    """加载已下载日期记录"""
    downloaded_dates = set()
    if os.path.exists(downloaded_file):
        with open(downloaded_file, 'r') as f:
            downloaded_dates = set(f.read().splitlines())
    return downloaded_dates


def save_downloaded_date(date_str):
    """保存已下载日期"""
    with open(downloaded_file, 'a') as f:
        f.write(date_str + '\n')


def get_month_days(year, month):
    """获取月份实际天数"""
    next_month = datetime(year, month, 28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).day


def download_with_idm(url, output_path):
    """使用IDM下载文件（显式指定.zip扩展名）"""
    try:
        args = [
            idm_path,
            '/d', url,  # 下载URL
            '/p', install_directory,  # 保存目录
            '/f', os.path.basename(output_path),  # 强制.zip文件名
            '/n',  # 不显示确认对话框
            '/s'  # 立即开始下载
        ]
        subprocess.run(
            args,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"\nIDM下载任务已创建: {os.path.basename(output_path)}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nIDM调用失败: {e.stderr}")
        return False


def verify_download(file_path, timeout=1800):
    """增强版文件验证（检查文件扩展名和大小）"""
    start_time = time.time()
    expected_ext = ".zip"

    while time.time() - start_time < timeout:
        try:
            # 检查文件是否存在且扩展名正确
            if os.path.exists(file_path):
                if not file_path.lower().endswith(expected_ext):
                    print(f"文件扩展名异常: {file_path}")
                    return False

                # 检查文件是否正在被写入
                initial_size = os.path.getsize(file_path)
                time.sleep(5)
                current_size = os.path.getsize(file_path)

                if current_size == initial_size and current_size > 1024:  # 确保最小文件尺寸
                    return True
        except FileNotFoundError:
            pass
        time.sleep(10)

    print(f"文件验证超时: {file_path}")
    return False


def main():
    # 创建下载目录
    os.makedirs(install_directory, exist_ok=True)

    # 加载已下载日期
    downloaded_dates = load_downloaded_dates()

    # 时间范围配置
    start_year = 1990
    end_year = 2000

    for year in tqdm(range(start_year, end_year + 1), desc="年份进度"):
        for month in tqdm(range(1, 13), desc=f"{year}年月份进度", leave=False):
            # 获取实际月份天数
            days = get_month_days(year, month)

            for day in tqdm(range(1, days + 1), desc=f"{year}-{month:02d}日期进度", leave=False):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str in downloaded_dates:
                    continue

                # 构建请求参数
                request = request_template.copy()
                request.update({
                    "year": str(year),
                    "month": f"{month:02d}",
                    "day": f"{day:02d}",
                })

                try:
                    # 获取下载链接
                    result = client.retrieve(dataset, request)
                    url = result.location
                    print(f"\n生成的下载链接: {url}")  # 调试输出

                    # 生成标准文件名
                    output_path = os.path.join(install_directory, f"{date_str}.zip")

                    # 启动下载
                    if download_with_idm(url, output_path):
                        # 验证下载
                        if verify_download(output_path):
                            save_downloaded_date(date_str)
                            print(f"✅ 成功下载并验证: {date_str}.zip")
                        else:
                            print(f"❌ 下载验证失败: {date_str}")
                            # 删除不完整文件
                            if os.path.exists(output_path):
                                os.remove(output_path)
                except Exception as e:
                    print(f"处理 {date_str} 时发生错误: {str(e)}")
                    time.sleep(60)  # 错误后等待


if __name__ == "__main__":
    main()