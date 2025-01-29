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
install_directory = "F:\\era5"
downloaded_file = os.path.join(install_directory, "downloaded_dates.txt")  # 保持按天记录
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

def save_downloaded_dates(dates):
    """保存已下载日期"""
    with open(downloaded_file, 'a') as f:
        for date_str in dates:
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
            '/d', url,
            '/p', install_directory,
            '/f', os.path.basename(output_path),
            '/n',
            '/s'
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

def verify_download(file_path, timeout=3600):  # 延长超时时间
    """增强版文件验证"""
    start_time = time.time()
    expected_ext = ".zip"

    while time.time() - start_time < timeout:
        try:
            if os.path.exists(file_path):
                if not file_path.lower().endswith(expected_ext):
                    print(f"文件扩展名异常: {file_path}")
                    return False

                initial_size = os.path.getsize(file_path)
                time.sleep(10)  # 延长检查间隔
                current_size = os.path.getsize(file_path)

                if current_size == initial_size and current_size > 10*1024*1024:  # 最小10MB
                    return True
        except FileNotFoundError:
            pass
        time.sleep(20)

    print(f"文件验证超时: {file_path}")
    return False

def main():
    os.makedirs(install_directory, exist_ok=True)
    downloaded_dates = load_downloaded_dates()

    start_year = 1990
    end_year = 2000

    for year in tqdm(range(start_year, end_year + 1), desc="年份进度"):
        for month in tqdm(range(1, 13), desc=f"{year}年月份进度", leave=False):
            # 生成当月的所有日期列表
            days_in_month = get_month_days(year, month)
            day_list = [f"{day:02d}" for day in range(1, days_in_month+1)]
            date_list = [f"{year}-{month:02d}-{day}" for day in day_list]

            # 检查哪些日期已经下载过
            missing_dates = [date_str for date_str in date_list if date_str not in downloaded_dates]
            if not missing_dates:
                print(f"跳过已下载的月份: {year}-{month:02d}")
                continue

            # 构建请求参数，只请求未下载的日期
            request = request_template.copy()
            request.update({
                "year": str(year),
                "month": f"{month:02d}",
                "day": [date_str.split("-")[2] for date_str in missing_dates],  # 只请求缺失的日期
            })

            try:
                result = client.retrieve(dataset, request)
                url = result.location
                print(f"\n生成的下载链接: {url}")

                # 生成月文件名
                output_path = os.path.join(install_directory, f"{year}-{month:02d}_partial.zip")

                if download_with_idm(url, output_path):
                    if verify_download(output_path):
                        # 下载成功，写入缺失的日期
                        save_downloaded_dates(missing_dates)
                        print(f"✅ 成功下载并验证: {year}-{month:02d}_partial.zip")
                    else:
                        print(f"❌ 下载验证失败: {year}-{month:02d}")
                        if os.path.exists(output_path):
                            os.remove(output_path)
            except Exception as e:
                print(f"处理 {year}-{month:02d} 时发生错误: {str(e)}")
                time.sleep(120)  # 错误后延长等待时间

if __name__ == "__main__":
    main()