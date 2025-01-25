import cdsapi
import os
import calendar
import netCDF4 as nc
import threading
from queue import Queue
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
# 创建一个函数来构建下载请求
def download_era5_data(year, month, day, download_dir):
    dataset = "derived-era5-pressure-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": ["geopotential"],
        "year": year,
        "month": [month],
        "day": [day],
        "pressure_level": [
            "300", "500", "700",
            "850"
        ],
        "daily_statistic": "daily_mean",
        "time_zone": "utc+00:00",
        "frequency": "6_hourly"
    }

    # 定义文件名格式为 年月日.nc，并设置下载路径
    filename = f"ERA5_{year}{month}{day}.nc"
    filepath = os.path.join(download_dir, filename)

    print(f"Checking if file {filename} exists and is complete...")
    # 检查文件是否已存在，且文件完整
    if os.path.exists(filepath):
        try:
            # 尝试打开文件以验证其完整性
            with nc.Dataset(filepath, 'r') as ds:
                print(f"File {filename} is complete and valid.")
        except OSError as e:
            # 如果文件不完整或损坏，删除并重新下载
            print(f"File {filename} is corrupted. Redownloading...")
            os.remove(filepath)
            download_file_from_era5(request, filepath)
    else:
        # 如果文件不存在，则直接下载
        print(f"File {filename} does not exist. Starting download...")
        download_file_from_era5(request, filepath)

# 创建一个函数来执行实际下载
def download_file_from_era5(request, filepath):
    print(f"Downloading data to {filepath}...")
    client = cdsapi.Client()
    client.retrieve("derived-era5-pressure-levels-daily-statistics", request).download(filepath)
    print(f"Download completed for {filepath}")

# 定义下载目录
download_dir = r"F:\ERA5\surface\geopotential"

print(f"Checking if download directory {download_dir} exists...")
# 检查目录是否存在，不存在则创建
if not os.path.exists(download_dir):
    print(f"Directory {download_dir} does not exist. Creating directory...")
    os.makedirs(download_dir)
else:
    print(f"Directory {download_dir} already exists.")

# 定义下载任务队列
queue = Queue()

# 创建一个下载工作线程类
class DownloadWorker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            year, month, day = self.queue.get()
            print(f"Worker {threading.current_thread().name} processing download for {year}-{month:02d}-{day:02d}...")
            try:
                # 将月份和日期格式化为两位数
                month_str = f"{month:02d}"
                day_str = f"{day:02d}"
                download_era5_data(str(year), month_str, day_str, download_dir)
            except Exception as e:
                print(f"Error downloading data for {year}-{month_str}-{day_str}: {e}")
            finally:
                print(f"Worker {threading.current_thread().name} finished processing download for {year}-{month:02d}-{day:02d}.")
                self.queue.task_done()

# 创建四个工作线程
print("Creating worker threads...")
for x in range(4):
    worker = DownloadWorker(queue)
    worker.daemon = True
    worker.start()
    print(f"Worker thread {worker.name} started.")

# 循环遍历2000到2023年，将任务加入队列
print("Adding download tasks to the queue...")
for year in range(2000, 2024):
    for month in range(1, 13):
        # 获取当前月份的最大天数
        _, max_day = calendar.monthrange(year, month)
        for day in range(1, max_day + 1):
            print(f"Adding task for {year}-{month:02d}-{day:02d} to the queue...")
            queue.put((year, month, day))

# 等待所有任务完成
print("Waiting for all tasks to complete...")
queue.join()
print("All download tasks completed.")

