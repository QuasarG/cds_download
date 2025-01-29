import cdsapi
import os
import subprocess
from datetime import datetime, timedelta
import time
import shutil
import threading
from queue import Queue

# 配置信息
dataset = "reanalysis-era5-land"
request_template = {
    "format": "zip",
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure"
    ],
    "time": [f"{hour:02d}:00" for hour in range(24)],
}

# 路径配置
install_directory = "F:\\era5"
min_disk_space = 10 * 1024 ** 3  # 10GB
downloaded_file = os.path.join(install_directory, "downloaded_dates.txt")
idm_path = r"D:\Internet Download Manager\idman.exe"

# 全局状态管理
download_queue = Queue()
active_downloads = {}
lock = threading.Lock()
client = cdsapi.Client()


def load_downloaded_dates():
    """加载已下载日期记录"""
    downloaded = set()
    if os.path.exists(downloaded_file):
        with open(downloaded_file, 'r') as f:
            downloaded = set(f.read().splitlines())
    return downloaded


def save_download_date(date_str):
    """原子化保存单个日期"""
    with lock:
        with open(downloaded_file, 'a') as f:
            f.write(date_str + '\n')


def scan_existing_files():
    """扫描现有文件并更新下载记录"""
    downloaded = load_downloaded_dates()
    new_dates = set()

    # 扫描按天下载的文件
    for filename in os.listdir(install_directory):
        if filename.endswith(".zip") and len(filename) == 14:
            try:
                date_str = filename[:10]
                datetime.strptime(date_str, "%Y-%m-%d")
                new_dates.add(date_str)
            except:
                continue

    # 合并新发现的日期
    added = new_dates - downloaded
    if added:
        print(f"发现 {len(added)} 个未记录日期，正在更新记录文件...")
        with open(downloaded_file, 'a') as f:
            for date in sorted(added):
                f.write(date + '\n')

    return downloaded.union(new_dates)


def get_month_days(year, month):
    """计算月份天数"""
    next_month = datetime(year, month, 28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).day


def get_download_dir(required_space):
    """获取可用下载目录"""
    try:
        usage = shutil.disk_usage(install_directory)
        if usage.free - required_space > min_disk_space:
            return install_directory
        print(f"⚠️ 磁盘空间不足 {usage.free / 1024 ** 3:.1f}GB < {required_space / 1024 ** 3:.1f}GB")
        return None
    except Exception as e:
        print(f"目录访问错误: {str(e)}")
        return None


def idm_downloader():
    """IDM下载线程"""
    while True:
        task = download_queue.get()
        if task is None:
            break

        year, month, days, url = task
        try:
            # 计算所需空间
            response = client.session.head(url)
            file_size = int(response.headers.get('Content-Length', 50 * 1024 ** 2))
            required_space = file_size + 500 * 1024 ** 2  # 500MB缓冲

            # 获取下载目录
            target_dir = get_download_dir(required_space)
            if not target_dir:
                print(f"空间不足，跳过 {year}-{month:02d}")
                continue

            filename = f"{year}-{month:02d}_partial.zip"
            output_path = os.path.join(target_dir, filename)

            # 启动下载
            subprocess.run(
                [
                    idm_path,
                    '/d', url,
                    '/p', target_dir,
                    '/f', filename,
                    '/n', '/s'
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                shell=True
            )

            # 记录活跃下载
            with lock:
                active_downloads[output_path] = {
                    'start_time': time.time(),
                    'dates': [f"{year}-{month:02d}-{d:02d}" for d in days],
                    'verified': False
                }
            print(f"✅ 已提交 {filename} 到下载队列")

        except Exception as e:
            print(f"❌ 下载提交失败 {year}-{month:02d}: {str(e)}")
        finally:
            download_queue.task_done()


def download_monitor():
    """下载状态监控线程"""
    while True:
        with lock:
            items = list(active_downloads.items())

        for path, info in items:
            try:
                if not os.path.exists(path):
                    if time.time() - info['start_time'] > 3600:  # 1小时超时
                        print(f"⌛ 下载超时 {path}")
                        del active_downloads[path]
                    continue

                # 检查文件稳定性
                size1 = os.path.getsize(path)
                time.sleep(30)
                size2 = os.path.getsize(path)

                if size1 == size2 and size1 > 50 * 1024 ** 2:  # 50MB最小
                    print(f"✅ 验证完成 {os.path.basename(path)}")
                    for date_str in info['dates']:
                        save_download_date(date_str)
                    del active_downloads[path]
                elif size1 != size2:
                    print(f"📥 正在下载 {os.path.basename(path)} ({size2 / 1024 ** 2:.1f}MB)")
                else:
                    print(f"⚠️ 文件异常 {os.path.basename(path)}")
                    os.remove(path)
                    del active_downloads[path]

            except Exception as e:
                print(f"监控异常 {path}: {str(e)}")

        time.sleep(60)


def generate_tasks(start_year, end_year):
    """生成下载任务"""
    downloaded = scan_existing_files()

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            days_in_month = get_month_days(year, month)
            missing_days = []

            for day in range(1, days_in_month + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str not in downloaded:
                    missing_days.append(day)

            if not missing_days:
                continue

            try:
                # 创建CDS请求
                request = request_template.copy()
                request.update({
                    "year": str(year),
                    "month": f"{month:02d}",
                    "day": [f"{d:02d}" for d in missing_days]
                })

                # 获取下载链接
                result = client.retrieve(dataset, request)
                download_queue.put((year, month, missing_days, result.location))
                print(f"已创建任务 {year}-{month:02d}")

            except Exception as e:
                print(f"任务创建失败 {year}-{month:02d}: {str(e)}")
                time.sleep(60)


def main():
    # 初始化环境
    os.makedirs(install_directory, exist_ok=True)

    # 启动工作线程
    idm_thread = threading.Thread(target=idm_downloader)
    monitor_thread = threading.Thread(target=download_monitor)

    idm_thread.start()
    monitor_thread.start()

    # 生成下载任务
    generate_tasks(2001, 2019)

    # 等待队列完成
    download_queue.join()

    # 清理线程
    download_queue.put(None)
    idm_thread.join()
    monitor_thread.join()


if __name__ == "__main__":
    main()