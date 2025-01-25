import cdsapi
import calendar
import os

c = cdsapi.Client()
dataset = "reanalysis-era5-land"
dic = {
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure",
        "total_precipitation"
    ],
    "year": "",
    "month": "",
    "day": [],
    "time": [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ],
    "data_format": "grib",
    "download_format": "zip"
}

# 创建存储目录
output_dir = 'E:\\ERA5\\1979-2021\\'
os.makedirs(output_dir, exist_ok=True)

# 批量下载2020年到2021年所有月份数据
for i in range(2020, 2022):
    for j in range(1, 13):
        day_num = calendar.monthrange(i, j)[1]  # 根据年月，获取当月日数
        dic['year'] = str(i)
        dic['month'] = str(j).zfill(2)
        dic['day'] = [str(d).zfill(2) for d in range(1, day_num + 1)]
        filename = os.path.join(output_dir, f'{dataset}{i}{str(j).zfill(2)}.nc')  # 文件存储路径

        # 添加异常处理和重试机制
        for attempt in range(3):
            try:
                c.retrieve(dataset, dic, filename)  # 下载数据
                print(f'{i}年{j}月数据下载完成')
                break
            except Exception as e:
                print(f'下载 {i}年{j}月 数据失败，尝试 {attempt + 1}/3 次: {e}')
                if attempt == 2:
                    print(f'{i}年{j}月 数据下载失败')