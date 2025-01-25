import rasterio
import numpy as np
import pandas as pd


def inspect_tif(tif_path):
    """
    详细检查 TIF 文件的结构和内容
    """
    with rasterio.open(tif_path) as src:
        print("=== 基本信息 ===")
        print(f"文件大小: {src.width} x {src.height} 像素")
        print(f"波段数量: {src.count}")
        print(f"数据类型: {src.dtypes[0]}")

        print("\n=== 坐标系统信息 ===")
        print(f"坐标参考系统: {src.crs}")
        print(f"变换矩阵: {src.transform}")

        print("\n=== 数据范围 ===")
        print(f"左上角坐标: ({src.bounds.left}, {src.bounds.top})")
        print(f"右下角坐标: ({src.bounds.right}, {src.bounds.bottom})")

        print("\n=== 数据预览 ===")
        # 读取第一个波段的数据
        data = src.read(1)

        # 计算基本统计信息
        valid_data = data[data != src.nodata] if src.nodata is not None else data
        print(f"数据最小值: {valid_data.min()}")
        print(f"数据最大值: {valid_data.max()}")
        print(f"数据平均值: {valid_data.mean():.2f}")
        print(f"标准差: {valid_data.std():.2f}")

        print("\n=== 数据示例（5x5网格）===")
        # 从中心位置获取5x5的数据样本
        center_y = src.height // 2
        center_x = src.width // 2
        sample = data[center_y - 2:center_y + 3, center_x - 2:center_x + 3]

        # 创建一个DataFrame来更好地展示数据
        df = pd.DataFrame(sample)
        print(df)


def export_all_data_to_csv(tif_path, output_csv):
    """
    导出TIF文件中的所有数据到CSV文件，包含坐标信息
    """
    with rasterio.open(tif_path) as src:
        data = src.read(1)  # 读取第一个波段

        # 获取所有像素的坐标
        rows, cols = np.indices((src.height, src.width))
        rows = rows.flatten()
        cols = cols.flatten()

        # 初始化一个空列表来存储数据
        results = []

        # 总像素数
        total_pixels = len(rows)

        # 每处理10,000个数据点输出一次
        print(f"开始导出数据，总像素数: {total_pixels}")
        for i in range(total_pixels):
            row = rows[i]
            col = cols[i]
            value = data[row, col]

            # 转换像素坐标为实际坐标
            x, y = src.xy(row, col)

            # 将结果添加到列表中
            results.append({
                'x_coordinate': x,
                'y_coordinate': y,
                'value': value
            })

            # 每处理10,000个数据点输出一次
            if (i + 1) % 10000 == 0:
                print(f"已处理 {i + 1} / {total_pixels} 个数据点")

        # 创建DataFrame
        df = pd.DataFrame(results)

        # 导出到CSV
        df.to_csv(output_csv, index=False)
        print(f"已导出所有数据到 {output_csv}")


# 使用示例
if __name__ == "__main__":
    tif_path = "E:\\下载\\CHN_wind-speed_10m.tif"  # 替换为您的文件路径

    # 检查文件结构
    inspect_tif(tif_path)

    # 导出所有数据到CSV
    export_all_data_to_csv(tif_path, "CHN_wind-speed_10m_all.csv")