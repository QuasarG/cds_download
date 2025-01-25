import os
import zipfile
import glob

def unzip_and_rename_grib_files(directory):
    # 获取目录中所有的zip文件
    zip_files = glob.glob(os.path.join(directory, '*.zip'))
    
    for zip_file in zip_files:
        # 获取zip文件名（不带路径和扩展名）
        zip_filename = os.path.basename(zip_file).replace('.zip', '')
        
        # 解压zip文件
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(directory)
        
        # 查找解压出来的grib文件并重命名
        grib_files = glob.glob(os.path.join(directory, '*.grib'))
        for grib_file in grib_files:
            new_grib_filename = f"{zip_filename}.grib"
            new_grib_filepath = os.path.join(directory, new_grib_filename)
            os.rename(grib_file, new_grib_filepath)

# 使用示例
if __name__ == "__main__":
    target_directory = "G:\\data_from_era5"
    unzip_and_rename_grib_files(target_directory)