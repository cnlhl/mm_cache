from data_loader import DataLoader
import os
import pandas as pd

if __name__ == "__main__":
# 列出目录中的所有文件和文件夹
    date_dirs = os.listdir('/home/Level2/2023')

    # 定义一个 lambda 函数将字符串转换为日期时间对象
    to_date_time = lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')

    # 将目录中的日期字符串转换为日期时间对象，并过滤掉无效的日期
    date_dirs = [to_date_time(date) for date in date_dirs if to_date_time(date) is not pd.NaT]

    # 过滤出在2023年11月1日到2023年11月30日之间的日期
    start_date = pd.to_datetime('20231101', format='%Y%m%d')
    end_date = pd.to_datetime('20231130', format='%Y%m%d')  # 11月只有30天
    date_dirs = [date for date in date_dirs if start_date <= date <= end_date]
    date_dirs = [date.strftime('%Y%m%d') for date in date_dirs]
    date_dirs
    for date_dir in date_dirs:
        data_loader = DataLoader()
        target_id = f'{date_dir}_order'
        df = data_loader.get(target_id)
        print(df)