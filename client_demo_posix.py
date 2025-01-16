from data_loader import DataLoader
import os
import pandas as pd
import multiprocessing


def process_date(date_dir):
    """子进程要执行的函数."""
    data_loader = DataLoader()
    df = data_loader.get('trade', date_dir)
    # 这里仅打印，也可以把 df 进行后续处理
    print(f"日期: {date_dir}, 数据:\n{df}")
    return df  # 如果需要获取结果，可以返回


if __name__ == "__main__":
    # date_dirs = os.listdir('/home/Level2/2023')
    date_dirs = ['20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226']

    to_date_time = lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')
    date_dirs = [to_date_time(date) for date in date_dirs if to_date_time(date) is not pd.NaT]

    # filter target date
    start_date = pd.to_datetime('20231201', format='%Y%m%d')
    end_date = pd.to_datetime('20231231', format='%Y%m%d')
    date_dirs = [date for date in date_dirs if start_date <= date <= end_date]
    date_dirs = [date.strftime('%Y%m%d') for date in date_dirs]

    # 开 10 个进程并行处理
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.map(process_date, date_dirs)

    # 如果需要在主进程中使用返回结果，可在此进行后续处理
    # results 中包含各个进程返回的 df 或其他结果
    print("并行处理完成，所有结果如下：")
    for r in results:
        print(r)
