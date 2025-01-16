from data_loader import DataLoader
import os
import pandas as pd

if __name__ == "__main__":
    # date_dirs = os.listdir('/home/Level2/2023')
    date_dirs = ['20231226']
    to_date_time = lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')
    date_dirs = [to_date_time(date) for date in date_dirs if to_date_time(date) is not pd.NaT]

    # filter target date
    start_date = pd.to_datetime('20231201', format='%Y%m%d')
    end_date = pd.to_datetime('20231231', format='%Y%m%d') 
    date_dirs = [date for date in date_dirs if start_date <= date <= end_date]
    date_dirs = [date.strftime('%Y%m%d') for date in date_dirs]
    date_dirs
    for date_dir in date_dirs:
        data_loader = DataLoader()
        df = data_loader.get('trade', date_dir)
        print(df)