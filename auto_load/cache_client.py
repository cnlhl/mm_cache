import requests
import logging
import posix_ipc
import mmap
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

order_columns = ['order_time', 'sysid', 'order_price', 'order_volume', 'bs_flag', 
                 'order_type', 'stock_code']
trade_columns = ['trad_time', 'sysid', 'trade_code', 'bs_flag', 'trade_price', 
                 'trade_volume', 'sell_order_id', 'buy_order_id', 'stock_code']
tick_columns = ['time', 'close', 'high', 'low', 'total_volume', 'total_amt', 'bdp1',
                'bdv1', 'bdp2', 'bdv2', 'bdp3', 'bdv3', 'bdp4', 'bdv4', 'bdp5', 'bdv5',
                'bdp6', 'bdv6', 'bdp7', 'bdv7', 'bdp8', 'bdv8', 'bdp9', 'bdv9', 'bdp10',
                'bdv10', 'akp1', 'akv1', 'akp2', 'akv2', 'akp3', 'akv3', 'akp4', 'akv4',
                'akp5', 'akv5', 'akp6', 'akv6', 'akp7', 'akv7', 'akp8', 'akv8', 'akp9',
                'akv9', 'akp10', 'akv10', 'total_num', 'total_volume_diff', 
                'total_amt_diff','total_num_diff', 'stock_code']

column_name = {'order':order_columns, 'trade':trade_columns, 'tick':tick_columns}

class CacheClient:
    # 服务器地址
    def __init__(self):
        self.base_url = 'http://localhost:6000'

    def _request_data(self, data_id):
        """请求数据"""
        response = requests.get(f'{self.base_url}/request/{data_id}')
        if response.status_code == 200:
            # 成功响应，返回解析后的 JSON 数据
            data = response.json()
            return data['shape']
        else:
            return 'Data not found'

    def _check_data(self):
        """检查缓存"""
        response = requests.get(f'{self.base_url}/check')
        if response.status_code == 200:
            return response.json()
        else:
            return None
            
    def get(self, table, date):
        data_id = f'{date}_{table}'
        shape = self._request_data(data_id)
        if shape:
            try:
                shm = posix_ipc.SharedMemory(name=f'/shm_{data_id}')
                shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_READ)
                shm_arr = np.ndarray(shape, dtype='float64', buffer=shm_mmap)
                df = pd.DataFrame(shm_arr)
                df.columns = column_name[table]
                return df
            except Exception as e:
                logging.error(f"Error loading data {data_id}: {e}")
                raise
        else:
            logging.info('data doesn\'t exist in cache')
    
    def check(self):
        cached = self._check_data()
        if cached:
            return cached
        else:
            logging.info('check failed')

