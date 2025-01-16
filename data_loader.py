import socket
import time
import numpy as np
import pandas as pd
import posix_ipc
import mmap
import logging
import sys

logger = logging.getLogger('loader_logger')
logger.setLevel(logging.DEBUG) 

file_handler = logging.FileHandler('date_loader.log')
file_handler.setLevel(logging.DEBUG)  
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class DataLoader:
    def __init__(self, host='localhost', port=6000):
        self.host = host
        self.port = port
        self.request_timeout = 60*60
        self.poll_interval = 30
        self.requested_data = []
    
    def __del__(self):
        for data_id in self.requested_data:
            self.finish_using(data_id)
    
    def _parse_info(self, info):
        shm_name, shape_str, dtype_str = info.split('|')
        shape = tuple(map(int, shape_str[1:-1].split(',')))
        dtype = np.dtype(dtype_str)
        return shm_name, shape, dtype
        

    def request_data(self, data_id):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))
        client_socket.send(f"REQUEST#{data_id}".encode())
        info = client_socket.recv(1024).decode()
        client_socket.close()
        if not info.startswith("WAIT"):
            return self._parse_info(info)
        return self._poll_result(data_id, time.time())
        
    def _poll_result(self, data_id: str, start_time: float):
        while True:
            if time.time() - start_time > self.request_timeout:
                logger.error(f"Request timeout for {data_id}")
                return None
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((self.host, self.port))
                if not client_socket:
                    time.sleep(self.poll_interval)
                    continue
                client_socket.send(f"CHECK#{data_id}".encode())
                response = client_socket.recv(1024).decode()
                client_socket.close()
                if response == "WAIT":
                    time.sleep(self.poll_interval)
                    continue
                elif response == "INVALID_REQUEST":
                    logger.error(f"Invalid request for {data_id}")
                    return None
                else:
                    return self._parse_info(response)
            except socket.error as e:
                logger.error(f"Error checking request for {data_id}: {e}")
                time.sleep(self.poll_interval)
                continue

    def notify_completion(self, data_id):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))
        client_socket.send(f"COMPLETE#{data_id}".encode())
        ack = client_socket.recv(1024).decode()
        if ack == "ACK":
            logger.info(f"Completion notification for {data_id} sent successfully.")
        client_socket.close()

    def load_day(self, table, date):
        data_id = f'{date}_{table}'
        shm_name, shape, dtype = self.request_data(data_id)
        print(shm_name, shape, dtype)

        try:
            shm = posix_ipc.SharedMemory(name=shm_name)
            shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_READ)
            shm_arr = np.ndarray(shape, dtype=dtype, buffer=shm_mmap)
            df = pd.DataFrame(shm_arr)
            self.requested_data.append(data_id)
            return df
        except Exception as e:
            logger.error(f"Error loading data {data_id}: {e}")
    
    def load_stock(self, table, date, stock):
        df = self.load_day(table, date)
        return df[df['stock_id'] == stock]
        

    def get(self, table, date, stock_ids=None):
        if stock_ids is None:
            return self.load_day(table, date)
        else :
            res = {}
            for stock in stock_ids:
                res[stock] = self.load_stock(table, date, stock)
            return res

    def finish_using(self, data_id):
        self.notify_completion(data_id)