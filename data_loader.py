import socket
import numpy as np
import pandas as pd
import posix_ipc
import mmap

class DataLoader:
    def __init__(self, host='localhost', port=6000):
        self.host = host
        self.port = port
        self.requested_data = []
    
    def __del__(self):
        for data_id in self.requested_data:
            self.finish_using(data_id)

    def request_data(self, data_id):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))
        client_socket.send(f"REQUEST#{data_id}".encode())
        info = client_socket.recv(1024).decode()
        client_socket.close()
        shm_name, shape_str, dtype_str = info.split('|')
        shape = tuple(map(int, shape_str.strip('()').split(',')))
        dtype = np.dtype(dtype_str)
        return shm_name, shape, dtype

    def notify_completion(self, data_id):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))
        client_socket.send(f"COMPLETE#{data_id}".encode())
        ack = client_socket.recv(1024).decode()
        if ack == "ACK":
            print(f"Completion notification for {data_id} sent successfully.")
        client_socket.close()

    def load_day(self, table, date):
        data_id = f'{date}_{table}'
        shm_name, shape, dtype = self.request_data(data_id)
        print(shm_name, shape, dtype)

        try:
            # Open the shared memory segment
            shm = posix_ipc.SharedMemory(name=shm_name)
            # Map the shared memory into the process's address space
            shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_READ)
            # Create a NumPy array from the mapped memory
            shm_arr = np.ndarray(shape, dtype=dtype, buffer=shm_mmap)
            df = pd.DataFrame(shm_arr)
            # print(df.head())
            self.requested_data.append(data_id)
            return df
        except Exception as e:
            print(f"Error loading data {data_id}: {e}")
    
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