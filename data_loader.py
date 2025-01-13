import socket
import numpy as np
import pandas as pd
import posix_ipc
import mmap

class DataLoader:
    def __init__(self, host='localhost', port=6000):
        self.host = host
        self.port = port

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

    def get(self, data_id):
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
            return df
        except Exception as e:
            print(f"Error loading data {data_id}: {e}")

    def finish_using(self, data_id):
        self.notify_completion(data_id)