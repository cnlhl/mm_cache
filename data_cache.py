import os
import fcntl
import posix_ipc
import mmap
import pandas as pd
import numpy as np
import socket
from priority_queue import PriorityQueue
import json

class DataCache:
    def __init__(self, config_file='config.json'):
        config = json.load(open(config_file))
        # Ensure only one instance is running
        self.lock_file = 'dataloader.lock'
        self.fp = open(self.lock_file, 'w')
        try:
            fcntl.lockf(self.fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print("Dataloader service is already running")
            exit()
        # Initialize cache management
        self.cache = {}
        self.cache_order = PriorityQueue(min_queue=True)
        self.request_queue = PriorityQueue(min_queue=False)
        self.cache_usage = 0
        # Load configuration, size is counted in gigabytes
        self.cache_capacity = config.get('cache_size', 20) * 1024 * 1024 * 1024
        self.data_path = config.get('data_path', '/home/haolinl/converted_parquet')

    def __del__(self):
        fcntl.lockf(self.fp, fcntl.LOCK_UN)
        os.remove(self.lock_file)

    def load_h5_data_to_memory(self, data_id):
        data_path = self.get_data_path(data_id)
        df = pd.read_parquet(data_path)
        array = df.to_numpy()
        shm_name = f"/shm_{data_id}" 
        try:
            shm = posix_ipc.SharedMemory(
                name=shm_name,
                flags= posix_ipc.O_CREAT | posix_ipc.O_EXCL,
                mode=0o600,
                size=array.nbytes
            )
        except posix_ipc.ExistentialError:
            shm = posix_ipc.SharedMemory(name=shm_name)
            if shm.size < array.nbytes:
                os.ftruncate(shm.fd, array.nbytes)
        shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_WRITE)
        shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm_mmap)
        shm_arr[:] = array[:]
        self.cache[data_id] = {'shm_name': shm_name, 'shape': array.shape, 'dtype': array.dtype}
        print(f"Loaded data {data_id} into shared memory {shm_name}")
        self.cache_usage += array.nbytes
        self.manage_cache()
        shm_mmap.close()
        shm.close_fd()

    def manage_cache(self):
        if self.request_queue.empty():
            return
        while not self.cache_order.empty() and self.cache_order.front()[1] == 0: 
            # Remove data not being used
            least_used_key = self.cache_order.front()[0]
            print(f"removing {least_used_key}")
            shm_name = self.cache[least_used_key]['shm_name']
            shm = posix_ipc.SharedMemory(name=shm_name)
            shm.unlink()
            del self.cache[least_used_key]
            self.cache_usage -= shm.size
            self.cache_order.pop()
        # not safe, when data is larger then the empty cache size
        while not self.request_queue.empty() and self.cache_usage < self.cache_capacity:
            # Load the next requested data
            next_data_id, next_data_weight = self.request_queue.pop()
            data_path = self.get_data_path(next_data_id)
            self.load_h5_data_to_memory(next_data_id, data_path)
            self.cache_order.increase(next_data_id, next_data_weight)

    def get_data_path(self, data_id):
        # Get the data file path based on data_id
        return  os.path.join(self.data_path,f'{data_id}s.parquet')
    
    def exit_and_clean(self):
        while not self.cache_order.empty():
            # Remove the least recently used data
            least_used_key, least_used_weight = self.cache_order.getmin()
            shm_name = self.cache[least_used_key]['shm_name']
            # Open and unlink the shared memory
            shm = posix_ipc.SharedMemory(name=shm_name)
            shm.unlink()
            self.cache_order.pop()
        os._exit(0)

    def on_complete(self, data_id):
        self.cache_order.decrease(data_id)
        print(f"Received completion notification for {data_id}, decreased weight.")
        self.manage_cache()
    
    def start_service(self, host='localhost', port=6000):
        self._initialize_server(host, port)
        print("Dataloader service started, waiting for connections...")
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                print(f"Received connection from {addr}")
                self._handle_client(client_socket)
        except KeyboardInterrupt:
            print("Dataloader service stopped")
            self.exit_and_clean()
        finally:
            self.server_socket.close()

    def _initialize_server(self, host, port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, port))
        self.server_socket.listen(5)

    def _handle_client(self, client_socket):
        data = client_socket.recv(1024).decode()
        if data.startswith("REQUEST"):
            self._handle_request(client_socket, data)
        elif data.startswith("CHECK"):    
            self._handle_check(client_socket, data)
        elif data.startswith("COMPLETE"):
            self._handle_complete(client_socket, data)
        client_socket.close()

    def _handle_request(self, client_socket, data):
        _, data_id = data.split('#')
        if data_id in self.cache:
            info = self._get_cache_info(data_id)
            client_socket.send(info.encode())
            self.cache_order.increase(data_id)
        elif self.cache_usage < self.cache_capacity:
            self.load_h5_data_to_memory(data_id)
            info = self._get_cache_info(data_id)
            client_socket.send(info.encode())
            self.cache_order.increase(data_id)
        else:
            # enqueue first, then try to free up space & load data
            self.request_queue.increase(data_id)
            self.manage_cache()
            print(f"Cache is full, added {data_id} to request queue.")
            client_socket.send("WAIT".encode())

    def _handle_check(self, client_socket, data):
        _, data_id = data.split('#')
        if data_id in self.cache:
            info = self._get_cache_info(data_id)
            client_socket.send(info.encode())
        elif data_id in self.request_queue:
            client_socket.send("WAIT".encode())
        else:
            client_socket.send("INVALID_REQUEST".encode())
        
    def _handle_complete(self, client_socket, data):
        _, data_id = data.split('#')
        self.on_complete(data_id)
        client_socket.send("ACK".encode())

    def _get_cache_info(self, data_id):
        return f"{self.cache[data_id]['shm_name']}|{self.cache[data_id]['shape']}|{self.cache[data_id]['dtype']}"