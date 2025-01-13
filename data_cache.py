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
        self.cache_order = PriorityQueue()
        self.cache_size = config.get('cache_size', 5)
        self.data_path = config.get('data_path', '/home/haolinl/converted_parquet')

    def __del__(self):
        fcntl.lockf(self.fp, fcntl.LOCK_UN)
        os.remove(self.lock_file)

    def load_h5_data_to_memory(self, data_id, data_path):
        # Load data as a Pandas DataFrame
        df = pd.read_parquet(data_path)
        # Convert to NumPy array for sharing
        array = df.to_numpy()
        # Create shared memory
        shm_name = f"/shm_{data_id}"  # Shared memory name must start with a slash
        try:
            # Create a new shared memory segment
            shm = posix_ipc.SharedMemory(
                name=shm_name,
                flags= posix_ipc.O_CREAT | posix_ipc.O_EXCL,
                mode=0o600,
                size=array.nbytes
            )
        except posix_ipc.ExistentialError:
            # If the segment already exists, open it
            shm = posix_ipc.SharedMemory(name=shm_name)
            # Resize the segment if necessary
            if shm.size < array.nbytes:
                os.ftruncate(shm.fd, array.nbytes)
        # Map the shared memory into the process's address space
        shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_WRITE)
        # Create a NumPy array from the mapped memory
        shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm_mmap)
        shm_arr[:] = array[:]
        # Store shared memory name, shape, and dtype in the cache
        self.cache[data_id] = {'shm_name': shm_name, 'shape': array.shape, 'dtype': array.dtype}
        print(f"Loaded data {data_id} into shared memory {shm_name}")
        # Manage cache size
        self.manage_cache()
        # Close the mmap object (shared memory remains open for other processes)
        shm_mmap.close()
        # Close the file descriptor (shared memory remains open for other processes)
        shm.close_fd()

    def manage_cache(self):
        while len(self.cache) > self.cache_size:
            # Remove the least recently used data
            least_used_key, least_used_weight = self.cache_order.getmin()
            if least_used_weight > 0:
                break
            print(f"Cache is full, removing {least_used_key}")
            shm_name = self.cache[least_used_key]['shm_name']
            # Open and unlink the shared memory
            shm = posix_ipc.SharedMemory(name=shm_name)
            shm.unlink()
            del self.cache[least_used_key]

    def get_data_path(self, data_id):
        # Get the data file path based on data_id
        return  os.path.join(self.data_path,f'{data_id}.parquet')
    
    def exit_and_clean(self):
        while not self.cache_order.empty():
            # Remove the least recently used data
            least_used_key, least_used_weight = self.cache_order.getmin()
            shm_name = self.cache[least_used_key]['shm_name']
            # Open and unlink the shared memory
            shm = posix_ipc.SharedMemory(name=shm_name)
            shm.unlink()
            self.cache_order.pop()
        fcntl.lockf(self.fp, fcntl.LOCK_UN)
        os.remove(self.lock_file)
        os._exit(0)

    def start_service(self, host='localhost', port=6000):
        # Create a socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, port))
        self.server_socket.listen(5)
        print("Dataloader service started, waiting for connections...")

        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                print(f"Received connection from {addr}")
                data = client_socket.recv(1024).decode()
                if data.startswith("REQUEST"):
                    _, data_id = data.split('#')
                    if data_id in self.cache:
                        # Data is in cache, return shared memory info
                        info = f"{self.cache[data_id]['shm_name']}|{self.cache[data_id]['shape']}|{self.cache[data_id]['dtype']}"
                        client_socket.send(info.encode())
                    else:
                        # Data is not in cache, load it
                        data_path = self.get_data_path(data_id)
                        self.load_h5_data_to_memory(data_id, data_path)
                        # Return shared memory info
                        info = f"{self.cache[data_id]['shm_name']}|{self.cache[data_id]['shape']}|{self.cache[data_id]['dtype']}"
                        client_socket.send(info.encode())
                    self.cache_order.increase(data_id)
                elif data.startswith("COMPLETE"):
                    _, data_id = data.split('#')
                    self.cache_order.decrease(data_id)
                    print(f"Received completion notification for {data_id}, decreased weight.")
                    client_socket.send("ACK".encode())
                client_socket.close()
        except KeyboardInterrupt:
            print("Dataloader service stopped")
            self.exit_and_clean()

        finally:
            self.server_socket.close()