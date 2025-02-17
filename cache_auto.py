import json
from collections import deque
import threading
import os
import logging
import sys
import time
import pandas as pd
import numpy as np
import posix_ipc
import mmap
import atexit

log_directory = './log'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger('cache_logger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('./log/data_cache.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class CacheAuto:
    def __init__(self, config_file = 'config.json'):
        config = json.load(open(config_file))

        self.loaded_queue = deque()
        self.unloaded_queue = deque()
        self.cache_usage = 0
        self.cache = {}

        self.cache_capacity = config.get('cache_size', 20)
        self.data_path = config.get('data_path', '/home/haolinl/converted_parquet')
        self.update_interval = config.get('update_interval', 60)
        
        # 添加内存使用统计
        self.total_memory_usage = 0
        self.max_memory_limit = config.get('max_memory_mb', 1024) * 1024 * 1024  # 转换为字节
        
        atexit.register(self.stop)

        self._cache_lock = threading.Lock()
        self._stop_event = threading.Event()
        self.loader_thread = threading.Thread(target=self._initial_load, daemon=True)
        self.loader_thread.start()
    
    
        
    def stop(self):
        self._stop_event.set()
        self.loader_thread.join()
        for data_id in self.loaded_queue:
            self._remove_by_data_id(data_id)
        logger.info('cache stopped')
        
    def _initial_load(self):
        cachable_items = os.listdir(self.data_path)
        data_ids = sorted([item.split('.')[0] for item in cachable_items])
        for data_id in data_ids:
            if self.cache_usage < self.cache_capacity:
                self._load_by_data_id(data_id)
                self.loaded_queue.append(data_id)
            else:
                self.unloaded_queue.append(data_id)
        logger.info('finished init')
        logger.info(f'loaded: {self.loaded_queue}')
        logger.info(f'unloaded: {self.unloaded_queue}')
        self._auto_manage_cache()
        
    def _auto_manage_cache(self):
        logger.info('auto manage method entered')
        if len(self.unloaded_queue) == 0:
            logger.info('all data cached, exit manager')
            return
        while True:
            to_free = self.loaded_queue.popleft()
            to_load = self.unloaded_queue.popleft()
            self._remove_by_data_id(to_free)
            self.unloaded_queue.append(to_free)
            self._load_by_data_id(to_load)
            self.loaded_queue.append(to_load)
            time.sleep(self.update_interval)
                
    def _load_by_data_id(self, data_id):
        with self._cache_lock:
            try:
                logger.debug(f'started loading {data_id}')
                data_path = self._get_data_path(data_id)
                if not os.path.exists(data_path):
                    logger.error(f"Data file not found: {data_path}")
                    return False
                
                try:
                    df = pd.read_parquet(data_path)
                except Exception as e:
                    logger.error(f"Failed to read parquet file from {data_path}: {e}")
                    return False
                
                # Group the dataframe by the last column
                last_col = df.columns[-1]
                groups = df.groupby(last_col)

                temp_cache = {}
    
                for group_val, group_df in groups:
                    group_val = int(group_val)
                    array = group_df.to_numpy()
                    # logger.debug(f"read parquet data {data_id} group {group_val} with shape {array.shape} and dtype {array.dtype}")
                    shm_name = f"/shm_{data_id}_{group_val}"
                    try:
                        shm = posix_ipc.SharedMemory(
                            name=shm_name,
                            flags=posix_ipc.O_CREAT | posix_ipc.O_EXCL,
                            mode=0o666,
                            size=array.nbytes
                        )
                    except posix_ipc.ExistentialError:
                        shm = posix_ipc.SharedMemory(name=shm_name)
                        if shm.size < array.nbytes:
                            try:
                                os.ftruncate(shm.fd, array.nbytes)
                            except OSError as e:
                                logger.error(f"Failed to resize shared memory {shm_name}: {e}")
                                shm.unlink()
                                continue
    
                    # logger.debug(f"Shared memory {shm_name} created with size {array.nbytes}")
                    shm_mmap = mmap.mmap(shm.fd, shm.size, access=mmap.ACCESS_WRITE)
                    shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm_mmap)
                    shm_arr[:] = array[:]
                    # logger.debug(f"Data {data_id} (group: {group_val}) written to shared memory {shm_name}")
                    # self.cache[f"{data_id}_{group_val}"] = array.shape
                    temp_cache[f"{data_id}_{group_val}"] = array.shape
    
                    # logger.info(f"[DataCache] Loaded data {data_id} (group: {group_val}) into shared memory {shm_name}")
                    shm_mmap.close()
                    shm.close_fd()
                logger.info(f'finished loading {data_id}')
                self.cache.update(temp_cache)
                self.cache_usage += 1
            except Exception as e:
                logger.error(e)
    
    def _remove_by_data_id(self, data_id):
        with self._cache_lock:
            try:
                # 找到所有以 data_id_ 开头的键
                keys_to_remove = [k for k in self.cache.keys() if k.startswith(f"{data_id}_")]
                for k in keys_to_remove:
                    del self.cache[k]
                    shm_name = f"/shm_{k}"
                    shm = posix_ipc.SharedMemory(name=shm_name)
                    shm.unlink()
                    shm.close_fd()
                    # logger.info(f"[DataCache] Removed data {k} from shared memory {shm_name}")
                logger.info(f"Removed data {data_id}")
                self.cache_usage -= 1
            except Exception as e:
                logger.error(f"Failed to remove shared memory for {data_id}: {e}")
            
    def _get_data_path(self, data_id):
        return os.path.join(self.data_path, f'{data_id}.parquet')
    
    def get(self,data_id):
        if data_id in self.cache:
            return self.cache[data_id]
        else:
            return None
    
    def check(self):
        return list(self.cache.keys())