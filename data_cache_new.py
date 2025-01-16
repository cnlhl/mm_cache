import os
import fcntl
import posix_ipc
import mmap
import pandas as pd
import numpy as np
import json
import threading
import queue
import logging
import sys

from priority_queue import PriorityQueue

logger = logging.getLogger('cache_logger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('date_cache.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class DataCache:
    def __init__(self, config_file='config.json'):
        config = json.load(open(config_file))

        self.lock_file = 'datacache.lock'
        self.fp = open(self.lock_file, 'w')
        try:
            fcntl.lockf(self.fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            logger.error("Another instance is running, exiting...")
            exit()

        # --- 共享资源 ---
        self.cache = {}
        self.cache_order = PriorityQueue(min_queue=True)
        self.request_queue = PriorityQueue(min_queue=False)
        self.cache_usage = 0

        self.cache_capacity = config.get('cache_size', 20) * 1024**3
        self.data_path = config.get('data_path', '/home/haolinl/converted_parquet')

        # 线程锁，用于保护以上共享数据结构
        self._cache_lock = threading.Lock()

        # 后台加载队列 & 线程
        self.load_queue = queue.Queue()
        self._stop_event = threading.Event()

        self.loader_thread = threading.Thread(target=self._loader_loop, daemon=True)
        self.loader_thread.start()

    def __del__(self):
        fcntl.lockf(self.fp, fcntl.LOCK_UN)
        os.remove(self.lock_file)

    def _loader_loop(self):
        """
        后台加载线程循环：
        - 从 self.load_queue 中取 data_id
        - 加载到共享内存
        """
        while not self._stop_event.is_set():
            try:
                data_id = self.load_queue.get(timeout=1)  # 如果1秒内没新任务，会抛 queue.Empty
            except queue.Empty:
                continue
            self._actually_load_data(data_id)

            self.load_queue.task_done()

    def _actually_load_data(self, data_id):
        """
        真正执行磁盘IO + 写共享内存的函数
        """
        with self._cache_lock:
            # 避免重复加载
            if data_id in self.cache:
                return

            data_path = self._get_data_path(data_id)
            df = pd.read_parquet(data_path)
            array = df.to_numpy()

            shm_name = f"/shm_{data_id}"
            try:
                shm = posix_ipc.SharedMemory(
                    name=shm_name,
                    flags=posix_ipc.O_CREAT | posix_ipc.O_EXCL,
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

            self.cache[data_id] = {
                'shm_name': shm_name,
                'shape': array.shape,
                'dtype': array.dtype
            }
            # 实际加载后，更正cache_usage
            self.cache_usage += array.nbytes
            self.cache_usage -= self._get_file_size(data_path)

            logger.info(f"[DataCache] Loaded data {data_id} into shared memory {shm_name}")

            shm_mmap.close()
            shm.close_fd()

            self.manage_cache()

    def _manage_cache(self):
        """淘汰和加载新的数据"""
        # 调用该方法必须先获取锁
        if self.request_queue.empty():
        # 若没有pending request，不作处理
            return

        while (not self.cache_order.empty()) and (self.cache_order.front()[1] == 0):
            least_used_key = self.cache_order.front()[0]
            logger.info(f"[DataCache] removing {least_used_key}")
            self._remove_data(least_used_key)

        while (not self.request_queue.empty()) and (self.cache_usage < self.cache_capacity):
            next_data_id, next_data_weight = self.request_queue.pop()
            self._ready_to_load(next_data_id)

    def _get_data_path(self, data_id):
        return os.path.join(self.data_path, f'{data_id}s.parquet')
    
    def _get_file_size(self, file_path):
        return os.path.getsize(file_path)
    
    def _ready_to_load(self, data_id):
        # load_queue, cache_order, cache_usage 的更新紧耦合
        if not self.cache_order.check_exist(data_id):
            # 如果是首次ready, 更新cache_usage，以原始文件大小预估
            self.cache_usage += self._get_file_size(self._get_data_path(data_id))
            # 同时入队准备被load
            self.load_queue.put(data_id)
        self.cache_order.increase(data_id)

        
    def _remove_data(self, data_id):
        shm_name = self.cache[data_id]['shm_name']
        shm = posix_ipc.SharedMemory(name=shm_name)
        shm.unlink()
        size_removed = shm.size
        del self.cache[data_id]
        self.cache_usage -= size_removed
        self.cache_order.pop()
    
    # 所有的开放给server的接口都必须持有锁

    def on_complete(self, data_id):
        """客户端用完后，减少其在 cache_order 中的使用权重"""
        with self._cache_lock:
            logger.debug('lock_acquired in on_complete')
            self.cache_order.decrease(data_id)
            logger.debug(f"[DataCache] on_complete {data_id}, decreased weight.")
            self.manage_cache()

    def request_load(self, data_id):
        """
        对外开放接口
        如果已经在cache里，就直接返回；若不在cache且有空间，就入load_queue；否则入request_queue等待；
        """
        with self._cache_lock:
            if data_id in self.cache:
                self.cache_order.increase(data_id)
                # 已加载
                return True
            # 如果还有空间或已经在cache_order中（被ready_load过），通过ready_load来更新cache_order
            if self.cache_usage < self.cache_capacity or self.cache_order.check_exist(data_id):
                self._ready_to_load(data_id)
                return True
            else:
                # 没空间，先排队
                logger.info(f"[DataCache] Not enough space, add {data_id} to request_queue.")
                self.request_queue.increase(data_id)
                self._manage_cache()
                return False

    def get_cache_info(self, data_id):
        """
        返回 shm_name|shape|dtype
        """
        with self._cache_lock:
            if data_id not in self.cache:
                return None
            info = self.cache[data_id]
            return f"{info['shm_name']}|{info['shape']}|{info['dtype']}"

    def exit_and_clean(self):
        """退出前的清理"""
        self._stop_event.set()
        self.loader_thread.join(timeout=3)

        with self._cache_lock:
            while not self.cache_order.empty():
                least_used_key, _ = self.cache_order.front()
                shm_name = self.cache[least_used_key]['shm_name']
                shm = posix_ipc.SharedMemory(name=shm_name)
                shm.unlink()
                self.cache_order.pop()
        os._exit(0)
