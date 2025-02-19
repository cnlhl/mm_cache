import json
from collections import deque
import os
import logging
import sys
import time
import pandas as pd
import numpy as np
import posix_ipc
import mmap
import atexit
import multiprocessing
import signal
import psutil

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
        # 移除信号处理相关的代码
        # self._instance = self  # 不再需要保存实例引用
        
        config = json.load(open(config_file))
        
        self.manager = multiprocessing.Manager()
        self.cache = self.manager.dict()
        
        # 添加运行状态标志
        self.running = True
        
        # 为每种数据类型创建独立的队列
        self.loaded_queues = {
            'trade': self.manager.list(),
            'order': self.manager.list(),
            'tick': self.manager.list()
        }
        self.unloaded_queues = {
            'trade': self.manager.list(),
            'order': self.manager.list(),
            'tick': self.manager.list()
        }
        self.cache_usage = self.manager.dict({
            'trade': 0,
            'order': 0,
            'tick': 0
        })

        self.cache_capacity = config.get('cache_size', 20) // 3  # 为每种类型分配相同的容量
        self.data_path = config.get('data_path', '/home/haolinl/converted_parquet')
        self.update_interval = config.get('update_interval', 60)
        
        self.total_memory_usage = 0
        self.max_memory_limit = config.get('max_memory_mb', 1024) * 1024 * 1024
        
        # 仍然保留退出处理器
        atexit.register(self.stop)
        
        self._cleaned_up = False
        self._cache_lock = multiprocessing.Lock()
        self._stop_event = multiprocessing.Event()
        self._cleanup_events = {
            'trade': multiprocessing.Event(),
            'order': multiprocessing.Event(),
            'tick': multiprocessing.Event()
        }
        
        # 启动子进程
        self.processes = []
        for data_type in ['trade', 'order', 'tick']:
            process = multiprocessing.Process(
                target=self._initial_load_by_type,
                args=(data_type,),
                daemon=True  # 设置为守护进程
            )
            self.processes.append(process)
            process.start()

    def _initial_load_by_type(self, data_type):
        """子进程中的加载和清理逻辑"""
        # 设置子进程的信号处理
        signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
        
        try:
            cachable_items = os.listdir(self.data_path)
            # 只选择对应类型的数据
            data_ids = sorted([item.split('.')[0] for item in cachable_items if data_type in item])
            
            for data_id in data_ids:
                # 检查清理信号
                if self._cleanup_events[data_type].is_set() or self._stop_event.is_set():
                    logger.info(f"Cleanup signal received during initial load for {data_type}")
                    break
                    
                if self.cache_usage[data_type] < self.cache_capacity:
                    self._load_by_data_id(data_id)
                    self.loaded_queues[data_type].append(data_id)
                else:
                    self.unloaded_queues[data_type].append(data_id)
                    
            logger.info(f'finished init for {data_type}')
            logger.info(f'loaded {data_type}: {self.loaded_queues[data_type]}')
            logger.info(f'unloaded {data_type}: {self.unloaded_queues[data_type]}')
            
            # 如果没有收到清理信号，进入循环
            if not (self._cleanup_events[data_type].is_set() or self._stop_event.is_set()):
                self._auto_manage_cache_by_type(data_type)
                
        finally:
            # 确保在任何情况下都执行清理
            self._cleanup_type(data_type)
            
    def _cleanup_type(self, data_type):
        """清理特定类型的所有数据"""
        try:
            logger.info(f"Starting cleanup for {data_type}")
            for data_id in list(self.loaded_queues[data_type]):  # 使用list创建副本
                try:
                    self._remove_by_data_id(data_id)
                    self.loaded_queues[data_type].remove(data_id)  # 从队列中移除已清理的数据
                except Exception as e:
                    logger.error(f"Error cleaning up {data_id}: {e}")
            logger.info(f'Cleaned up shared memory for {data_type}')
        except Exception as e:
            logger.error(f"Cleanup for {data_type} failed: {e}")

    def _auto_manage_cache_by_type(self, data_type):
        logger.info(f'auto manage method entered for {data_type}')
        if len(self.unloaded_queues[data_type]) == 0:
            logger.info(f'all {data_type} data cached, exit manager')
            return
            
        while not self._stop_event.is_set() and self.running:
            try:
                if self._cleanup_events[data_type].is_set():
                    logger.info(f"Cleanup signal received for {data_type}, exiting manager")
                    break
                    
                # 使用列表方法而不是 deque 方法
                to_free = self.loaded_queues[data_type].pop(0)
                to_load = self.unloaded_queues[data_type].pop(0)
                self._remove_by_data_id(to_free)
                self.unloaded_queues[data_type].append(to_free)
                self._load_by_data_id(to_load)
                self.loaded_queues[data_type].append(to_load)
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Error in cache management for {data_type}: {e}")
                if not self.running or self._cleanup_events[data_type].is_set():
                    break
                time.sleep(1)

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
                self.cache_usage[data_id.split('_')[1]] += 1
            except Exception as e:
                logger.error(f"Error loading data {data_id}: {e}")
    
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
                self.cache_usage[data_id.split('_')[1]] -= 1
            except Exception as e:
                logger.error(f"Failed to remove shared memory for {data_id}: {e}")
            
    def _get_data_path(self, data_id):
        return os.path.join(self.data_path, f'{data_id}.parquet')

    def stop(self):
        if self._cleaned_up:
            return
        try:
            logger.info("Initiating cache shutdown...")
            self.running = False
            self._stop_event.set()
            
            # 1. 首先通知所有子进程停止工作
            for data_type in ['trade', 'order', 'tick']:
                self._cleanup_events[data_type].set()
            
            # 2. 给子进程一定时间进行自清理
            cleanup_timeout = 60
            for process in self.processes:
                process.join(timeout=cleanup_timeout)
            
            # 3. 检查并强制终止未能正常退出的进程
            for process in self.processes:
                if process.is_alive():
                    logger.warning(f"Process {process.pid} did not exit gracefully, terminating...")
                    process.terminate()
                    process.join(timeout=1)
                    if process.is_alive():
                        logger.warning(f"Force killing process {process.pid}")
                        process.kill()

            # 4. 清理共享内存
            self._emergency_cleanup()
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")
            self._emergency_cleanup()
        finally:
            self._cleaned_up = True
            logger.info('Cache stopped and cleaned up')

    def _emergency_cleanup(self):
        logger.info('emergency cleanup')
        try:
            # 1. 首先终止所有子进程
            current_process = psutil.Process()
            for child in current_process.children(recursive=True):
                try:
                    child.terminate()
                    child.wait(timeout=1)
                except psutil.TimeoutExpired:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            
            # 2. 等待短暂时间确保进程已经完全终止
            time.sleep(0.5)
            
            shm_directory = '/dev/shm'
            prefix = 'shm'

            for filename in os.listdir(shm_directory):
                if filename.startswith(prefix):
                    file_path = os.path.join(shm_directory, filename)
                    try:
                        os.remove(file_path)
                        print(f"Removed {file_path}")
                    except Exception as e:
                        print(f"Failed to remove {file_path}: {e}")
                    
        except Exception as e:
            logger.error(f"Emergency cleanup failed: {e}")

    def get(self,data_id):
        if data_id in self.cache:
            return self.cache[data_id]
        else:
            return None
    
    def check(self):
        return list(self.cache.keys())