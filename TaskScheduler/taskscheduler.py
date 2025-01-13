import importlib
import multiprocessing
from multiprocessing import Lock, Manager, Pool
import psutil

from datablock import DataBlock

class TaskScheduler:
    def __init__(self, max_memory):
        self.max_memory = max_memory  # 设置最大内存容量（字节）
        self.manager = Manager()
        self.data_cache = self.manager.dict()  # 共享数据缓存
        self.data_priority_queue = self.manager.list()  # 数据块优先级队列
        self.task_queue = self.manager.Queue()  # 任务队列
        self.lock = Lock()  # 锁
        

    def add_task(self, task):
        with self.lock:
            for data_block in task.data_requirements:
                if data_block in self.data_cache:
                    self.data_cache[data_block].increase_ref()
                else:
                    db = DataBlock(data_block)
                    db.increase_ref()
                    self.data_cache[data_block] = db
                self._update_data_priority(data_block)
            self.task_queue.put(task)

    def _update_data_priority(self, data_block):
        # 根据引用计数更新数据块优先级
        # 优先级为 -ref_count 以实现最高优先级先加载
        priority = -self.data_cache[data_block].ref_count
        if data_block not in self.data_priority_queue:
            self.data_priority_queue.append((priority, data_block))

    def load_data(self, data_block):
        # 检查内存是否足够
        current_memory_usage = psutil.Process().memory_info().rss
        if current_memory_usage + self.data_cache[data_block].size <= self.max_memory:
            self.data_cache[data_block].load()
            self.data_cache[data_block].in_memory = True
        else:
            # 内存不足，卸载低优先级的数据块
            self._unload_low_priority_data()
            self.data_cache[data_block].load()
            self.data_cache[data_block].in_memory = True
        db = self.data_cache[data_block]
        print(f"Data block {data_block} loaded into memory.{db.data.head()}")

    def _unload_low_priority_data(self):
        # 按优先级卸载低优先级的数据块
        if self.data_priority_queue:
            # 找到优先级最低的数据块
            lowest_priority = max(self.data_priority_queue, key=lambda x: x[0])[0]
            for db_item in self.data_priority_queue:
                if db_item[0] == lowest_priority:
                    db_path = db_item[1]
                    if self.data_cache[db_path].ref_count == 0:
                        self.data_cache[db_path].unload()
                        self.data_cache[db_path].in_memory = False
                        self.data_priority_queue.remove(db_item)
                        break

    def execute_tasks(self):
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            while not self.task_queue.empty():
                task = self.task_queue.get()
                # 确保所有数据块都已加载
                for data_block in task.data_requirements:
                    if not self.data_cache[data_block].in_memory:
                        self.load_data(data_block)
                # 执行任务

                pool.apply_async(self._run_task, args=(task,), callback=self._task_callback)

    def _run_task(self, task):
        task.start()
        # 动态加载任务函数
        print('runnning task:', task.task_id)
        module = importlib.import_module(task.module_name)
        func = getattr(module, task.func_name)
        # 执行任务，传入数据缓存
        result = func(self.data_cache)
        task.complete()
        return task, result

    def _task_callback(self, task, result):
        try:
            print(f"Task completed with result: {result}")
            with self.lock:
                for data_block in task.data_requirements:
                    self.data_cache[data_block].decrease_ref()
                    if self.data_cache[data_block].ref_count == 0:
                        self.data_cache[data_block].unload()
                        del self.data_cache[data_block]
        except Exception as e:
            print(f"Error in callback: {e}")
 
