import socket
import threading
import sys
import logging
import json
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import time

from data_cache_new import DataCache

logger = logging.getLogger('cache_server_logger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('./log/cache_server.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class CacheServer:
    def __init__(self, data_cache:DataCache , auto_load=False ,host='localhost', port=6000, max_workers=10):
        """
        :param data_cache: 一个 DataCache 实例
        """
        self.data_cache = data_cache
        self.host = host
        self.port = port
        self.max_workers = max_workers

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        self.auto_load = auto_load

        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)
        if self.auto_load:
            self.pool.submit(self._auto_request_init)
            logger.info('auto load enabled')

        logger.info(f"CacheServer listening on {self.host}:{self.port}")
    
    def start(self):
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                logger.info(f"Accepted connection from {addr}")
                if not self.auto_load:
                    self.pool.submit(self._handle_client, client_socket, addr)
                else:
                    self.pool.submit(self._handle_client_auto_load, client_socket, addr)             
        except KeyboardInterrupt:
            logger.info("CacheServer stopped by KeyboardInterrupt")
            self.stop()

    def stop(self):
        logger.info("Stopping CacheServer...")
        self.data_cache.exit_and_clean()
        self.server_socket.close()
        
    def _auto_request_init(self):
        logger.debug('auto_request_init')
        data_id_list = sorted(self.data_cache.get_cachable_items_list())
        logger.debug(data_id_list)
        loaded_queue = deque()
        unloaded_queue = deque()
        try:
            for data_id in data_id_list:
                loaded = self.data_cache.request_load(data_id)
                if loaded:
                    loaded_queue.append(data_id)
                else:
                    unloaded_queue.append(data_id)
            self._auto_request(loaded_queue,unloaded_queue)
        except Exception as e:
            logger.error(e)
            self.stop()

    def _auto_request(self,loaded_queue:deque, unloaded_queue:deque):
        # 当未加载队列不为空，且已加载队列末元素已完成加载时，加载未加载队列首元素
        logger.debug('auto_request called')
        logger.debug(loaded_queue)
        logger.debug(unloaded_queue)
        while True:
            if self.data_cache.get_cache_info_by_id(loaded_queue[-1]) is None or len(unloaded_queue) == 0:
                time.sleep(30)
                continue  
            next_to_load = unloaded_queue.popleft()
            next_to_free = loaded_queue.popleft()
            self.data_cache.on_complete(next_to_free)
            unloaded_queue.append(next_to_free)
            self.data_cache.request_load(next_to_free)
            while self.data_cache.get_cache_info_by_id(next_to_load) == 'WAITING':
                next_to_free = loaded_queue.popleft()
                self.data_cache.on_complete(next_to_free)
                unloaded_queue.append(next_to_free)
                self.data_cache.request_load(next_to_free)
                time.sleep(30)
            loaded_queue.append(next_to_load)
        

    def _handle_client(self, client_socket:socket, addr):
        """
        处理一个客户端连接（一次交互）
        """
        data = client_socket.recv(1024).decode().strip()
        if not data:
            client_socket.close()
            return

        if data.startswith("REQUEST"):
            logger.debug('REQUEST received')
            # data 格式: "REQUEST#<data_id>"
            cmd, data_id = data.split('#', 1)
            loaded = self.data_cache.request_load(data_id)
            if loaded is None:
                # 数据不存在
                logger.error(f"Data {data_id} not found.")
                client_socket.send("NOT_FOUND".encode())
            if loaded:
                # 可能已经在缓存，也可能刚开始加载
                info = self.data_cache.get_cache_info_by_id(data_id)
                if info == 'LOADING':
                    logger.debug('REQUEST data is in loading')
                    # 正在加载中
                    client_socket.send("WAIT".encode())
                else:
                    logger.debug('REQUEST data loaded')
                    # 已经加载完
                    client_socket.send(info.encode())
            else:
                # 内存不够，排队中
                logger.debug('insufficient cache space, wait')
                client_socket.send("WAIT".encode())

        elif data.startswith("CHECK"):
            logger.debug('CHECK received')
            # data 格式: "CHECK#<data_id>"
            cmd, data_id = data.split('#', 1)
            info = self.data_cache.get_cache_info_by_id(data_id)
            if info == 'LOADING' or info == 'WAITING':
                # 默认check是非首次请求，也即data_id合法且在等待加载中
                client_socket.send("WAIT".encode())
            else:
                client_socket.send(info.encode())
        elif data.startswith("LOOK"):
            logger.debug('LOOK received')
            # LOOK 请求，返回已在cache中的数据
            cached = self.data_cache.get_cached_items_list()
            logger.debug(type(cached))
            client_socket.send(json.dumps(cached).encode())
            logger.debug('LOOK data sent')
        elif data.startswith("COMPLETE"):
            logger.debug('complete notification received')
            # data 格式: "COMPLETE#<data_id>"
            cmd, data_id = data.split('#', 1)
            self.data_cache.on_complete(data_id)
            client_socket.send("ACK".encode())
            logger.debug('ack sent')
        else:
            client_socket.send("INVALID_REQUEST".encode())

        client_socket.close()

    def _handle_client_auto_load(self, client_socket:socket, addr):
        """
        处理一个客户端连接（自动加载情况下）
        """
        data = client_socket.recv(1024).decode().strip()
        if not data:
            client_socket.close()
            return

        if data.startswith("REQUEST") or data.startswith("CHECK"):
            # data 格式: "CHECK#<data_id>"
            try:
                cmd, data_id = data.split('#', 1)
                logger.debug(f'{cmd} received')
                info = self.data_cache.get_cache_info_by_id(data_id)
                logger.debug(f'info: {info}')
                if info == 'LOADING' or info == 'WAITING':
                    client_socket.send('WAIT'.encode())
                elif info is None:
                    client_socket.send('NOT_FOUND'.encode()) 
                else:
                    client_socket.send(info.encode())
            except Exception as e:
                logger.error(e)
                client_socket.send('INVALID_REQUEST'.encode())
        elif data.startswith("LOOK"):
            logger.debug('LOOK received')
            # LOOK 请求，返回已在cache中的数据
            cached = self.data_cache.get_cached_items_list()
            client_socket.send(json.dumps(cached).encode())
        elif data.startswith("COMPLETE"):
            logger.debug('complete notification received')
            # data 格式: "COMPLETE#<data_id>"
            cmd, data_id = data.split('#', 1)
            self.data_cache.on_complete(data_id)
            client_socket.send("ACK".encode())
            logger.debug('ack sent')
        else:
            client_socket.send("INVALID_REQUEST".encode())

        client_socket.close()