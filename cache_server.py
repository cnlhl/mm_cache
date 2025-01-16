import socket
import threading
import sys
import logging
from concurrent.futures import ThreadPoolExecutor

from data_cache_new import DataCache

logger = logging.getLogger('cache_server_logger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('cache_server.log')
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
    def __init__(self, data_cache:DataCache , host='localhost', port=6000, max_workers=10):
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

        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)

        logger.info(f"CacheServer listening on {self.host}:{self.port}")

    def start(self):
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                logger.info(f"Accepted connection from {addr}")
                self.pool.submit(self._handle_client, client_socket, addr)
        except KeyboardInterrupt:
            logger.info("CacheServer stopped by KeyboardInterrupt")
            self.stop()

    def stop(self):
        logger.info("Stopping CacheServer...")
        self.data_cache.exit_and_clean()
        self.server_socket.close()

    def _handle_client(self, client_socket:socket, addr):
        """
        处理一个客户端连接（一次交互）
        """
        data = client_socket.recv(1024).decode().strip()
        if not data:
            client_socket.close()
            return

        if data.startswith("REQUEST"):
            # data 格式: "REQUEST#<data_id>"
            cmd, data_id = data.split('#', 1)
            loaded = self.data_cache.request_load(data_id)
            if loaded:
                # 可能已经在缓存，也可能刚开始加载
                info = self.data_cache.get_cache_info(data_id)
                if info:
                    # 已经加载完
                    client_socket.send(info.encode())
                else:
                    # 正在加载中
                    client_socket.send("WAIT".encode())
            else:
                # 内存不够，排队中
                client_socket.send("WAIT".encode())

        elif data.startswith("CHECK"):
            # data 格式: "CHECK#<data_id>"
            cmd, data_id = data.split('#', 1)
            info = self.data_cache.get_cache_info(data_id)
            if info:
                client_socket.send(info.encode())
            else:
                # 可能在 request_queue 中排队，也可能无效
                # 这里你可以根据 self.request_queue.contains(data_id) 做区分
                client_socket.send("WAIT".encode())

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
