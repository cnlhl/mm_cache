
from cache_server import CacheServer
from data_cache_new import DataCache

if __name__ == '__main__':
    loader = DataCache(config_file='config.json')
    server = CacheServer(data_cache=loader)
    server.start()