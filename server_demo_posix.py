
from data_cache import DataCache

if __name__ == '__main__':
    loader = DataCache(config_file='config.json')
    loader.start_service()