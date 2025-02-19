from cache_client import CacheClient

cache_client = CacheClient()
print(cache_client.get('tick', '20230901',600030))