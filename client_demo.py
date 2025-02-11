from cache_client import CacheClient

cache_client = CacheClient()
print(cache_client.get('order','20230907'))