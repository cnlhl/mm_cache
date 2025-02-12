# CacheClient 使用

`CacheClient` 类用于与缓存服务器进行交互，获取缓存数据并将其转换为 Pandas DataFrame。

## 初始化

首先，导入 `CacheClient` 类并创建一个实例：

```python
from cache_client import CacheClient

cache_client = CacheClient()
```

## 方法

### `get(table, date)`

获取指定表和日期的数据。

#### 参数

- `table (str)`: 表名，可以是 `'order'`、`'trade'` 或 `'tick'`。
- `date (str)`: 日期，格式为 `YYYYMMDD`。

#### 返回值

- 如果数据存在于缓存中，返回一个 Pandas DataFrame。
- 如果数据不存在于缓存中，返回 `None` 并记录日志信息。

#### 示例

```python
df = cache_client.get('order', '20230907')
if df is not None:
    print(df.head())
else:
    print("Data not found in cache.")
```

### `check()`

检查当前缓存中的数据。

#### 返回值

- 返回一个包含缓存中所有数据 ID 的列表。
- 如果检查失败，返回 `None` 并记录日志信息。

#### 示例

```python
cached_data = cache_client.check()
if cached_data is not None:
    print("Cached data IDs:", cached_data)
else:
    print("Failed to check cache.")
```
