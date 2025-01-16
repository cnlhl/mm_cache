## 接口说明

### 初始化 DataLoader

```python
from data_loader import DataLoader

# 初始化 DataLoader 实例
data_loader = DataLoader()
```

### 加载数据

#### 加载某一天的数据

```python
# 加载某一天的全部数据
date = '20231226'
table = 'trade'
df = data_loader.load_day(table, date)
print(df)
```

#### 根据股票ID列表加载数据

```python
# 根据股票ID列表加载数据
date = '20231226'
table = 'trade'
stock_ids = ['601699', '600030']
data = data_loader.get(table, date, stock_ids)
for stock_id, df in data.items():
    print(f"Stock ID: {stock_id}")
    print(df)
```

### 完成数据使用

```python
# 完成数据使用，通知服务端
data_id = '20231226_trade'
data_loader.finish_using(data_id)
```

### 示例代码

```python
from data_loader import DataLoader
import os
import pandas as pd
import multiprocessing

def process_date(date_dir):
    """子进程要执行的函数."""
    data_loader = DataLoader()
    df = data_loader.get('trade', date_dir)
    # 这里仅打印，也可以把 df 进行后续处理
    print(f"日期: {date_dir}, 数据:\n{df}")
    return df  # 如果需要获取结果，可以返回

if __name__ == "__main__":
    # date_dirs = os.listdir('/home/Level2/2023')
    date_dirs = ['20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226', '20231226']

    to_date_time = lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')
    date_dirs = [to_date_time(date) for date in date_dirs if to_date_time(date) is not pd.NaT]

    # filter target date
    start_date = pd.to_datetime('20231201', format='%Y%m%d')
    end_date = pd.to_datetime('20231231', format='%Y%m%d')
    date_dirs = [date for date in date_dirs if start_date <= date <= end_date]
    date_dirs = [date.strftime('%Y%m%d') for date in date_dirs]

    # 开 10 个进程并行处理
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.map(process_date, date_dirs)

    # 如果需要在主进程中使用返回结果，可在此进行后续处理
    # results 中包含各个进程返回的 df 或其他结果
    print("并行处理完成，所有结果如下：")
    for r in results:
        print(r)
```

## To do
- [x] 用户侧：封装更高层次的读取方法，支持逐股票筛选
- [x] 服务侧：缓存淘汰方法完善
- [x] 服务侧：增加请求队列，使能够同时处理多个请求
- [x] 用户侧：长轮询机制，等待服务侧加载
- [x] 用户侧：类销毁减引用和清理操作
- [x] 服务侧：增加日志记录
- [x] 服务侧：异常处理
- [x] 服务测：多进程并发处理
- [ ] 服务测：测试用例完善

## 讨论
当某data_id正在加载（在load_que中/正在被load）时，接收到别的用户对相同id的请求，服务能够正确处理吗？：
- 正在被加载，说明还没实际加载到cache中，也就是正在跑实际的load函数或正在load队列中等待
- 以上的这两种情况，`cache_order` 中都已经更新了该id的位置，最终 `request_load` 方法都会走到 `ready_to_load` 方法正确的更新引用计数
- 反之，如果不是正在被加载，那 `cache` 已经有数据，同样也会正确更新引用计数