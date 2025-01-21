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

### 查看cached中已加载的数据：

```python
# 查看cached中已加载的数据
cached = data_loader.get_cached_items()
print(cached)
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
- [ ] 服务测: 支持主动加载
- [ ] 服务测：测试用例完善

## 讨论
### 当某data_id正在加载（在load_que中/正在被load）时，接收到别的用户对相同id的请求，服务能够正确处理吗？：
- 正在被加载，说明还没实际加载到cache中，也就是正在跑实际的load函数或正在load队列中等待
- 以上的这两种情况，`cache_order` 中都已经更新了该id的位置，最终 `request_load` 方法都会走到 `ready_to_load` 方法正确的更新引用计数
- 反之，如果不是正在被加载，那 `cache` 已经有数据，同样也会正确更新引用计数

### 被动触发加载和主动加载数据到cache中，哪一种更快？
假设：总的请求文件数为n，文件总数为m，加载一个文件的IO耗时为t，cache大小为c；所有请求都并发的同时到达；
- 当cache大小可以容纳下全量数据时：（c>m>n）
    - 主动加载：一次IO，完成所有数据加载；总IO耗时取决于全量文件大小
    - 被动加载：多次IO，每次只加载被需要的数据；总IO耗时取决于被请求文件大小
    从IO耗时来说，被动加载的数据量小于主动加载，耗时更短
    从访问等待来说：
        - 被动加载，第一次访问需要等待对应文件的IO时间，后续重复访问无IO等待
        - 主动加载，如果在加载时请求，需要等待所有数据加载时间；如果在加载后访问，无IO等待
- 当cache大小无法容纳全量数据时，但可以容纳被请求的数据时：（m>c>n）
    - 同样的，从IO耗时来说，被动加载的数据量小于主动加载，耗时更短
    从访问等待来说：
    - 被动加载，第一次访问需要等待对应文件的IO时间，后续访问无IO等待
    - 主动加载，对于第一批能够加载到内存中的数据来说，同上一种情况；对于未能被第一次加载满足的数据请求：相当于在第一批加载的基础上，至少等待再次将cache填满的耗时，才能获取所请求数据，也即必然存在相应的IO等待时间
        - 被动加载的访问等待：`n*t` ；主动加载的访问等待：如果请求数据为连续的能被完整加载到cache中，且命中第一次预加载数据，则完成IO后无需等待，否则至少等待`c*t`至多等待`(m-1)*t`
- 当cache大小无法容纳被请求数据时：（m>n>c）
    IO耗时：同上
    访问等待：
        - 被动加载：`n*t`
        - 主动加载：由于n>c，也就意味着必然有一部分数据需要忍受cache被flush然后加载下一批数据的IO开销，则至少`c*t`至多等待`(m-1)*t`

更进一步的，当请求是异步抵达，无法在一开始得知所有会被请求数据时：
引入请求次数r，r一定大于等于n，可能大于m和c
- 当cache大小可以容纳下全量数据时：（c>m）
    同上
- 去重后请求大小大于cache大小时：（n>c）
    - 被动加载：
        - 最坏场景：重复的数据请求在cache数据项的生命周期外到达（到达时缓存已被flush），需要重新加载：`r*t`
        - 最优场景：同并发到达，可以优先满足被请求次数最多的数据 ：`n*t`
    - 主动加载：
        - 最坏场景：每个请求到达时，都恰好请求的是上一个刚被cache flush的数据：`r*m*t`
        - 最优场景：每个请求到达时，恰好需要的数据都在cache中，且请求的数据是被连续加载的，不存在无用数据：`n*t`
        - 平均来说：假设将总文件大小划分为cache大小的数据块，也就是1-m/c块数据，如果请求随机的打到这些数据块上，其落在当前cache中数据之前和之后的概率相等，则最终的平均等待期望为: `r*[(m/c)*c*t]/2`，也即 `(r*m*t)/2` 

