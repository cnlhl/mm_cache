## To do
- [x] 用户侧：封装更高层次的读取方法，支持逐股票筛选
- [x] 服务侧：缓存淘汰方法完善
- [x] 服务侧：增加请求队列，使能够同时处理多个请求
- [x] 用户侧：长轮询机制，等待服务侧加载
- [x] 用户侧：类销毁减引用和清理操作
- [ ] 服务侧：增加日志记录
- [ ] 服务侧：异常处理和测试用例完善
- [ ] 服务测：多进程并发处理

## 讨论
当某data_id正在加载（在load_que中/正在被load）时，接收到别的用户对相同id的请求，服务能够正确处理吗？：
- 正在被加载，说明还没实际加载到cache中，也就是正在跑实际的load函数或正在load队列中等待
- 以上的这两种情况，`cache_order` 中都已经更新了该id的位置，最终 `request_load` 方法都会走到 `ready_to_load` 方法正确的更新引用计数
- 反之，如果不是正在被加载，那 `cache` 已经有数据，同样也会正确更新引用计数