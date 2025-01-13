def strategy_func1(data_cache):
    # 从数据缓存中获取数据
    data = data_cache["20230103_sh600030_order"].data
    data.to_csv("20230103_sh600030_order.csv")
    return "策略1结果"

def strategy_func2(data_cache):
    # 从数据缓存中获取数据
    data = data_cache["data_20231010_stockB.h5"]
    # 执行策略计算
    # ...
    return "策略2结果"