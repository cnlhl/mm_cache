import pandas as pd

from level2engine import Level2Engine


class BigOrder(Level2Engine):
    def __init__(self, param: dict):
        super().__init__(param)
        self._threshold = param.get('threshold', 0.9)

    def on_calculate(self, data_cache):
        if 'order' not in data_cache:
            return pd.DataFrame()
        
        order = data_cache['order']
        try:
            res = pd.Series(dtype=float)
            big_ord = order[order['order_volume'] >= order['order_volume'].quantile(self._threshold)]
            res['big_order'] = big_ord[big_ord['bs_flag'] == 0]['order_volume'].sum() / order['order_volume'].sum()
            return pd.DataFrame(res).T
        except:
            return pd.DataFrame()


if __name__ == '__main__':
    big_ord = BigOrder({'map_dir': '/home/sharedriver/data/cn_lvl2_map', 'o_dir': '/home/haolinl/test_res/big_order'})
    big_ord.run_task(20230901, 20231230, core=10)



