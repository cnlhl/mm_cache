import shelve
import time
import bisect
import pandas as pd
import multiprocessing as mp
from functools import partial
import os
import logging

from cache_client import CacheClient

def create_path(path_):
    os.makedirs(os.path.dirname(path_), exist_ok=True)
    return path_

class Level2Engine:
    def __init__(self, param: dict):
        self._cache_client = CacheClient()
        self._data_types = param.get('data_types', ['order'])
         
        self._map_dir = param.get('map_dir', '/home/sharedriver/ProdData/cn_lvl2_map')
        self._map = shelve.open(self._map_dir, 'r')
        
        self._o_dir = param.get('o_dir', '')
        
        self._code_column = param.get('code_column', 'cn_code')
        self._date_column = param.get('date_column', 'date')
        self._index_columns = [self._code_column, self._date_column]

    def on_calculate(self, data_cache):
        raise NotImplementedError
    
    def _get_cached_days(self, data_types):
        if isinstance(data_types, str):
            data_types = [data_types]
        return self._cache_client.check(date_only=True)
        
    
    def _calculate(self, code_, date_):
        try:
            s_t = time.time()
            data_cache = {t: self._cache_client.get(t, date_, code_) for t in self._data_types}
            res = self.on_calculate(data_cache)
            if len(res) > 0:
                res[self._code_column] = int(code_)
            return res
        except Exception as e_msg:
            return pd.DataFrame()
    
    def run_task(self, start_d, end_d, core=-1):
        ks = list(self._map.keys())
        calc_days = ks[bisect.bisect_left(ks, str(start_d)) : bisect.bisect_left(ks, str(end_d)) + 1]
        pool = mp.Pool(core) if core>0 else None

        while len(calc_days) > 0:
            cached_days = self._get_cached_days(self._data_types)
            finished_days = set()
            for type in self._data_types:
                type_cached_days = cached_days.get(type, [])[1:]
                for d in type_cached_days:
                    if d not in calc_days:
                        continue

                    s_t = time.time()
                    codes = self._map.get(d)
                    cached_ids = self._cache_client.check(type, date_only=False)
                    cached_d_ids = [c for c in cached_ids if d in c]

                    if len(cached_d_ids)/len(codes) < 0.8:
                        logging.warning(f'{d} is not ready')
                        continue
                    
                    s_t_1 = time.time()

                    if core <= 0:
                        res_d = pd.concat([self._calculate(code,d) for code in codes])
                    else:
                        calc_params = [(code, d) for code in codes]
                        map_res = pool.starmap(self._calculate, calc_params)
                        res_d = pd.concat(map_res)
                        del map_res
                    
                    logging.warning(f'calc {d} in {time.time() - s_t_1:.2f}s')

                    if len(res_d) > 0:
                        res_d[self._date_column] = d
                        res_d[self._index_columns] = res_d[self._index_columns].astype(int)
                        
                        o_path = create_path(os.path.join(self._o_dir, f'{int(d)//10000}/{d}.txt.gz'))
                        res_d.to_csv(o_path, sep='\t', compression='gzip')
                        logging.warning('{} is done in {}s'.format(d, time.time() - s_t))

                        finished_days.add(d)

            calc_days = list(set(calc_days) - finished_days)
            time.sleep(1)


        if pool is not None:
            pool.close()
            pool.join()

