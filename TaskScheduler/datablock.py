import os
import pandas as pd

class DataBlock:
    def __init__(self, db_id):
        date, stock, self.table = db_id.split('_')
        self.file_path = os.path.join('/home/Level2/2023', date, f'{stock}.h5')
        self.size = os.path.getsize(self.file_path)
        self.in_memory = False
        self.ref_count = 0
        self.data = None

    def load(self):
        if not self.in_memory:
            self.data = pd.read_hdf(self.file_path, key=self.table)
            self.in_memory = True

    def unload(self):
        if self.in_memory:
            # 卸载数据，释放内存
            del self.data
            self.in_memory = False

    def increase_ref(self):
        self.ref_count += 1

    def decrease_ref(self):
        if self.ref_count > 0:
            self.ref_count -= 1