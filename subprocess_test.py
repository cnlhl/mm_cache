import pandas as pd
import multiprocessing as mp
import multiprocessing.shared_memory as sm
import numpy as np
import os
import time

def process_1(shm_name, shape, dtype):
    """打印DataFrame的head"""
    existing_shm = sm.SharedMemory(name=shm_name)
    df = pd.DataFrame(np.ndarray(shape=shape, dtype=dtype, buffer=existing_shm.buf))
    print("Process 1: DataFrame Head\n", df.head())
    existing_shm.close()

def process_2(shm_name, shape, dtype):
    """打印DataFrame的大小"""
    existing_shm = sm.SharedMemory(name=shm_name)
    df = pd.DataFrame(np.ndarray(shape=shape, dtype=dtype, buffer=existing_shm.buf))
    print("Process 2: DataFrame Shape", df.shape)
    existing_shm.close()

def process_3(shm_name):
    """确认共享内存是否还存在"""
    try:
        existing_shm = sm.SharedMemory(name=shm_name)
        print("Process 3: Shared memory still exists.")
        existing_shm.close()
    except FileNotFoundError:
        print("Process 3: Shared memory does not exist.")


if __name__ == '__main__':
    # 读取HDF文件
    try:
        df = pd.read_hdf('/home/Level2/2023/20230104/sh600030.h5', 'order')
    except FileNotFoundError:
        print("Error: HDF5 file not found. Please make sure the path is correct.")
        exit()
    except Exception as e:
        print(f"Error reading HDF5 file: {e}")
        exit()

    # 将DataFrame转换为NumPy数组并获取其形状和数据类型
    np_array = df.values
    shape = np_array.shape
    dtype = np_array.dtype

    # 创建共享内存
    shm = sm.SharedMemory(create=True, size=np_array.nbytes)

    # 将NumPy数组复制到共享内存
    shared_array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    shared_array[:] = np_array[:]

    with mp.Pool(processes=3) as pool: # 创建一个进程池，进程数为3
        results = []

        # 异步提交任务
        r1 = pool.apply_async(process_1, (shm.name, shape, dtype))
        results.append(r1)
        print("Main process: Submitted process 1.")

        r2 = pool.apply_async(process_2, (shm.name, shape, dtype))
        results.append(r2)
        print("Main process: Submitted process 2.")

        r3 = pool.apply_async(process_3, (shm.name,))
        results.append(r3)
        print("Main process: Submitted process 3.")

        print("Main process: Continuing execution without waiting for child processes.")

        # 等待所有任务完成 (非常重要！)
        for r in results:
            r.get() # 获取结果，这会阻塞直到任务完成

    print("Main process: All child processes (managed by Pool.apply_async) have finished.")

    # 清理共享内存
    shm.close()
    shm.unlink()
    print("Shared Memory cleaned up")
