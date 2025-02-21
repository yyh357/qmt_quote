# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import time

import pandas as pd
from tqdm import tqdm

from examples.config import FILE_STOCK_1t, TOTAL_STOCK_1t, MINUTE1_STOCK, FILE_STOCK_1m, TOTAL_STOCK_1m, FILE_INDEX_1t, TOTAL_INDEX_1t, FILE_INDEX_1m, TOTAL_INDEX_1m, MINUTE1_INDEX, FILE_STOCK_1d, TOTAL_STOCK_1d
from qmt_quote.dtypes import DTYPE_STOCK_1t, DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils_bar import get_label_60_qmt, BarManager, get_label_86400

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

stk1, stk2 = get_mmap(FILE_STOCK_1t, DTYPE_STOCK_1t, TOTAL_STOCK_1t, readonly=True)
stk3, stk4 = get_mmap(FILE_STOCK_1m, DTYPE_STOCK_1m, TOTAL_STOCK_1m, readonly=False)
stk5, stk6 = get_mmap(FILE_STOCK_1d, DTYPE_STOCK_1m, TOTAL_STOCK_1d, readonly=False)

idx1, idx2 = get_mmap(FILE_INDEX_1t, DTYPE_STOCK_1t, TOTAL_INDEX_1t, readonly=True)
idx3, idx4 = get_mmap(FILE_INDEX_1m, DTYPE_STOCK_1m, TOTAL_INDEX_1m, readonly=False)

slice_stk_1m = SliceUpdater(min1=MINUTE1_STOCK, overlap_ratio=3, step_ratio=30)
slice_idx_1m = SliceUpdater(min1=MINUTE1_INDEX, overlap_ratio=3, step_ratio=30)
slice_stk_1d = SliceUpdater(min1=6000, overlap_ratio=3, step_ratio=30)

if __name__ == "__main__":
    print()
    print(f"Tick数据转1分钟数据, CTRL+C退出")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar_stk = tqdm(total=TOTAL_STOCK_1m, desc="股票", initial=0, bar_format=bar_format, ncols=80)
    pbar_idx = tqdm(total=TOTAL_INDEX_1m, desc="指数", initial=0, bar_format=bar_format, ncols=80)

    bm_stk_1m = BarManager(stk3, stk4, True)
    bm_idx_1m = BarManager(idx3, idx4, True)
    bm_stk_1d = BarManager(stk5, stk6, False)

    while True:
        start1, end1, cursor1 = slice_stk_1m.update(int(stk2[0]))
        start2, end2, cursor2 = slice_idx_1m.update(int(stk4[0]))
        start3, end3, cursor3 = slice_stk_1d.update(int(stk6[0]))
        if (start1 == end1) and (start2 == end2):
            time.sleep(5)
            continue

        arr_stk = stk1[slice_stk_1m.for_next()]
        arr_idx = idx1[slice_idx_1m.for_next()]
        step1, step2 = 0, 0
        if len(arr_stk) > 0:
            start1, end1, step1 = bm_stk_1m.extend(arr_stk, get_label_60_qmt, 3600 * 8)
            start3, end3, step3 = bm_stk_1d.extend(arr_stk, get_label_86400, 3600 * 8)
            if step1 > 0:
                pbar_stk.update(step1)
        if len(arr_idx) > 0:
            start2, end2, step2 = bm_idx_1m.extend(arr_idx, get_label_60_qmt, 3600 * 8)
            if step2 > 0:
                pbar_idx.update(step2)

        if step1 == 0 and step2 == 0:
            time.sleep(3)
