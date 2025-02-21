# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import time

from tqdm import tqdm

from examples.config import FILE_1t, FILE_1m, FILE_1d, TOTAL_1t, TOTAL_1m, TOTAL_1d, MINUTE1, TOTAL_5m, FILE_5m
from qmt_quote.dtypes import DTYPE_STOCK_1t, DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils_bar import BarManager, get_label_86400, get_label_60, get_label_300

arr1t1, arr1t2 = get_mmap(FILE_1t, DTYPE_STOCK_1t, TOTAL_1t, readonly=True)
arr1d1, arr1d2 = get_mmap(FILE_1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=False)
arr1m1, arr1m2 = get_mmap(FILE_1m, DTYPE_STOCK_1m, TOTAL_1m, readonly=False)
arr5m1, arr5m2 = get_mmap(FILE_5m, DTYPE_STOCK_1m, TOTAL_5m, readonly=False)

slice_1t = SliceUpdater(min1=MINUTE1, overlap_ratio=3, step_ratio=30)

if __name__ == "__main__":
    print()
    print(f"Tick数据转分钟数据, CTRL+C退出")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar = tqdm(total=TOTAL_1m, desc="股票/指数", initial=0, bar_format=bar_format, ncols=80)

    bm_1d = BarManager(arr1d1, arr1d2, False)
    bm_1m = BarManager(arr1m1, arr1m2, True)
    bm_5m = BarManager(arr5m1, arr5m2, True)

    while True:
        start, end, cursor = slice_1t.update(int(arr1t2[0]))
        if start == end:
            time.sleep(5)
            continue

        arr_stk = arr1t1[slice_1t.for_next()]
        # df = arr_to_pl(arr_stk, pl.col('time')).to_pandas()
        if len(arr_stk) > 0:
            start, end, step = bm_1d.extend(arr_stk, get_label_86400, 3600 * 8)
            start, end, step = bm_5m.extend(arr_stk, get_label_300, 3600 * 8)
            start, end, step = bm_1m.extend(arr_stk, get_label_60, 3600 * 8)

            if step > 0:
                pbar.update(step)
        else:
            time.sleep(3)
