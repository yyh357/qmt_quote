# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import time

from tqdm import tqdm

from examples.config import FILE_1t, FILE_1m, FILE_1d, TOTAL_1t, TOTAL_1m, TOTAL_1d, TICKS_PER_MINUTE, TOTAL_5m, FILE_5m, TOTAL_ASSET
from qmt_quote.dtypes import DTYPE_STOCK_1t, DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.mmap_bars import BarManager, get_label_stock_1d, get_label_stock_1m, get_label_stock_5m

arr1t1, arr1t2 = get_mmap(FILE_1t, DTYPE_STOCK_1t, TOTAL_1t, readonly=True)
arr1d1, arr1d2 = get_mmap(FILE_1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=False)
arr1m1, arr1m2 = get_mmap(FILE_1m, DTYPE_STOCK_1m, TOTAL_1m, readonly=False)
arr5m1, arr5m2 = get_mmap(FILE_5m, DTYPE_STOCK_1m, TOTAL_5m, readonly=False)

slice_1t = SliceUpdater(min1=TICKS_PER_MINUTE, overlap_ratio=3, step_ratio=30)
slice_1m = SliceUpdater(min1=TOTAL_ASSET, overlap_ratio=3, step_ratio=30)

if __name__ == "__main__":
    print()
    print(f"Tick数据转分钟数据。可盘中多次开启关闭。CTRL+C退出")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar_1t = tqdm(total=TOTAL_1m, desc="股票+指数 来自1t", initial=0, bar_format=bar_format, ncols=80)
    # pbar_1m = tqdm(total=TOTAL_5m, desc="股票/指数 来自1m", initial=0, bar_format=bar_format, ncols=80)

    bm_1d = BarManager(arr1d1, arr1d2, False, True)
    bm_1m = BarManager(arr1m1, arr1m2, True, True)
    bm_5m = BarManager(arr5m1, arr5m2, True, False)

    while True:
        start, end, cursor = slice_1t.update(int(arr1t2[0]))
        if start == end:
            time.sleep(5)
            continue

        # tick数据顺序调用，每一条不会重复使用
        arr1t = arr1t1[slice_1t.for_next()]
        if len(arr1t) == 0:
            time.sleep(5)
            continue

        # 屏蔽tick转5分钟示例，改演示用1分钟转5分钟
        start1, end1, step1 = bm_5m.extend_ticks(arr1t, get_label_stock_5m, 3600 * 8)
        start2, end2, step2 = bm_1d.extend_ticks(arr1t, get_label_stock_1d, 3600 * 8)
        start3, end3, step3 = bm_1m.extend_ticks(arr1t, get_label_stock_1m, 3600 * 8)
        if step3 > 0:
            pbar_1t.update(step3)
            time.sleep(0.5)
        else:
            time.sleep(2)

        # 1分钟数据转5分钟数据，由于要用全量1m数据，感觉效率低,但可用在非实盘环境
        test_5m = False
        if test_5m:
            start, end, cursor = slice_1m.update(int(arr1m2[0]))
            arr1m = arr1m1[slice_1m.for_all()]
            bm_5m.reset()
            start, end, step = bm_5m.extend_bars(arr1m, get_label_stock_5m, 3600 * 8)
