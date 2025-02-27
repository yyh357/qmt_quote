"""
数据转换Tick数据转各种数据
1. Tick转1分钟
2. Tick转1天
3. Tick转5分钟

用户可以定制此代码

可盘中开启，会重新转换
"""
import time
from datetime import datetime

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
    pbar = tqdm(total=TOTAL_1t, desc="股票+指数", initial=0, bar_format=bar_format, ncols=100)

    bm_1d = BarManager(arr1d1, arr1d2, False, True)
    bm_1m = BarManager(arr1m1, arr1m2, True, True)
    bm_5m = BarManager(arr5m1, arr5m2, True, False)

    last_cursor = -1
    while True:
        start, end, cursor = slice_1t.update(int(arr1t2[0]))
        if last_cursor == cursor:
            time.sleep(0.5)
            continue
        last_cursor = cursor

        # tick数据顺序调用，每一条不会重复使用
        arr1t = arr1t1[slice_1t.for_next()]

        now = datetime.now().timestamp()
        t = arr1t[-1]['now'] / 1000
        pbar.update(arr1t.size)
        pbar.set_description(f"延时 {now - t:8.3f}s")

        start1, end1, step1 = bm_5m.extend_ticks(arr1t, get_label_stock_5m, 3600 * 8)
        start2, end2, step2 = bm_1d.extend_ticks(arr1t, get_label_stock_1d, 3600 * 8)
        start3, end3, step3 = bm_1m.extend_ticks(arr1t, get_label_stock_1m, 3600 * 8)
        if step3 == 0:
            time.sleep(0.5)
            continue
