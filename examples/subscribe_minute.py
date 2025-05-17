"""
数据转换Tick数据转各种数据
1. Tick转1分钟
2. Tick转1天
3. Tick转5分钟

用户可以定制此代码

可盘中开启，会重新转换

本项目一定要开启，因为实盘策略利用它生成的日线数据下单
"""
import sys
import time
from datetime import datetime
from pathlib import Path

from npyt import NPYT
from tqdm import tqdm

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import FILE_d1t, FILE_d1d, TOTAL_1d, FILE_d1m, TOTAL_1m, TOTAL_5m, FILE_d5m, TICKS_PER_MINUTE
from qmt_quote.bars.labels import get_label_stock_5m, get_label_stock_1m
from qmt_quote.bars.tick_day import BarManager as BarManagerD
from qmt_quote.bars.tick_minute import BarManager as BarManagerM
from qmt_quote.dtypes import DTYPE_STOCK_1m

d1t = NPYT(FILE_d1t).load(mmap_mode="r")
d1d = NPYT(FILE_d1d).save(dtype=DTYPE_STOCK_1m, capacity=TOTAL_1d).load(mmap_mode="r+")
d1m = NPYT(FILE_d1m).save(dtype=DTYPE_STOCK_1m, capacity=TOTAL_1m).load(mmap_mode="r+")
d5m = NPYT(FILE_d5m).save(dtype=DTYPE_STOCK_1m, capacity=TOTAL_5m).load(mmap_mode="r+")

if __name__ == "__main__":
    print()
    print(f"Tick数据转分钟数据。可盘中多次开启关闭。CTRL+C退出")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar = tqdm(total=d1t.capacity(), desc="股票+指数", initial=0, bar_format=bar_format, ncols=100)

    bm_d1d = BarManagerD(d1d._a, d1d._t)
    bm_d1m = BarManagerM(d1m._a, d1m._t)
    bm_d5m = BarManagerM(d5m._a, d5m._t)

    while True:
        # tick数据顺序调用，每一条不会重复使用
        a1t = d1t.read(n=TICKS_PER_MINUTE, prefetch=0)
        if len(a1t) == 0:
            # 没有新数据要更新，等等
            time.sleep(0.5)
            continue

        now = datetime.now().timestamp()
        t = a1t[-1]['now'] / 1000
        pbar.update(a1t.size)
        # 这里要refresh，否则看起来行情延时很大
        pbar.set_description(f"延时 {now - t:8.3f}s", refresh=True)

        bm_d1d.extend(a1t, 3600 * 8)
        bm_d5m.extend(a1t, get_label_stock_5m, 3600 * 8)
        _, _, step = bm_d1m.extend(a1t, get_label_stock_1m, 3600 * 8)
        if step == 0:
            # 如果没有产生新的一分钟K线，就等等
            time.sleep(0.5)
            continue
