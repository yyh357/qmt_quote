"""
数据转换Tick数据转各种数据
1. Tick转1分钟
2. Tick转1天
3. Tick转5分钟

用户可以定制此代码

可盘中开启，会重新转换

本项目一定要开启，因为实盘策略利用它生成的日线数据下单
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from loguru import logger
from npyt import NPYT
from tqdm import tqdm

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import (FILE_d1d, TOTAL_1d, FILE_d1m, TOTAL_1m, TOTAL_5m, FILE_d5m, TICKS_PER_MINUTE,
                             FILE_d1t, FILE_d1t_H1)
from qmt_quote.bars.labels import get_label_stock_5m, get_label_stock_1m
from qmt_quote.bars.tick_day import BarManager as BarManagerD
from qmt_quote.bars.tick_minute import BarManager as BarManagerM
from qmt_quote.dtypes import DTYPE_STOCK_1m, DTYPE_STOCK_1t

# 历史TICK数据+实时TICK数据,如果计算数据要求提前加载几天的数据，可以多加几天
dt1s = [FILE_d1t_H1, FILE_d1t]
DAYS = len(dt1s)
# 分种级别数据，将历史TICK和实盘TICK拼接成一个实盘分钟级别数据
d1m = NPYT(FILE_d1m, dtype=DTYPE_STOCK_1m).save(capacity=TOTAL_1m * DAYS).load(mmap_mode="r+")
d5m = NPYT(FILE_d5m, dtype=DTYPE_STOCK_1m).save(capacity=TOTAL_5m * DAYS).load(mmap_mode="r+")
# !!! 当日开高低收快照，用于交易系统下单时取当前行情，所以每天起始位置要重置
d1d = NPYT(FILE_d1d, dtype=DTYPE_STOCK_1m).save(capacity=TOTAL_1d).load(mmap_mode="r+")


def do(file, is_live=False):
    """

    Parameters
    ----------
    file : str
        原始tick数据文件
    is_live
        是否是实盘，实盘会等待数据，历史会推出

    """
    # 文件不存在，直接返回
    if not os.path.exists(file):
        logger.error("数据文件 {} 不存在，直接返回", file)
        return

    d1t = NPYT(file, dtype=DTYPE_STOCK_1t).load(mmap_mode="r")

    # 接着昨天数据生成新K线
    bm_d1m = BarManagerM(d1m._a, d1m._t)
    bm_d5m = BarManagerM(d5m._a, d5m._t)
    # 日线快照数据要重置
    d1d.clear()
    bm_d1d = BarManagerD(d1d._a, d1d._t)

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    # 昨天的行情数据放一起
    pbar = tqdm(total=d1t.capacity(), desc="股票+指数", initial=0, bar_format=bar_format, ncols=100)
    while True:
        # tick数据顺序调用，每一条不会重复使用
        a1t = d1t.read(n=TICKS_PER_MINUTE, prefetch=0)
        if len(a1t) == 0:
            # 没有新数据要更新，等等
            if is_live:
                time.sleep(0.5)
                continue
            else:
                break

        now = datetime.now().timestamp()
        t = a1t[-1]['now'] / 1000
        pbar.update(a1t.size)
        # 这里要refresh，否则看起来行情延时很大
        pbar.set_description(f"延时 {now - t:8.3f}s", refresh=True)

        bm_d1d.extend(a1t, 3600 * 8)
        bm_d5m.extend(a1t, get_label_stock_5m, 3600 * 8)
        _, _, step = bm_d1m.extend(a1t, get_label_stock_1m, 3600 * 8)
        # if step == 0:
        #     # 如果没有产生新的一分钟K线，就等等
        #     if is_live:
        #         time.sleep(0.5)


if __name__ == "__main__":
    print()
    print(f"Tick数据转分钟数据。可盘中多次开启关闭。CTRL+C退出")
    print()
    print("=" * 60)
    print(f"分钟数据当前指针：{d1m.end()}")

    d1m.clear()
    d5m.clear()
    d1d.clear()

    print("!!!重置文件指针成功!!!")

    for i, file in enumerate(dt1s):
        print("=" * 60)
        do(file, is_live=i == DAYS - 1)
        print()
