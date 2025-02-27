"""
实盘策略

"""
import time
from datetime import datetime

r"""
此文件一般已经放到了用户的项目目录下了，但qmt_quote由于过于简单，并没有发布到pypi。
有两种方式可以使用，选用一种即可。

1. 手动添加到sys.path中，简单粗暴。但代码运行中才添加，所以IDE无法识别会有警告
```
import sys
sys.path.insert(0, r"D:\GitHub\qmt_quote")
```

2. 到`D:\Users\Kan\miniconda3\envs\py312\Lib\site-packages`目录下，
   新建一个`qmt_quote.pth`文件，IDE可识别，内容为：
```
D:\GitHub\qmt_quote
```

"""
import pandas as pd
import polars as pl

from examples.config import FILE_1m, FILE_1d, TOTAL_1m, TOTAL_1d, HISTORY_STOCK_1d, HISTORY_STOCK_1m, TICKS_PER_MINUTE, FILE_5m, TOTAL_5m, HISTORY_STOCK_5m, FILE_1t, TOTAL_1t
from factor_calc import main
from qmt_quote.dtypes import DTYPE_STOCK_1m, DTYPE_STOCK_1t
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils import arr_to_pl, calc_factor1, concat_interday

arr1t1, arr1t2 = get_mmap(FILE_1t, DTYPE_STOCK_1t, TOTAL_1t, readonly=True)
arr1d1, arr1d2 = get_mmap(FILE_1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=True)
arr1m1, arr1m2 = get_mmap(FILE_1m, DTYPE_STOCK_1m, TOTAL_1m, readonly=True)
arr5m1, arr5m2 = get_mmap(FILE_5m, DTYPE_STOCK_1m, TOTAL_5m, readonly=True)

# 约定df1存1分钟数据，df2存日线数据
slice_1d = SliceUpdater(min1=TOTAL_1d, overlap_ratio=3, step_ratio=30)
slice_1m = SliceUpdater(min1=TICKS_PER_MINUTE, overlap_ratio=3, step_ratio=30)
slice_5m = SliceUpdater(min1=TICKS_PER_MINUTE * 5, overlap_ratio=3, step_ratio=30)

# 加载历史数据
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


def load_history_data():
    # 历史日线，只设置一次，当天不再更新
    his_stk_1d = (
        pl.read_parquet(HISTORY_STOCK_1d)
        .filter(pl.col('suspendFlag') == 0)
        .with_columns(
            pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
            pl.col('volume').cast(pl.UInt64),
        )
    )

    # 历史分钟线，只设置一次，当天不再更新
    his_stk_1m = (
        pl.read_parquet(HISTORY_STOCK_1m)
        .filter(pl.col('suspendFlag') == 0)
        .with_columns(
            pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
            pl.col('volume').cast(pl.UInt64),
        )
    )
    # 历史5分钟线，只设置一次，当天不再更新
    his_stk_5m = (
        pl.read_parquet(HISTORY_STOCK_5m)
        .filter(pl.col('suspendFlag') == 0)
        .with_columns(
            pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
            pl.col('volume').cast(pl.UInt64),
        )
    )
    return his_stk_1d, his_stk_1m, his_stk_5m


# 仅当日
his_stk_1d, his_stk_1m, his_stk_5m = None, None, None

# 取历史
his_stk_1d, his_stk_1m, his_stk_5m = load_history_data()


def process_1m(arr1m1, slice_1m, his_stk_1m, filter_1m):
    arr = arr1m1[slice_1m.for_all()]
    arr = arr[arr['type'] == 1]  # 过滤掉指数，只处理股票
    # TODO 左标签过滤，分钟线已经完成，如取实时K线, 注释此行即可
    arr = arr[arr['time'] <= filter_1m]
    df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
    df = concat_interday(his_stk_1m, df)
    df = calc_factor1(df)
    df = main(df)
    return df


def process_1d(arr1d1, slice_1d, his_stk_1d):
    arr = arr1d1[slice_1d.for_all()]
    arr = arr[arr['type'] == 1]  # 过滤掉指数，只处理股票
    df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
    df = concat_interday(his_stk_1d, df)
    df = calc_factor1(df)
    df = main(df)
    return df


if __name__ == "__main__":
    last_time = -1
    while True:
        # 调整成成分钟标签，当前分钟还在更新
        curr_time = datetime(2025, 2, 27, 15, 0).timestamp() // 60 * 60
        # curr_time = datetime.now().timestamp() // 60 * 60
        if curr_time == last_time:
            time.sleep(0.5)
            continue
        last_time = curr_time
        # 过滤时间。调整成成分钟标签，是取当前更新中的K线，还是去上一根不变的K线
        filter_1m = (curr_time // 60 * 60 - 60) * 1000
        # 8时区处理
        filter_1d = (curr_time // 86400 * 86400 - 3600 * 8) * 1000

        # 更新当前位置
        slice_1d.update(int(arr1d2[0]))  # 日线
        slice_1m.update(int(arr1m2[0]))  # 1分钟

        df = process_1d(arr1d1, slice_1d, his_stk_1d)
        df = df.filter(pl.col('time').dt.timestamp(time_unit='ms') == filter_1d)
        df = df.filter(pl.col('OUT'))
        print(df.select("stock_code", "time", pl.exclude("stock_code", "time")))

        df = process_1m(arr1m1, slice_1m, his_stk_1m, filter_1m)
        # 这种取法，能有效避免没有行情的股票也被选出
        df = df.filter(pl.col('time').dt.timestamp(time_unit='ms') == filter_1m)
        df = df.filter(pl.col('OUT'))
        print(df.select("stock_code", "time", pl.exclude("stock_code", "time")))
