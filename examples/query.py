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
from loguru import logger

from examples.config import FILE_1m, FILE_1d, TOTAL_1m, TOTAL_1d, HISTORY_STOCK_1d, HISTORY_STOCK_1m, TICKS_PER_MINUTE, FILE_5m, TOTAL_5m, HISTORY_STOCK_5m
from factor_calc import main
from qmt_quote.dtypes import DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils import arr_to_pl, calc_factor1, concat_interday

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

if __name__ == "__main__":
    last_time = -1
    while True:
        # TODO 屏蔽输入，可以用定时触发
        if True:
            x = input("输入`:q`退出；输入其它键打印最新数据\n")
            if x == ":q":
                break

        # # TODO 替换成时间判断语句，就可以每分钟定时触发了
        # if False:
        #     curr_time = datetime.now().minute
        #     if curr_time == last_time:
        #         time.sleep(1)
        #         continue
        #     last_time = curr_time
        #     logger.info(curr_time)

        # 更新当前位置
        start, end, current = slice_1d.update(int(arr1d2[0]))
        start, end, current = slice_5m.update(int(arr5m2[0]))
        start, end, current = slice_1m.update(int(arr1m2[0]))
        logger.info("{}, {}, {}", start, end, current)

        logger.info("1分钟==================")
        arr = arr1m1[slice_1m.for_all()]
        arr = arr[arr['type'] == 1]  # 过滤掉指数，只处理股票
        df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
        df = concat_interday(his_stk_1m, df)
        df = calc_factor1(df)
        df = main(df)
        logger.info("==================")
        print(df.filter(pl.col('stock_code') == '000001.SZ').to_pandas())

        logger.info("日线==================")
        arr = arr1d1[slice_1d.for_all()]
        arr = arr[arr['type'] == 1]
        df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
        df = concat_interday(his_stk_1d, df)
        df = calc_factor1(df)
        df = main(df)
        logger.info("==================")
        print(df.filter(pl.col('stock_code') == '000001.SZ').to_pandas())

        logger.info("5分钟==================")
        arr = arr5m1[slice_5m.for_all()]
        arr = arr[arr['type'] == 1]
        df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
        df = concat_interday(his_stk_5m, df)
        df = calc_factor1(df)
        df = main(df)
        logger.info("==================")
        print(df.filter(pl.col('stock_code') == '000001.SZ').to_pandas())
