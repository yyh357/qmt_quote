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

from config import FILE_INDEX_1t, TOTAL_INDEX_1t, MINUTE1_INDEX, HISTORY_STOCK_1m, FILE_STOCK_1m, TOTAL_STOCK_1m, FILE_STOCK_1d, TOTAL_STOCK_1d  # noqa
from config import FILE_STOCK_1t, TOTAL_STOCK_1t, MINUTE1_STOCK, HISTORY_STOCK_1d  # noqa
from factor_calc import main
from qmt_quote.dtypes import DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils import arr_to_pl, calc_factor, concat_interday

stk1, stk2 = get_mmap(FILE_STOCK_1m, DTYPE_STOCK_1m, TOTAL_STOCK_1m, readonly=True)
stk3, stk4 = get_mmap(FILE_STOCK_1d, DTYPE_STOCK_1m, TOTAL_STOCK_1d, readonly=True)

# 约定df1存1分钟数据，df2存日线数据
slice_stk_1m = SliceUpdater(min1=6000 * 5, overlap_ratio=3, step_ratio=30)
slice_stk_1d = SliceUpdater(min1=6000, overlap_ratio=3, step_ratio=30)
# 加载历史数据
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

use_history = True
if use_history:
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
else:
    his_stk_1d = None
    his_stk_1m = None

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
        start1, end1, current1 = slice_stk_1m.update(int(stk2[0]))
        start2, end2, current2 = slice_stk_1d.update(int(stk4[0]))
        logger.info("{}, {}, {}", start2, end2, current2)

        logger.info("分钟==================")
        arr = stk1[slice_stk_1m.for_all()]
        df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
        df = concat_interday(df, his_stk_1m)
        df = calc_factor(df)
        df = main(df)
        logger.info("==================")
        # print(df.filter(pl.col('stock_code') == '000001.SZ').to_pandas())

        logger.info("日线==================")
        arr = stk3[slice_stk_1d.for_all()]
        # print(arr)
        df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
        df = concat_interday(his_stk_1d, df)
        df = calc_factor(df)
        df = main(df)
        logger.info("==================")
        # print(df.filter(pl.col('stock_code') == '000001.SZ').to_pandas())
