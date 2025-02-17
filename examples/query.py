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
import time
from datetime import datetime

import polars as pl
from loguru import logger

from config import FILE_INDEX, TICK_INDEX, TOTAL_INDEX, MINUTE1_INDEX, HISTORY_STOCK_1m  # noqa
from config import FILE_STOCK, TICK_STOCK, TOTAL_STOCK, MINUTE1_STOCK, HISTORY_STOCK_1d  # noqa
from factor_calc import main  # 因子计算模块
from qmt_quote.memory_map import get_mmap, SliceUpdater
from qmt_quote.utils import arr_to_pl, concat_interday, calc_factor, concat_intraday
from qmt_quote.utils_qmt import ticks_to_day, filter_suspend, adjust_ticks_time_astock, ticks_to_minute

stk1, stk2 = get_mmap(FILE_STOCK, TICK_STOCK, TOTAL_STOCK, readonly=True)
idx1, idx2 = get_mmap(FILE_INDEX, TICK_INDEX, TOTAL_INDEX, readonly=True)

# 约定df1存1分钟数据，df2存日线数据
slice_stk = SliceUpdater(min1=MINUTE1_STOCK, overlap_ratio=3, step_ratio=30)
# 加载历史数据

# 历史分钟线，只设置一次，当天不再更新
slice_stk.df1 = (
    pl.read_parquet(HISTORY_STOCK_1m)
    .filter(pl.col('suspendFlag') == 0)
    .with_columns(
        pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
        pl.col('volume').cast(pl.UInt64),
    )
)
# 历史日线，只设置一次，当天不再更新
slice_stk.df2 = (
    pl.read_parquet(HISTORY_STOCK_1d)
    .filter(pl.col('suspendFlag') == 0)
    .with_columns(
        pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
        pl.col('volume').cast(pl.UInt64),
    )
)


def process_day():
    df = arr_to_pl(stk1[slice_stk.day()])
    df = ticks_to_day(df)
    df = filter_suspend(df)
    slice_stk.df4 = concat_interday(slice_stk.df2, df)
    slice_stk.df4 = calc_factor(slice_stk.df4, by1='code', by2='time', close='close', pre_close='preClose')
    return slice_stk.df4


def process_min():
    df = arr_to_pl(stk1[slice_stk.minute()])
    df = adjust_ticks_time_astock(df, col=pl.col('time'))
    df = ticks_to_minute(df, period="1m")
    slice_stk.df3 = concat_intraday(slice_stk.df3, df, by1='code', by2='time', by3='duration')
    slice_stk.df5 = concat_interday(slice_stk.df1, slice_stk.df3)
    slice_stk.df5 = calc_factor(slice_stk.df5, by1='code', by2='time', close='close', pre_close='preClose')
    return slice_stk.df5


if __name__ == "__main__":
    last_time = -1
    while True:
        # TODO 屏蔽输入，可以用定时触发
        if False:
            x = input("输入`:q`退出；输入其它键打印最新数据\n")
            if x == ":q":
                break

        # TODO 替换成时间判断语句，就可以每分钟定时触发了
        if True:
            curr_time = datetime.now().minute
            if curr_time == last_time:
                time.sleep(1)
                continue
            last_time = curr_time
            logger.info(curr_time)

        # 更新当前位置
        start, end, current = slice_stk.update(int(stk2[0]))
        logger.info("{}, {}, {}", start, end, current)

        logger.info("最新5条原始数据==================")
        df = stk1[slice_stk.tail()]
        print(df)
        #
        logger.info("转日线数据==================只取最后一段合成日线")
        t1 = time.perf_counter()
        df = process_day()
        df = main(df)
        t2 = time.perf_counter()
        logger.info(f"日线耗时{t2 - t1:.2f}秒")
        print(df.tail(2))

        logger.info("转分钟数据==================数据量大分批转换")
        t1 = time.perf_counter()
        df = process_min()
        df = main(df)
        t2 = time.perf_counter()
        logger.info(f"分钟耗时{t2 - t1:.2f}秒")
        print(df.tail(2))
