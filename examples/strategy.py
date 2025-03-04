import time
from datetime import datetime

import pandas as pd
import polars as pl

from examples.config import FILE_d1m, FILE_d1d, TOTAL_1m, TOTAL_1d, TICKS_PER_MINUTE, FILE_d5m, TOTAL_5m, HISTORY_STOCK_1d, HISTORY_STOCK_1m, HISTORY_STOCK_5m, FILE_s1t, FILE_s1d
from factor_calc import main
from qmt_quote.bars.labels import get_label_stock_1m
from qmt_quote.bars.signals import BarManager as BarManagerS
from qmt_quote.dtypes import DTYPE_STOCK_1m, DTYPE_SIGNAL_1t, DTYPE_SIGNAL_1m
from qmt_quote.memory_map import get_mmap, SliceUpdater, update_array2
from qmt_quote.utils_qmt import load_history_data, last_factor

# 策略数量
STRATEGY_COUNT = 3
# K线
d1d1, d1d2 = get_mmap(FILE_d1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=True)
d1m1, d1m2 = get_mmap(FILE_d1m, DTYPE_STOCK_1m, TOTAL_1m, readonly=True)
d5m1, d5m2 = get_mmap(FILE_d5m, DTYPE_STOCK_1m, TOTAL_5m, readonly=True)
# 信号
s1t1, s1t2 = get_mmap(FILE_s1t, DTYPE_SIGNAL_1t, TOTAL_1m * STRATEGY_COUNT, readonly=False)
s1d1, s1d2 = get_mmap(FILE_s1d, DTYPE_SIGNAL_1m, TOTAL_1d * STRATEGY_COUNT, readonly=False)

# 约定df1存1分钟数据，df2存日线数据
slice_d1d = SliceUpdater(min1=TOTAL_1d, overlap_ratio=3, step_ratio=30)
slice_d1m = SliceUpdater(min1=TICKS_PER_MINUTE, overlap_ratio=3, step_ratio=30)
slice_d5m = SliceUpdater(min1=TICKS_PER_MINUTE * 5, overlap_ratio=3, step_ratio=30)
# 策略数量
slice_s1m = SliceUpdater(min1=TOTAL_1d * STRATEGY_COUNT, overlap_ratio=3, step_ratio=30)

# 加载历史数据
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# 取历史
his_stk_1d = load_history_data(HISTORY_STOCK_1d)
his_stk_1m = load_history_data(HISTORY_STOCK_1m)
his_stk_5m = load_history_data(HISTORY_STOCK_5m)
# 仅当日
his_stk_1d = None
his_stk_1m = None
his_stk_5m = None

columns = list(DTYPE_SIGNAL_1t.names)

if __name__ == "__main__":
    bm_s1d = BarManagerS(s1d1, s1d2)

    last_time = -1
    while True:
        # 调整成成分钟标签，当前分钟还在更新
        curr_time = datetime(2025, 2, 28, 15, 0).timestamp() // 60 * 60
        # curr_time = datetime.now().timestamp() // 60 * 60
        if curr_time == last_time:
            time.sleep(0.5)
            continue
        last_time = curr_time
        # 过滤时间。调整成成分钟标签，是取当前更新中的K线，还是去上一根不变的K线
        label_1m = (curr_time // 60 * 60 - 60) * 1000
        label_5m = (curr_time // 300 * 300 - 300) * 1000
        # 日线, 东八区处理
        label_1d = ((curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8) * 1000

        # 更新当前位置
        slice_d1d.update(int(d1d2[0]))  # 日线
        slice_d1m.update(int(d1m2[0]))  # 1分钟
        slice_d5m.update(int(d5m2[0]))  # 5分钟

        # 取今天全部数据和历史数据计算因子
        df = last_factor(d1m1[slice_d1m.for_all()], his_stk_1m, label_1m, main)

        #
        df = df.select("stock_code", pl.col("time").cast(pl.UInt64), pl.lit(1).alias('strategy_id'),
                       pl.col('A').alias('float32'),
                       pl.col('B').alias('int32'),
                       pl.col('OUT').alias('boolean')).to_pandas()

        start, end, step = update_array2(s1t1, s1t2, df[columns], index=False)
        if step == 0:
            continue

        start, end, step = bm_s1d.extend(s1t1[start:end], get_label_stock_1m, 3600 * 8)

        print(s1d1[start:end])
