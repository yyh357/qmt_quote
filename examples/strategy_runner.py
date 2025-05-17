import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from npyt import NPYT
from npyt.format import dtype_to_column_dtypes

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import FILE_d1m, FILE_d1d, TOTAL_1m, TOTAL_1d, FILE_d5m, HISTORY_STOCK_1d, \
    HISTORY_STOCK_1m, HISTORY_STOCK_5m, FILE_s1t, FILE_s1d, TICKS_PER_MINUTE
from examples.factor_calc import main as factor_func
from qmt_quote.bars.labels import get_label_stock_1d
from qmt_quote.bars.signals import BarManager as BarManagerS
from qmt_quote.dtypes import DTYPE_SIGNAL_1t, DTYPE_SIGNAL_1m
from qmt_quote.utils_qmt import load_history_data, last_factor

columns = list(DTYPE_SIGNAL_1t.names)

# TODO 策略数量
STRATEGY_COUNT = 3
# K线
d1d = NPYT(FILE_d1d).load(mmap_mode="r")
d1m = NPYT(FILE_d1m).load(mmap_mode="r")
d5m = NPYT(FILE_d5m).load(mmap_mode="r")

# 信号
s1t = NPYT(FILE_s1t).save(dtype=DTYPE_SIGNAL_1t, capacity=TOTAL_1m * STRATEGY_COUNT).load(mmap_mode="r+")
s1d = NPYT(FILE_s1d).save(dtype=DTYPE_SIGNAL_1m, capacity=TOTAL_1d * STRATEGY_COUNT).load(mmap_mode="r+")

column_dtypes = dtype_to_column_dtypes(DTYPE_SIGNAL_1t)

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# 取历史，不建议太长，只要保证最新数据生成正常即可
his_stk_1d = load_history_data(HISTORY_STOCK_1d)
his_stk_1m = load_history_data(HISTORY_STOCK_1m)
his_stk_5m = load_history_data(HISTORY_STOCK_5m)

# # 仅当日
# his_stk_1d = None
# his_stk_1m = None
# his_stk_5m = None


def to_pandas(df: pl.DataFrame, strategy_id: int = 0) -> pd.DataFrame:
    df = df.select("stock_code", pl.col("time").cast(pl.UInt64),
                   strategy_id=strategy_id,
                   float32=pl.col('A').cast(pl.Float32),
                   int32=pl.col('B').fill_null(0).cast(pl.Int32),
                   boolean=pl.col('OUT').cast(pl.Boolean),
                   ).to_pandas()
    return df


def main(curr_time: int) -> None:
    # 过滤时间。调整成成分钟标签，是取当前更新中的K线，还是去上一根不变的K线
    label_1m = (curr_time // 60 * 60 - 60) * 1000
    label_5m = (curr_time // 300 * 300 - 300) * 1000
    # 日线, 东八区处理
    label_1d = ((curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8) * 1000

    # 取今天全部数据和历史数据计算因子，但只取最新的值
    df = last_factor(d1m.data(), his_stk_1m, factor_func, label_1m)
    if df.is_empty():
        return

    # 只对最新值转换格式
    df = to_pandas(df, strategy_id=1)
    # 这里表面上是参照tick数据顺序更新，但上层是按
    records = df[columns].to_records(index=False, column_dtypes=column_dtypes)
    remaining = s1t.append(records)
    # 更新方式，全量更新
    start, end, step = bm_s1d.extend(s1t.read(n=TICKS_PER_MINUTE * 6), get_label_stock_1d, 3600 * 8)
    # 只显示最新的3条
    print(end, datetime.fromtimestamp(curr_time), datetime.now())
    print(s1d.tail(3))


if __name__ == "__main__":
    bm_s1d = BarManagerS(s1d._a, s1d._t)

    # 实盘运行
    last_time = -1
    while True:
        # 调整成成分钟标签，用户可以考虑设置成10秒等更快频率。注意!!!内存映射文件要扩大几倍
        curr_time = datetime.now().timestamp() // 60 * 60
        if curr_time == last_time:
            time.sleep(0.5)
            continue
        last_time = curr_time
        main(curr_time)

    # # TODO 测试用，记得修改日期
    # for curr_time in range(int(datetime(2025, 3, 5, 9, 29).timestamp() // 60 * 60),
    #                        int(datetime(2025, 3, 5, 15, 1).timestamp() // 60 * 60),
    #                        60):
    #     # 调整成成分钟标签，当前分钟还在更新
    #     # curr_time = datetime(2025, 2, 28, 15, 0).timestamp() // 60 * 60
    #     last_time = curr_time
    #     main(curr_time)
