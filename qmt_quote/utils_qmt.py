"""
依赖于QMT的工具函数
"""
import time
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
import polars as pl
from tqdm import tqdm
from xtquant import xtdata

from qmt_quote.enums import InstrumentType
from qmt_quote.utils import cast_datetime, concat_dataframes_from_dict, ticks_to_dataframe, arr_to_pl, concat_interday, calc_factor1


def download_history_data2_wrap(desc: str, stock_list: List[str], period: str, start_time: str, end_time: str) -> None:
    """下载历史数据

    日线下得动，分钟线下不动？还是建议手动下载
    """
    pbar = tqdm(total=len(stock_list), desc=desc)
    xtdata.download_history_data2(stock_list, period=period, start_time=start_time, end_time=end_time, incrementally=True, callback=lambda x: pbar.update(1))
    while pbar.n < pbar.total:
        time.sleep(3)
    pbar.close()


def get_local_data_wrap(stock_list: List[str], period: str, start_time: str, end_time: str, data_dir: str) -> pl.DataFrame:
    """获取本地历史数据

    Notes
    -----
    反而通过QMT客户端手动下载数据，比通过API下载数据更靠谱

    """
    datas = xtdata.get_local_data([], stock_list, period, start_time, end_time, dividend_type='none', data_dir=data_dir)
    df = concat_dataframes_from_dict(datas)
    return cast_datetime(df, pl.col("time"))


def get_instrument_detail_wrap(stock_list: List[str]) -> pd.DataFrame:
    """批量获取股票详情，内有涨跌停字段 UpStopPrice和DownStopPrice
    """
    datas = {x: xtdata.get_instrument_detail(x) for x in stock_list}
    df = pd.DataFrame.from_dict(datas, orient='index')
    df.index.name = 'stock_code'
    # return pl.from_pandas(df, include_index=True)
    return df


def get_full_tick_1d(stock_list: List[str], level: int, rename: bool) -> pd.DataFrame:
    """获取tick数据，加了level后成日k线数据

    Parameters
    ----------
    stock_list
    level
        行情深度
    rename
        是否重命名列名

    """
    now = datetime.now().timestamp()
    now_ms = int(now * 1000)

    ticks = xtdata.get_full_tick(stock_list)
    ticks = ticks_to_dataframe(ticks, now=now_ms, index_name='stock_code', level=level)
    if rename:
        ticks = ticks.rename(columns={'lastPrice': 'close', 'lastClose': 'preClose'})
    return ticks


def load_history_data(path: str) -> pl.DataFrame:
    """加载历史数据，并做一定的调整

    Parameters
    ----------
    path

    """
    df = pl.read_parquet(path)
    df = df.filter(pl.col('suspendFlag') == 0).with_columns(
        pl.col('open', 'high', 'low', 'close', 'preClose').cast(pl.Float32),
        pl.col('volume').cast(pl.UInt64),
    )
    return df


def last_factor(arr: np.ndarray, his: pl.DataFrame = None, func=None, filter_label: float = 0) -> pl.DataFrame:
    """获取最终因子值

    Parameters
    ----------
    arr:
        当日分钟数据
    his
        历史数据
    filter_label:int
        取指定标签
    func
        因子计算函数

    """
    arr = arr[arr['type'] == InstrumentType.Stock]  # 过滤掉指数，只处理股票
    if filter_label > 0:
        arr = arr[arr['time'] <= filter_label]
    df = arr_to_pl(arr, col=pl.col('time', 'open_dt', 'close_dt'))
    df = concat_interday(his, df)
    df = calc_factor1(df)
    if func is not None:
        df = func(df)
    if filter_label > 0:
        df = df.filter(pl.col('time').dt.timestamp(time_unit='ms') == filter_label)
    return df
