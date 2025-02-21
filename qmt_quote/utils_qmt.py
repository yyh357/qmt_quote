"""
依赖于QMT的工具函数
"""
import time

import pandas as pd
import polars as pl
from tqdm import tqdm
from xtquant import xtdata

from qmt_quote.utils import cast_datetime, concat_dataframes_from_dict, ticks_to_dataframe


def download_history_data2_wrap(desc, stock_list, period, start_time, end_time):
    """下载历史数据

    日线下得动，分钟线下不动？还是建议手动下载
    """
    pbar = tqdm(total=len(stock_list), desc=desc)
    xtdata.download_history_data2(stock_list, period=period, start_time=start_time, end_time=end_time, incrementally=True, callback=lambda x: pbar.update(1))
    while pbar.n < pbar.total:
        time.sleep(3)
    pbar.close()


def get_local_data_wrap(stock_list, period: str, start_time: str, end_time: str, data_dir: str) -> pl.DataFrame:
    """获取本地历史数据

    Notes
    -----
    反而通过QMT客户端手动下载数据，比通过API下载数据更靠谱

    """
    datas = xtdata.get_local_data([], stock_list, period, start_time, end_time, dividend_type='none', data_dir=data_dir)
    df = concat_dataframes_from_dict(datas)
    return cast_datetime(df, pl.col("time"))


def get_instrument_detail_wrap(stock_list) -> pl.DataFrame:
    """批量获取股票详情，内有涨跌停字段 UpStopPrice和DownStopPrice
    """
    datas = {x: xtdata.get_instrument_detail(x) for x in stock_list}
    df = pd.DataFrame.from_dict(datas, orient='index')
    df.index.name = 'stock_code'
    return pl.from_pandas(df, include_index=True)


def get_full_kline_1d(stock_list, level: int, rename: bool) -> pd.DataFrame:
    """获取日k线数据"""
    ticks = xtdata.get_full_tick(stock_list)
    ticks = ticks_to_dataframe(ticks, now=pd.Timestamp.now(), index_name='stock_code', level=level)
    if rename:
        ticks = ticks.rename(columns={'lastPrice': 'close', 'lastClose': 'preClose'})
    return ticks
