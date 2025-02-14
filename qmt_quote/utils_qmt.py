import time
from typing import Dict, Any

import numpy as np
import pandas as pd
import polars as pl
from tqdm import tqdm
from xtquant import xtdata

from qmt_quote.utils import cast_datetime, from_dict_dataframe


def ticks_to_dataframe(datas: Dict[str, Dict[str, Any]], level: int, now: pd.Timestamp) -> pd.DataFrame:
    """ 字典转DataFrame

    Parameters
    ----------
    datas : dict
        字典数据
    level : int
        行情深度
    now : pd.Timestamp
        当前时间
    Returns
    -------
    pd.DataFrame

    """
    df = pd.DataFrame.from_dict(datas, orient="index")
    df["now"] = now

    # 行情深度
    for i in range(level):
        j = i + 1
        df[[f"askPrice_{j}", f"bidPrice_{j}", f"askVol_{j}", f"bidVol_{j}"]] = df[
            ["askPrice", "bidPrice", "askVol", "bidVol"]
        ].map(lambda x: x[i])

    # 索引股票代码，之后要用
    df.index.name = "code"
    return df


def arr_to_pl(arr: np.ndarray) -> pl.DataFrame:
    """numpy数组转polars DataFrame"""
    return cast_datetime(pl.from_numpy(arr), pl.col("time"))


def adjust_ticks_time(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
    """调整时间边缘，方便生成与qmt历史数据一致整齐的1分钟K线

    标签打在右边界上

    9点25要调整到9点29
    11点30要调整到11点29
    """
    t = col.dt.time()
    df = df.filter(t >= pl.time(9, 25)).with_columns(
        # 9点25要调整到9点29
        time=pl.when(t < pl.time(9, 29)).then(col.dt.replace(minute=29)).otherwise(col)
    ).with_columns(
        # 11点30要调整到11点29
        time=pl.when((t >= pl.time(11, 30)) & (t < pl.time(11, 31))).then(col.dt.replace(minute=29, second=59, microsecond=100)).otherwise(col)
    )
    return df


def ticks_to_minute(df: pl.DataFrame, period: str = "1m") -> pl.DataFrame:
    """tick转分钟数据

    Parameters
    ----------
    df
    period:str
        周期，例如：1m, 5m, 15m, 30m, 60m

    Notes
    -----
    1. 只能日内tick数据转成分钟数据。多日tick数据需要先按日分组合
    2. 通过最新价格计算最高低价。凑不出日线最高低价

    """
    # 成交量和成交额转成K线数据前要做差分
    df = (
        df
        .sort("code", "volume")
        .with_columns(
            volume_diff=pl.col("volume").diff().fill_null(pl.col("volume")).over("code", order_by="volume"),
            amount_diff=pl.col("amount").diff().fill_null(pl.col("amount")).over("code", order_by="volume"),
        )
        .sort("code", "time")
        .group_by_dynamic(
            "time",
            every=period,
            closed="left",
            label="right",
            group_by=[
                "code",
            ],
        )
        .agg(
            open_dt=pl.first("time"),
            close_dt=pl.last("time"),
            open=pl.first("lastPrice"),
            high=pl.max("lastPrice"),
            low=pl.min("lastPrice"),
            close=pl.last("lastPrice"),
            volume=pl.sum("volume_diff").cast(pl.UInt64),
            amount=pl.sum("amount_diff"),
            preClose=pl.first("lastClose"),
        )
        .with_columns(
            preClose=pl.col("close").shift(1, fill_value=pl.col("preClose")).over('code', order_by='time'),
        )
        .with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )

    return df


def ticks_to_day(df: pl.DataFrame) -> pl.DataFrame:
    """ tick转日数据

    Notes
    -----
    只能日内tick数据转成日数据。多日tick数据需要先按日分组合

    """
    df = (
        df
        .sort("code", "time")
        .group_by_dynamic(
            "time",
            every="1d",
            closed="left",
            label="left",
            group_by=[
                "code",
            ],
        )
        .agg(
            open_dt=pl.first("time"),
            close_dt=pl.last("time"),
            open=pl.first("open"),
            high=pl.max("high"),
            low=pl.min("low"),
            close=pl.last("lastPrice"),
            volume=pl.last("volume"),
            amount=pl.last("amount"),
            preClose=pl.first("lastClose"),
        ).with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )
    return df


def filter_suspend(df: pl.DataFrame) -> pl.DataFrame:
    """过滤停牌数据"""
    # return df.filter(pl.col('suspendFlag') == 0)
    return df.filter(pl.col('volume') > 0, pl.col('high') > 0)


def concat_intraday(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """日内分钟合并，需要排除重复

    数据是分批到来的，所以合成K线也是分批的，但很有可能出现不完整的数据

    1. 前一DataFrame后期数据不完整
    2. 后一DataFrame前期数据不完整
    3. 前后DataFrame有重复数据

    """
    if df1 is None:
        return df2

    df = pl.concat([df1, df2], how='vertical')
    return df.sort("code", "time", "duration").unique(subset=["code", "time"], keep='last', maintain_order=True)


def concat_interday(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """日间线合并，不会重复，但格式会有偏差"""
    if df1 is None:
        return df2

    return pl.concat([df1, df2], how="align")


def calc_factor(df: pl.DataFrame) -> pl.DataFrame:
    """计算复权因子，使用交易所发布的昨收盘价计算
    """
    df = (
        df.filter(pl.col('suspendFlag') == 0)
        .sort("code", "time")
        .with_columns(factor1=(pl.col('close').shift(1) / pl.col('preClose')).fill_null(1).round(8).over('code', order_by='time'))
        .with_columns(factor2=(pl.col('factor1').cum_prod()).over('code', order_by='time'))
    )
    return df


def download_history_data2_task(desc, stock_list, period, start_time, end_time):
    """下载历史数据"""
    pbar = tqdm(total=len(stock_list), desc=desc)
    xtdata.download_history_data2(stock_list, period=period, start_time=start_time, end_time=end_time, incrementally=True, callback=lambda x: pbar.update(1))
    while pbar.n < pbar.total:
        time.sleep(3)
    pbar.close()


def get_market_data_ex_task(stock_list, period, start_time, end_time):
    datas = xtdata.get_market_data_ex([], stock_list, period=period, start_time=start_time, end_time=end_time)
    df = from_dict_dataframe(datas)
    return cast_datetime(df, pl.col("time"))
