"""
依赖于QMT的工具函数
"""
import time

import polars as pl
from tqdm import tqdm
from xtquant import xtdata

from qmt_quote.utils import cast_datetime, concat_dataframes_from_dict


def adjust_ticks_time_astock(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
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
