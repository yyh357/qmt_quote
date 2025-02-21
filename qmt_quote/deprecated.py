"""
已弃用的函数

弃用原因：
1. 分钟数据以前是按需取tick数据，然后转换成分钟，导致要取时花费时间
2. 现在改成有专用模块一只计算转存分钟线

"""
import polars as pl

from examples.query import stk1, slice_1m
from qmt_quote.utils import arr_to_pl, concat_interday, calc_factor, concat_intraday


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
        .sort("stock_code", "volume")
        .with_columns(
            volume_diff=pl.col("volume").diff().fill_null(pl.col("volume")).over("stock_code", order_by="volume"),
            amount_diff=pl.col("amount").diff().fill_null(pl.col("amount")).over("stock_code", order_by="volume"),
        )
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every=period,
            closed="left",
            label="right",
            group_by=[
                "stock_code",
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
            preClose=pl.col("close").shift(1, fill_value=pl.col("preClose")).over('stock_code', order_by='time'),
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
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every="1d",
            closed="left",
            label="left",
            group_by=[
                "stock_code",
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


def process_day():
    df = arr_to_pl(stk1[slice_1m.for_day()])
    df = ticks_to_day(df)
    df = filter_suspend(df)
    slice_1m.df4 = concat_interday(slice_1m.df2, df)
    slice_1m.df4 = calc_factor(slice_1m.df4, by1='stock_code', by2='time', close='close', pre_close='preClose')
    return slice_1m.df4


def process_min():
    df = arr_to_pl(stk1[slice_1m.for_minute()])
    df = adjust_ticks_time_astock(df, col=pl.col('time'))
    df = ticks_to_minute(df, period="1m")
    slice_1m.df3 = concat_intraday(slice_1m.df3, df, by1='stock_code', by2='time', by3='duration')
    slice_1m.df5 = concat_interday(slice_1m.df1, slice_1m.df3)
    slice_1m.df5 = calc_factor(slice_1m.df5, by1='stock_code', by2='time', close='close', pre_close='preClose')
    return slice_1m.df5
