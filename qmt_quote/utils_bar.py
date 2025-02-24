import polars as pl


def ticks_to_minute(df: pl.DataFrame, every: str = "1m", closed: str = "left", label: str = "right") -> pl.DataFrame:
    """tick转分钟数据

    Parameters
    ----------
    df
    every:str
        周期，例如：1m, 5m, 15m, 30m, 60m
    closed: str
        周期的闭合方式，例如：left, right
    label: str
        标签的位置，例如：left, right

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
            every=every,
            closed=closed,
            label=label,
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


def minute_1m_to_5m(df: pl.DataFrame, every: str = "5m", closed: str = "right", label: str = "right") -> pl.DataFrame:
    """1分钟线转5分钟线

    Parameters
    ----------
    df
    every:str
        周期
    closed:str
        周期闭合
    label:str
        周期标签

    """
    df = (
        df
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every=every,
            closed=closed,
            label=label,
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
            close=pl.last("close"),
            volume=pl.sum("volume"),
            amount=pl.sum("amount"),
            preClose=pl.first("preClose"),
            # 历史1分钟有这个字典，5分钟要补上
            suspendFlag=pl.last("suspendFlag"),
        )
    )
    return df
