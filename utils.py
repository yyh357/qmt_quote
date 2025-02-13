import os
from pathlib import Path
from typing import Tuple, Optional

import numpy as np
import pandas as pd
import polars as pl

_EXT1_ = ".bin"
_EXT2_ = ".idx"
_COUNT_ = 64


def extend_file_size(file_path: str, new_size: int) -> None:
    """扩展文件大小

    Parameters
    ----------
    file_path : str
        文件路径
    new_size : int
        新的文件大小

    """
    old_size = os.path.getsize(file_path)
    if old_size >= new_size:
        return

    with open(file_path, "r+") as f:
        f.truncate(new_size)
        f.flush()
        print(f"File {file_path} has been extended from {old_size} to {new_size} bytes.")


def truncate_file_size(file_path: str, new_size: int) -> None:
    """截断文件大小

    Parameters
    ----------
    file_path : str
        文件路径
    new_size : int
        新的文件大小

    """
    old_size = os.path.getsize(file_path)
    if old_size <= new_size:
        return
    if new_size == 0:
        return

    with open(file_path, "r+") as f:
        f.truncate(new_size)
        f.flush()
        print(f"File {file_path} has been truncated from {old_size} to {new_size} bytes.")


def mmap_truncate(filename: str):
    """截断内存映射文件

    Parameters
    ----------
    filename

    """
    file1 = filename + _EXT1_
    file2 = filename + _EXT2_

    arr2 = np.memmap(file2, dtype=np.uint64, shape=(_COUNT_,), mode="r")
    truncate_file_size(file1, int(arr2[0] * arr2[1]))


def get_mmap(filename: str, dtype: np.dtype, count: int, readonly: bool = True, resize: bool = False) -> Tuple[np.ndarray, np.ndarray]:
    """创建获取内存映射文件

    Parameters
    ----------
    filename : str
        文件路径
    dtype : np.dtype
        数据类型
    count : int
        数据行数
    readonly : bool
        是否只读
    resize : bool, optional
        是否调整文件大小

    Returns
    -------
    Tuple[np.ndarray, np.ndarray]
        内存映射文件和索引文件

    """
    file1 = filename + _EXT1_
    file2 = filename + _EXT2_

    if Path(file1).exists():
        print(f"File {file1} already exists.")
        if resize:
            extend_file_size(file1, count * dtype.itemsize)
        else:
            # !!! 一定要调整，否则会扩展文件大小，所以这里重新调整
            count = os.path.getsize(file1) // dtype.itemsize
    else:
        print(f"Creating new file {file1}.")
        np.memmap(file1, dtype=dtype, shape=(count,), mode="w+")
        np.memmap(file2, dtype=np.uint64, shape=(_COUNT_,), mode="w+")

    if readonly:
        arr1 = np.memmap(file1, dtype=dtype, shape=(count,), mode="r")
        arr2 = np.memmap(file2, dtype=np.uint64, shape=(_COUNT_,), mode="r")
    else:
        arr1 = np.memmap(file1, dtype=dtype, shape=(count,), mode="r+")
        arr2 = np.memmap(file2, dtype=np.uint64, shape=(_COUNT_,), mode="r+")
        # 1号位置放itemsize，后面可能用到
        arr2[1] = dtype.itemsize

    return arr1, arr2


def dict_to_dataframe(datas: dict, level: int, now: pd.Timestamp) -> pd.DataFrame:
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


def update_array(arr1: np.ndarray, arr2: np.ndarray, df: pd.DataFrame) -> Tuple[int, int, int]:
    """将DataFrame数据更新到内存映射文件中

    Parameters
    ----------
    arr1 : np.ndarray
        内存映射文件
    arr2 : np.ndarray
        索引文件
    df : pd.DataFrame
        DataFrame数据

    Returns
    -------
    Tuple[int, int, int]
        最后一行，数据行数，新的行数

    """
    arr = df.to_records(index=True)

    start = arr2[0]
    step = len(arr)
    end = start + step
    arr1[start:end] = arr
    arr2[0] = end
    return int(start), step, int(end)


def arr_to_pd(arr: np.ndarray) -> pd.DataFrame:
    """numpy数组转pandas DataFrame"""
    df = pd.DataFrame(arr)
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
    # df["_time_"] = df["time"].dt.time
    return df


def arr_to_pl(arr: np.ndarray) -> pl.DataFrame:
    """numpy数组转polars DataFrame"""
    df = pl.from_numpy(arr)
    df = df.with_columns(
        pl.col("time").cast(pl.Datetime(time_unit="ms", time_zone="Asia/Shanghai"))
    )
    # df = df.with_columns(_time_=pl.col("time").dt.time())
    return df


def filter__0930_1130__1300_1500(df: pl.DataFrame, col: str = '_time_') -> pl.DataFrame:
    """过滤9:30-11:30和13:00-15:00的数据

    Parameters
    ----------
    df : pl.DataFrame
        polars DataFrame
    col : str
        时间列名，默认'_time_'

    Notes
    -----
    丢弃了边缘超出范围的数据。假设少量几秒数据对K线影响不大

    """
    t = pl.col(col)
    t1 = (t >= pl.time(9, 30)) & (t < pl.time(11, 30))
    t2 = (t >= pl.time(13, 00)) & (t < pl.time(15, 00))
    return df.filter(t1 | t2)


def tick_to_minute(df: pl.DataFrame, period: str = "1m") -> pl.DataFrame:
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
    df = df.sort("code", "time").with_columns(
        volume_diff=pl.col("pvolume")
        .diff()
        .fill_null(pl.col("pvolume"))
        .over("code", order_by="time"),
        amount_diff=pl.col("amount")
        .diff()
        .fill_null(pl.col("amount"))
        .over("code", order_by="time"),
    )
    df = (
        df.sort("code", "time")
        .group_by_dynamic(
            "time",
            every=period,
            closed="left",
            label="left",
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
            volume=pl.sum("volume_diff"),
            amount=pl.sum("amount_diff"),
            pre_close=pl.first("lastClose"),
        )
        .with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )
    return df


def tick_to_day(df: pl.DataFrame) -> pl.DataFrame:
    """ tick转日数据

    Notes
    -----
    只能日内tick数据转成日数据。多日tick数据需要先按日分组合

    """
    df = (
        df.sort("code", "time")
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
            volume=pl.last("pvolume"),
            amount=pl.last("amount"),
            pre_close=pl.first("lastClose"),
        ).with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )
    return df


def concat_unique(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """合并去重DataFrame

    数据是分批到来的，所以合成K线也是分批的，但很有可能出现不完整的数据

    1. 前一DataFrame后期数据不完整
    2. 后一DataFrame前期数据不完整
    3. 前后DataFrame有重复数据

    """
    if df1 is None:
        return df2
    else:
        df = pl.concat([df1, df2])
    # 另一种方法
    # df.sort("code", "time", "duration").group_by("code", "time", maintain_order=True).last()
    return df.sort("code", "time", "duration").unique(subset=["code", "time"], keep='last', maintain_order=True)


class SliceUpdater:
    """切片增量更新

    由于全量数据计算量大，计算一次约12秒，因此采用增量更新的方式，每次只计算一定范围的数据。
    每次更新的范围为：[start, end)，每次更新的步长为step，每次更新的重叠范围为overlap。

    Attributes
    ----------
    df1 : pl.DataFrame
        合并去重后的DataFrame
    start : int
        起始位置
    end : int
        结束位置
    """

    def __init__(self, overlap: int = 500_000, step: Optional[int] = None):
        """初始化增量更新器

        Parameters
        ----------
        overlap : int
            重叠范围。建议设置为3分钟的tick数据量
        step : int, optional
            步长，默认None，建议设置为30分钟的tick数据量

        """
        # 合并K线时存储数据使用，所以预留了几个位置
        # 比如1分钟、日线，或者5分钟等等
        self.df1 = None
        self.df2 = None
        self.df3 = None
        self.df4 = None
        self.df5 = None

        self.start = 0
        self.end = 0
        self.current = 0
        self.overlap = overlap
        if step is None:
            self.step = self.overlap * 10
        else:
            self.step = step
        assert self.step >= self.overlap * 2, "step must be greater than overlap*2"

    def update(self, current: int):
        self.current = int(current)
        self.start = max(self.end - self.overlap, 0)
        self.end = min(self.start + self.step, self.current)
        return self.start, self.end, self.current

    def head(self, n: int = 5) -> slice:
        """前n条"""
        return slice(0, min(n, self.current))

    def tail(self, n: int = 5) -> slice:
        """最后n条"""
        return slice(max(self.current - n, 0), self.current)

    def minute(self) -> slice:
        """tick转分钟时需要全部数据，所以增量切片"""
        return slice(self.start, self.end)

    def day(self) -> slice:
        """tick转日线时只要最后一段的数据。因为数据中已经包含了OHLCV"""
        return self.tail(self.overlap)
