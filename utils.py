import os
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
import polars as pl


def modify_file_size(file_path: str, new_size: int) -> None:
    """修改文件大小

    Parameters
    ----------
    file_path : str
        文件路径
    new_size : int
        新的文件大小

    Raises
    ------
    ValueError
        如果没有写权限

    """
    if not os.access(file_path, os.W_OK):
        raise ValueError("Permission denied. You don't have read permission.")

    if os.path.getsize(file_path) < new_size:
        with open(file_path, "wb") as f:
            f.seek(new_size, os.SEEK_SET)
            f.write(b'\0')
            f.flush()
            print(f"File {file_path} has been extended to {new_size} bytes.")


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
    file1 = filename + ".bin"
    file2 = filename + ".idx"
    count2 = 64
    if Path(file1).exists():
        print(f"File {file1} already exists.")
        if resize:
            modify_file_size(file1, count * dtype.itemsize)
    else:
        print(f"Creating new file {file1}.")
        np.memmap(file1, dtype=dtype, shape=(count,), mode="w+")
        np.memmap(file2, dtype=np.uint64, shape=(count2,), mode="w+")

    if readonly:
        arr1 = np.memmap(file1, dtype=dtype, shape=(count,), mode="r")
        arr2 = np.memmap(file2, dtype=np.uint64, shape=(count2,), mode="r")
    else:
        arr1 = np.memmap(file1, dtype=dtype, shape=(count,), mode="r+")
        arr2 = np.memmap(file2, dtype=np.uint64, shape=(count2,), mode="r+")

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
    return start, step, end


def arr_to_pd(arr: np.ndarray) -> pd.DataFrame:
    """numpy数组转pandas DataFrame"""
    df = pd.DataFrame(arr)
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
    return df


def arr_to_pl(arr: np.ndarray) -> pl.DataFrame:
    """numpy数组转polars DataFrame"""
    df = pl.from_numpy(arr)
    df = df.with_columns(
        pl.col("time").cast(pl.Datetime(time_unit="ms", time_zone="Asia/Shanghai"))
    )
    return df


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
        )
    )
    return df
