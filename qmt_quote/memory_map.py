"""
内存映射文件
1. 注意保持一写多读，防止数据混乱

设计思路
1. 两个文件。一个是数据文件bin，一个是索引文件idx
2. 两个文件都使用`np.memmap`进行管理
3. bin文件按行存储，每行数据的长度是固定的
4. idx文件存储索引，第一行存储数据的行数，第二行存储每行数据的长度
5. 每次更新数据时，先更新数据文件，然后更新索引文件
6. 每次读取数据时，先读取索引文件，然后读取数据文件

"""
import os
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

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


def mmap_truncate(filename: str, reserve: int = 10000):
    """截断内存映射文件

    Parameters
    ----------
    filename
    reserve: int
        预留行数

    """
    assert reserve >= 0
    file1 = filename + _EXT1_
    file2 = filename + _EXT2_

    arr2 = np.memmap(file2, dtype=np.uint64, shape=(_COUNT_,), mode="r")
    truncate_file_size(file1, int((arr2[0] + reserve) * arr2[1]))


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
    # arr2.flush()
    return int(start), int(end), step


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

    def __init__(self, min1: int, overlap_ratio: float = 3, step_ratio: float = 30):
        """初始化增量更新器

        Parameters
        ----------
        min1: int
            1分钟数据量
        overlap_ratio : int
            重叠范围。 默认3分钟
        step_ratio : int, optional
            步长。默认30分钟

        """
        # 无重叠取数据
        self.start = 0
        self.end = 0

        self.cursor = 0
        self.min1 = min1
        self.overlap = int(min1 * overlap_ratio)
        self.step = int(min1 * step_ratio)
        assert overlap_ratio >= 2.5, "overlap_ratio must be greater than 2.5"
        assert step_ratio >= overlap_ratio * 2, "step_ratio must be greater than overlap_ratio*2"

    def update(self, cursor: int) -> Tuple[int, int, int]:
        """更新最新位置，和切片开始结束位置"""
        self.cursor = int(cursor)
        self.start = self.end
        self.end = min(self.start + self.step, self.cursor)
        return self.start, self.end, self.cursor

    def head(self, n: int = 5) -> slice:
        """前n条"""
        return slice(0, min(n, self.cursor))

    def tail(self, n: int = 5) -> slice:
        """最后n条"""
        return slice(max(self.cursor - n, 0), self.cursor)

    def for_minute(self) -> slice:
        """tick转分钟时需要全部数据，所以增量切片"""
        return slice(max(self.start - self.overlap, 0), self.end)

    def for_all(self) -> slice:
        """tick"""
        return slice(0, self.cursor)

    def for_next(self) -> slice:
        """tick转分钟时需要全部数据，所以增量切片"""
        return slice(self.start, self.end)

    def for_day(self) -> slice:
        """tick转日线时只要最后一段的数据。因为数据中已经包含了OHLCV"""
        return self.tail(self.min1)
