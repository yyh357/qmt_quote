from typing import Tuple

from numba import njit


@njit(cache=True)
def _get_label_stock(t: int, tz: int, bar_size: int) -> Tuple[int, int, int]:
    """对时间标签化，返回时间段的起始点

    非交易时间会收到0点的数据，需要过滤掉
    盘后交易也有数据，这里也丢弃

    9点25之前的数据丢弃
    9点25到9点31之前的数据标签为9点30
    11点29到11点31之前的数据标签为11点29
    14点59到15点01之前的数据标签为14点59
    15点01之后的盘后交易丢弃

    """
    t = (t + tz) // bar_size * bar_size  # 先转成秒再整理成分钟
    t0 = t // 86400 * 86400
    s = t - t0
    n = s
    while True:
        if s < 33900:  # 9:25
            return 0, 0, 0
        if s > 54000:  # 15:00
            return 0, 0, 0
        if s < 34200:  # 9:30
            n = 34200  # TODO 这里感觉QMT设计有问题
            break
        if s == 41400:  # 11:30
            n = 41400 - bar_size  # 11:29
            break
        if s == 54000:  # 15:00
            n = 54000 - bar_size  # 14:59
            break
        break
    return n, t0, tz


@njit(cache=True)
def get_label_stock_1m(t: int, tz: int = 3600 * 8) -> int:
    """1分钟标签"""
    n, t0, tz = _get_label_stock(t, tz, 60)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_5m(t: int, tz: int = 3600 * 8) -> int:
    """5分钟标签"""
    n, t0, tz = _get_label_stock(t, tz, 300)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_15m(t: int, tz: int = 3600 * 8) -> int:
    """15分钟标签"""
    n, t0, tz = _get_label_stock(t, tz, 900)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_30m(t: int, tz: int = 3600 * 8) -> int:
    """30分钟标签"""
    n, t0, tz = _get_label_stock(t, tz, 1800)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_60m(t: int, tz: int = 3600 * 8) -> int:
    """60分钟标签

    9点30到10点29之间的标签为9点30
    10点30到11点30之间的标签为10点30
    13点00到13点59之间的标签为13点00
    14点00到15点00之间的标签为14点00
    """
    n, t0, tz = _get_label_stock(t, tz, 60)
    if n == 0:
        return 0
    if n < 43200:
        out = (n - 1800) // 3600 * 3600 + 1800 + t0 - tz
    else:
        out = n // 3600 * 3600 + t0 - tz
    return out


@njit(cache=True)
def get_label_stock_120m(t: int, tz: int = 3600 * 8) -> int:
    """120分钟标签
    9点30到11点29之间的标签为9点30
    13点00到15点00之间的标签为13点00

    """
    n, t0, tz = _get_label_stock(t, tz, 60)
    if n == 0:
        return 0
    if n < 43200:
        out = 34200 + t0 - tz
    else:
        out = 46800 + t0 - tz
    return out


@njit(cache=True)
def get_label_stock_1d(t: int, tz: int = 3600 * 8) -> int:
    """1日线标签

    由于想用日线实现tick数据快照的功能，所以不丢弃数据，直接返回当天的0点
    """
    t = (t + tz) // 86400 * 86400
    return t - tz

# @njit(cache=True)
# def get_label_stock_1d(t: int, tz: int = 3600 * 8) -> int:
#     n, t0, tz = _get_label_stock(t, tz, 60)
#     if n == 0:
#         return 0
#     return t0 - tz
