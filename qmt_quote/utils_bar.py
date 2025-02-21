"""


"""
# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import os
from typing import Tuple

import numpy as np
from numba import njit, uint64, float32, float64, uint32, typeof, boolean
from numba.experimental import jitclass
from numba.typed.typeddict import Dict

from qmt_quote.dtypes import DTYPE_STOCK_1m


@njit(cache=True)
def get_label_60(t: int, tz: int = 3600 * 8) -> int:
    """对时间标签化，返回时间段的起始点

    9点25之前的数据丢弃
    9点25到9点31之前的数据标签为9点30
    11点29到11点31之前的数据标签为11点29
    14点59到15点01之前的数据标签为14点59
    15点01之后的盘后交易丢弃

    """
    t = (t + tz) // 60 * 60  # 先转成秒再整理成分钟
    t0 = t // 86400 * 86400
    s = t - t0
    n = s
    while True:
        if s < 33900:  # 9:25
            return 0
        if s > 54000:  # 15:00
            return 0
        if s < 34200:  # 9:30
            n = 34200  # TODO 这里感觉QMT设计有问题
            break
        if s == 41400:  # 11:30
            n = 41400 - 60  # 11:29
            break
        if s == 54000:  # 15:00
            n = 54000 - 60  # 14:59
            break
        break
    return n + t0 - tz


@njit(cache=True)
def get_label_60_qmt(t: int, tz: int = 3600 * 8) -> int:
    """

    9点25之前的数据丢弃
    9点25数据标签为9点29
    11点30数据标签为11点29

    """
    out = get_label_60(t, tz)
    if out == 0:
        return 0
    else:
        return out + 60


@njit(cache=True)
def get_label_300(t: int, tz: int = 3600 * 8) -> int:
    out = get_label_60(t, tz)
    if out == 0:
        return 0
    else:
        return out // 300 * 300


@njit(cache=True)
def get_label_86400(t: int, tz: int = 3600 * 8) -> int:
    """对时间标签化，返回时间段的起始点

    9点25之前的数据丢弃
    9点25到9点31之前的数据标签为9点30
    11点29到11点31之前的数据标签为11点29
    14点59到15点01之前的数据标签为14点59
    15点01之后的盘后交易丢弃

    """
    t = (t + tz) // 60 * 60  # 先转成秒再整理成分钟
    t0 = t // 86400 * 86400
    s = t - t0
    while True:
        if s < 33900:  # 9:25
            return 0
        if s > 54000:  # 15:00
            return 0
        break
    return t0 - tz


class Bar:
    def __init__(self, pre_close: float, is_minute: bool = True):
        self.is_minute = is_minute
        self.close: float = pre_close
        self.pre_close: float = 0.
        self.pre_amount: float = 0.
        self.pre_volume: int = 0
        self.index: int = 0
        self.time: int = 0
        self.open_dt: int = 0
        self.close_dt: int = 0
        self.open: float = 0.
        self.high: float = 0.
        self.low: float = 0.
        self.last_amount: float = 0.
        self.last_volume: int = 0
        self.type: int = 0
        self.askPrice_1: float = 0.
        self.bidPrice_1: float = 0.
        self.askVol_1: int = 0
        self.bidVol_1: int = 0
        self.askVol_2: int = 0
        self.bidVol_2: int = 0

    def fill(self, arr: np.ndarray, stock_code: str) -> None:
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['open'] = self.open
        arr['high'] = self.high
        arr['low'] = self.low
        arr['close'] = self.close
        arr['preClose'] = self.pre_close
        arr['amount'] = self.last_amount - self.pre_amount
        arr['volume'] = self.last_volume - self.pre_volume
        arr['type'] = self.type
        arr['askPrice_1'] = self.askPrice_1
        arr['bidPrice_1'] = self.bidPrice_1
        arr['askVol_1'] = self.askVol_1
        arr['bidVol_1'] = self.bidVol_1
        arr['askVol_2'] = self.askVol_2
        arr['bidVol_2'] = self.bidVol_2

    def update(self, arr: np.ndarray, time: int) -> bool:
        self.askPrice_1 = arr['askPrice_1']
        self.bidPrice_1 = arr['bidPrice_1']
        self.askVol_1 = arr['askVol_1']
        self.bidVol_1 = arr['bidVol_1']
        self.askVol_2 = arr['askVol_2']
        self.bidVol_2 = arr['bidVol_2']
        self.close_dt = arr['time']
        if self.time != time:
            self.time = time
            self.open_dt = arr['time']
            self.type = arr['type']
            if self.is_minute:
                self.pre_close = self.close
                self.pre_amount = self.last_amount
                self.pre_volume = self.last_volume
                self.open = arr['lastPrice']
                self.high = arr['lastPrice']
                self.low = arr['lastPrice']
            else:
                self.pre_close = arr['lastClose']
                self.open = arr['open']
                self.high = arr['high']
                self.low = arr['low']

            self.close = arr['lastPrice']
            self.last_amount = arr['amount']
            self.last_volume = arr['volume']
            return True
        else:
            if self.is_minute:
                self.high = max(arr['lastPrice'], self.high)
                self.low = min(arr['lastPrice'], self.low)
            else:
                self.high = arr['high']
                self.low = arr['low']

            self.close = arr['lastPrice']
            self.last_amount = arr['amount']
            self.last_volume = arr['volume']
            return False


class BarManager:

    def __init__(self, arr1: np.ndarray, arr2: np.ndarray, is_minute: bool = True):
        tmp = Dict()
        tmp['600000.SH'] = Bar(0.0, True)
        tmp.clear()
        self.bars = tmp

        self.index: int = 0
        self.arr1 = arr1
        self.arr2 = arr2
        self.is_minute = is_minute

    def extend(self, arr: np.ndarray, get_label, get_label_arg1: float) -> Tuple[int, int, int]:
        last_index = self.index
        for a in arr:
            # TODO 时间戳请选用特别的格式
            time = get_label(a['time'] // 1000, get_label_arg1) * 1000
            if time == 0:
                continue
            stock_code = str(a['stock_code'])
            # if stock_code != '301068.SZ':  # TODO test
            #     continue
            not_in = stock_code not in self.bars
            if not_in:
                self.bars[stock_code] = Bar(a['lastClose'], self.is_minute)

            b = self.bars[stock_code]
            if b.update(a, time):
                b.index = self.index
                self.index += 1
            b.fill(self.arr1[b.index], stock_code)
        # 记录位子
        self.arr2[0] = self.index
        return last_index, self.index, self.index - last_index


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    spec = [
        ('is_minute', boolean),
        ('index', uint64),
        ('time', uint64),
        ('open_dt', uint64),
        ('close_dt', uint64),
        ('open', float32),
        ('high', float32),
        ('low', float32),
        ('close', float32),
        ('pre_close', float32),
        ('pre_amount', float64),
        ('pre_volume', uint64),
        ('last_amount', float64),
        ('last_volume', uint64),
        ('type', boolean),
        ('askPrice_1', float32),
        ('bidPrice_1', float32),
        ('askVol_1', uint32),
        ('bidVol_1', uint32),
        ('askVol_2', uint32),
        ('bidVol_2', uint32),
    ]
    Bar = jitclass(spec)(Bar)

if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp = Dict()
    tmp['600000.SH'] = Bar(0.0, True)
    tmp.clear()

    idx_type = typeof(np.empty(64, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_STOCK_1m))
    spec = [
        ('bars', typeof(tmp)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
        ('is_minute', boolean),
    ]
    BarManager = jitclass(spec)(BarManager)
