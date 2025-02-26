"""
Tick转Bar
小K线转大K线
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
    n, t0, tz = _get_label_stock(t, tz, 60)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_5m(t: int, tz: int = 3600 * 8) -> int:
    n, t0, tz = _get_label_stock(t, tz, 300)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_15m(t: int, tz: int = 3600 * 8) -> int:
    n, t0, tz = _get_label_stock(t, tz, 900)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_30m(t: int, tz: int = 3600 * 8) -> int:
    n, t0, tz = _get_label_stock(t, tz, 1800)
    return n + t0 - tz


@njit(cache=True)
def get_label_stock_60m(t: int, tz: int = 3600 * 8) -> int:
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
    n, t0, tz = _get_label_stock(t, tz, 60)
    if n == 0:
        return 0
    return t0 - tz


class Bar:
    def __init__(self, pre_close: float, is_minute: bool, include_quote: bool):
        self.is_minute = is_minute
        self.include_quote = include_quote
        self.close: float = pre_close
        self.pre_close: float = 0.
        self.pre_amount: float = 0.
        self.pre_volume: int = 0
        self.last_amount: float = 0.
        self.last_volume: int = 0
        self.time: int = 0  # 当前bar时间戳
        self.last_time: int = 0  # 数据源bar的时间戳
        self.index: int = 0
        self.open_dt: int = 0
        self.close_dt: int = 0
        self.open: float = 0.
        self.high: float = 0.
        self.low: float = 0.
        self.type: int = 0
        self.askPrice_1: float = 0.
        self.bidPrice_1: float = 0.
        self.askVol_1: int = 0
        self.bidVol_1: int = 0
        self.askVol_2: int = 0
        self.bidVol_2: int = 0

        tmp1 = Dict()
        tmp1[uint64(1)] = uint64(1)
        tmp1.clear()

        tmp2 = Dict()
        tmp2[uint64(1)] = 0.0
        tmp2.clear()

        self.volumes = tmp1
        self.amounts = tmp2

    def fill_tick(self, arr: np.ndarray, stock_code: str) -> None:
        """
        pre_amount 上一k线最后的累计金额
        pre_volume 上一k线最后的累计成交量
        last_amount 当前k线的累计金额
        last_volume 当前k线的累计成交量
        amount = last_amount - pre_amount 当前K线内产生的金额
        volume = last_volume - pre_volume 当前K线内产生的成交量

        """
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

        if self.include_quote:
            arr['askPrice_1'] = self.askPrice_1
            arr['bidPrice_1'] = self.bidPrice_1
            arr['askVol_1'] = self.askVol_1
            arr['bidVol_1'] = self.bidVol_1
            arr['askVol_2'] = self.askVol_2
            arr['bidVol_2'] = self.bidVol_2

    def fill_bar_v1(self, arr: np.ndarray, stock_code: str) -> None:
        """
        pre_amount 本根大k线内，前几根小k线的累计金额
        pre_volume 本根大k线内，前几根小k线的累计成交量
        last_amount 当前小k线的金额，会一直变化
        last_volume 当前小k线的成交量，会一直变化
        amount = pre_amount + last_amount 本根大k线内产生的金额
        volume = pre_volume + last_volume 本根大k线内产生的成交量

        """
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['open'] = self.open
        arr['high'] = self.high
        arr['low'] = self.low
        arr['close'] = self.close
        arr['preClose'] = self.pre_close
        arr['amount'] = self.pre_amount + self.last_amount
        arr['volume'] = self.pre_volume + self.last_volume
        arr['type'] = self.type

        if self.include_quote:
            arr['askPrice_1'] = self.askPrice_1
            arr['bidPrice_1'] = self.bidPrice_1
            arr['askVol_1'] = self.askVol_1
            arr['bidVol_1'] = self.bidVol_1
            arr['askVol_2'] = self.askVol_2
            arr['bidVol_2'] = self.bidVol_2

    def fill_bar_v2(self, arr: np.ndarray, stock_code: str) -> None:
        """
        使用字典存储小k线的累计金额和成交量，然后求和
        """
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['open'] = self.open
        arr['high'] = self.high
        arr['low'] = self.low
        arr['close'] = self.close
        arr['preClose'] = self.pre_close
        arr['amount'] = sum([v for k, v in self.amounts.items()])
        arr['volume'] = sum([v for k, v in self.volumes.items()])
        arr['type'] = self.type

        if self.include_quote:
            arr['askPrice_1'] = self.askPrice_1
            arr['bidPrice_1'] = self.bidPrice_1
            arr['askVol_1'] = self.askVol_1
            arr['bidVol_1'] = self.bidVol_1
            arr['askVol_2'] = self.askVol_2
            arr['bidVol_2'] = self.bidVol_2

    def update_tick(self, tick: np.ndarray, time: int) -> bool:
        """数据增量更新，同一条tick不会重复使用

        后一段会随着新数据到来而更新。前一段已经成为了历史不再变化

        """
        if self.time != time:
            self.time = time
            is_new = True
            self.open_dt = tick['time']
            self.type = tick['type']
            if self.is_minute:
                self.pre_close = self.close
                self.pre_amount = self.last_amount
                self.pre_volume = self.last_volume
                self.open = tick['lastPrice']
                self.high = tick['lastPrice']
                self.low = tick['lastPrice']
            else:
                self.pre_close = tick['lastClose']
                self.open = tick['open']
                self.high = tick['high']
                self.low = tick['low']
        else:
            is_new = False
            if self.is_minute:
                self.high = np.maximum(tick['lastPrice'], self.high)
                self.low = np.minimum(tick['lastPrice'], self.low)
            else:
                self.high = tick['high']
                self.low = tick['low']

        self.close_dt = tick['time']
        self.close = tick['lastPrice']
        self.last_amount = tick['amount']
        self.last_volume = tick['volume']

        if self.include_quote:
            self.askPrice_1 = tick['askPrice_1']
            self.bidPrice_1 = tick['bidPrice_1']
            self.askVol_1 = tick['askVol_1']
            self.bidVol_1 = tick['bidVol_1']
            self.askVol_2 = tick['askVol_2']
            self.bidVol_2 = tick['bidVol_2']

        return is_new

    def update_bar_v1(self, bar: np.ndarray, time: int) -> bool:
        """短周期bar数据，更新成长周期数据

        后一段会随着新数据到来而更新。前一段已经成为了历史不再变化

        """
        if self.time != time:
            self.time = time
            is_new = True
            self.open_dt = bar['open_dt']
            self.type = bar['type']

            self.pre_close = self.close
            self.open = bar['open']
            self.high = bar['high']
            self.low = bar['low']

            self.last_time = bar['time']
            self.pre_amount = 0.0
            self.pre_volume = 0
        else:
            is_new = False
            self.high = np.maximum(bar['high'], self.high)
            self.low = np.minimum(bar['low'], self.low)

        if self.last_time != bar['time']:
            self.last_time = bar['time']
            self.pre_amount += self.last_amount
            self.pre_volume += self.last_volume

        self.last_amount = bar['amount']
        self.last_volume = bar['volume']
        self.close_dt = bar['close_dt']
        self.close = bar['close']

        if self.include_quote:
            self.askPrice_1 = bar['askPrice_1']
            self.bidPrice_1 = bar['bidPrice_1']
            self.askVol_1 = bar['askVol_1']
            self.bidVol_1 = bar['bidVol_1']
            self.askVol_2 = bar['askVol_2']
            self.bidVol_2 = bar['bidVol_2']

        return is_new

    def update_bar_v2(self, bar: np.ndarray, time: int) -> bool:
        """短周期bar数据，更新成长周期数据

        后一段会随着新数据到来而更新。前一段已经成为了历史不再变化

        """
        if self.time != time:
            self.time = time
            is_new = True
            self.open_dt = bar['open_dt']
            self.type = bar['type']

            self.pre_close = self.close
            self.open = bar['open']
            self.high = bar['high']
            self.low = bar['low']

            self.volumes.clear()
            self.amounts.clear()
        else:
            is_new = False
            self.high = np.maximum(bar['high'], self.high)
            self.low = np.minimum(bar['low'], self.low)

        self.close_dt = bar['close_dt']
        self.close = bar['close']
        self.volumes[bar['time']] = bar['volume']
        self.amounts[bar['time']] = bar['amount']

        if self.include_quote:
            self.askPrice_1 = bar['askPrice_1']
            self.bidPrice_1 = bar['bidPrice_1']
            self.askVol_1 = bar['askVol_1']
            self.bidVol_1 = bar['bidVol_1']
            self.askVol_2 = bar['askVol_2']
            self.bidVol_2 = bar['bidVol_2']

        return is_new


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp1 = Dict()
    tmp1[uint64(1)] = uint64(1)
    tmp1.clear()

    tmp2 = Dict()
    tmp2[uint64(1)] = 0.0
    tmp2.clear()

    spec = [
        ('is_minute', boolean),
        ('include_quote', boolean),
        ('index', uint64),
        ('time', uint64),
        ('last_time', uint64),
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
        ('volumes', typeof(tmp1)),
        ('amounts', typeof(tmp2)),
    ]
    Bar = jitclass(spec)(Bar)


class BarManager:

    def __init__(self, arr1: np.ndarray, arr2: np.ndarray, is_minute: bool, include_quote: bool):
        tmp = Dict()
        tmp['600000.SH'] = Bar(0.0, True, True)
        tmp.clear()
        self.bars = tmp

        self.index: int = 0
        self.arr1 = arr1
        self.arr2 = arr2
        self.is_minute = is_minute
        self.include_quote = include_quote

    def reset(self):
        self.bars.clear()
        self.index = 0
        self.arr2[0] = 0

    def extend_ticks(self, ticks: np.ndarray, get_label, get_label_arg1: float) -> Tuple[int, int, int]:
        """来ticks数据，更新bar数据

        tick不能重复，使用for_next()来获取

        """
        last_index = self.index
        for t in ticks:
            if t['open'] == 0:
                # 出现部分股票9点25过几秒open价还是0的情况
                continue
            # TODO 时间戳请选用特别的格式
            time = get_label(t['time'] // 1000, get_label_arg1) * 1000
            if time == 0:
                continue
            stock_code = str(t['stock_code'])
            # if stock_code != '301068.SZ':  # TODO test
            #     continue
            not_in = stock_code not in self.bars
            if not_in:
                self.bars[stock_code] = Bar(t['lastClose'], self.is_minute, self.include_quote)

            bb = self.bars[stock_code]
            if bb.update_tick(t, time):
                bb.index = self.index
                self.index += 1
            bb.fill_tick(self.arr1[bb.index], stock_code)
        # 记录位子
        self.arr2[0] = self.index
        return last_index, self.index, self.index - last_index

    def extend_bars(self, bars: np.ndarray, get_label, get_label_arg1: float) -> Tuple[int, int, int]:
        """来短周期bar数据，更新成长周期数据。

        由于历史重复不好取，所以使用for_all()来获取

        """
        last_index = self.index
        for b in bars:
            # TODO 时间戳请选用特别的格式
            time = get_label(b['time'] // 1000, get_label_arg1) * 1000
            if time == 0:
                continue
            stock_code = str(b['stock_code'])
            # if stock_code != '000001.SZ':  # TODO test
            #     continue
            not_in = stock_code not in self.bars
            if not_in:
                self.bars[stock_code] = Bar(b['preClose'], self.is_minute, self.include_quote)

            bb = self.bars[stock_code]
            if bb.update_bar_v1(b, time):
                bb.index = self.index
                self.index += 1
            bb.fill_bar_v1(self.arr1[bb.index], stock_code)
        # 记录位子
        self.arr2[0] = self.index
        return last_index, self.index, self.index - last_index


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp1 = Dict()
    tmp1['600000.SH'] = Bar(0.0, True, True)
    tmp1.clear()

    idx_type = typeof(np.empty(64, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_STOCK_1m))
    spec = [
        ('bars', typeof(tmp1)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
        ('is_minute', boolean),
        ('include_quote', boolean),
    ]
    BarManager = jitclass(spec)(BarManager)
