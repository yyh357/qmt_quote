"""
分钟转换

默认用1分钟转5分钟测试

由于已经有Tick转5分钟了，1分钟转5分钟没有必要，并且当输入K线是变化的时，这个地方实现就非常复杂，所以推荐用Tick来转
"""
# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import os
from typing import Tuple

import numpy as np
from numba import uint64, float32, float64, uint32, typeof, boolean, int8
from numba.experimental import jitclass
from numba.typed.typeddict import Dict

from qmt_quote.dtypes import DTYPE_STOCK_1m


class Bar:
    def __init__(self, pre_close: float, include_quote: bool):
        self.include_quote: bool = include_quote
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
        self.avg_price: float = 0.
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
        arr['avg_price'] = self.avg_price

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
        arr['avg_price'] = self.avg_price

        if self.include_quote:
            arr['askPrice_1'] = self.askPrice_1
            arr['bidPrice_1'] = self.bidPrice_1
            arr['askVol_1'] = self.askVol_1
            arr['bidVol_1'] = self.bidVol_1
            arr['askVol_2'] = self.askVol_2
            arr['bidVol_2'] = self.bidVol_2

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
        self.avg_price = bar['avg_price']

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
        self.avg_price = bar['avg_price']
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
        ('type', int8),
        ('avg_price', float32),
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

    def __init__(self, arr1: np.ndarray, arr2: np.ndarray, include_quote: bool):
        tmp = Dict()
        tmp['600000.SH'] = Bar(0.0, True)
        tmp.clear()
        self.bars = tmp

        self.index: int = 0
        self.arr1: np.ndarray = arr1
        self.arr2: np.ndarray = arr2
        self.include_quote: bool = include_quote

    def reset(self):
        self.bars.clear()
        self.index = 0
        self.arr2[1] = 0

    def extend(self, bars: np.ndarray, get_label, get_label_arg1: float) -> Tuple[int, int, int]:
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
                self.bars[stock_code] = Bar(b['preClose'], self.include_quote)

            bb = self.bars[stock_code]
            if bb.update_bar_v1(b, time):
                bb.index = self.index
                self.index += 1
            bb.fill_bar_v1(self.arr1[bb.index], stock_code)
        # 记录位子
        self.arr2[1] = self.index
        return last_index, self.index, self.index - last_index


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp1 = Dict()
    tmp1['600000.SH'] = Bar(0.0, True)
    tmp1.clear()

    idx_type = typeof(np.empty(4, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_STOCK_1m))
    spec = [
        ('bars', typeof(tmp1)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
        ('include_quote', boolean),
    ]
    BarManager = jitclass(spec)(BarManager)
