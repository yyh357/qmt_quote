"""
信号记录

类似于K线的维护方式，策略中产生信号，
1. 按tick一直顺序存。取数据时要取最近的重叠一部分
2. 按日线一天一条存。取数据时全取即可，数据量也不大
3. 按分钟线存。相当于记录了历史，但只要数据和算法不变就能还原

留了3个字段，用户可根据需要选择对应字段使用

多加了一个策略ID字段，方便在策略中区分不同的信号

"""
# import os
#
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import os
from typing import Tuple

import numpy as np
from numba import uint64, float32, typeof, int32, boolean, int16
from numba.experimental import jitclass
from numba.typed.typeddict import Dict

from qmt_quote.dtypes import DTYPE_SIGNAL_1m


class Bar:
    def __init__(self):
        self.time: int = 0  # 当前bar时间戳
        self.index: int = 0
        self.strategy_id: int = 0  # 策略ID
        self.open_dt: int = 0
        self.close_dt: int = 0
        self.float32: float = 0.
        self.int32: int = 0
        self.boolean: bool = False

    def fill(self, arr: np.ndarray, stock_code: str) -> None:
        """
        """
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['strategy_id'] = self.strategy_id
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['float32'] = self.float32
        arr['int32'] = self.int32
        arr['boolean'] = self.boolean

    def update(self, signal: np.ndarray, time: int) -> bool:
        """数据增量更新，同一条tick不会重复使用

        后一段会随着新数据到来而更新。前一段已经成为了历史不再变化

        """
        if self.time != time:
            self.time = time
            is_new = True
            self.open_dt = signal['time']
            self.strategy_id = signal['strategy_id']
        else:
            is_new = False

        self.close_dt = signal['time']
        self.float32 = signal['float32']
        self.int32 = signal['int32']
        self.boolean = signal['boolean']

        return is_new


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    spec = [
        ('index', uint64),
        ('time', uint64),
        ('strategy_id', int16),
        ('open_dt', uint64),
        ('close_dt', uint64),
        ('float32', float32),
        ('int32', int32),
        ('boolean', boolean),
    ]
    Bar = jitclass(spec)(Bar)


class BarManager:

    def __init__(self, arr1: np.ndarray, arr2: np.ndarray):
        tmp1 = Dict()
        tmp1[('600000.SH', -1)] = Bar()
        tmp1.clear()
        self.bars = tmp1
        # self.bars = dict()

        self.index: int = 0
        self.arr1: np.ndarray = arr1
        self.arr2: np.ndarray = arr2

    def reset(self):
        self.bars.clear()
        self.index = 0
        self.arr2[0] = 0

    def extend(self, signals: np.ndarray, get_label, get_label_arg1: int) -> Tuple[int, int, int]:
        """来ticks数据，更新bar数据

        tick不能重复，使用for_next()来获取

        """
        last_index = self.index
        for s in signals:
            # TODO 时间戳请选用特别的格式
            time = get_label(s['time'] // 1000, get_label_arg1) * 1000
            stock_code = str(s['stock_code'])
            key = stock_code, int(s['strategy_id'])
            not_in = key not in self.bars
            if not_in:
                self.bars[key] = Bar()

            bb = self.bars[key]
            if bb.update(s, time):
                bb.index = self.index
                self.index += 1
            bb.fill(self.arr1[bb.index], stock_code)
        # 记录位子
        self.arr2[0] = self.index
        return last_index, self.index, self.index - last_index


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp1 = Dict()
    tmp1[('600000.SH', -1)] = Bar()
    tmp1.clear()

    idx_type = typeof(np.empty(64, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_SIGNAL_1m))
    spec = [
        ('bars', typeof(tmp1)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
    ]
    BarManager = jitclass(spec)(BarManager)
