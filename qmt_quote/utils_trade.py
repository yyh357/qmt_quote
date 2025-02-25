import math

import numpy as np
import pandas as pd
from numba import njit
from xtquant import xtconstant
from xtquant.xtconstant import *  # noqa


def to_dict(obj):
    """将xtquant对象转换为字典，排除私有属性和以m_开头的属性"""
    return {attr: getattr(obj, attr) for attr in dir(obj) if not attr.startswith(("__", "m_"))}


def objs_to_dataframe(objs):
    """对象列表转换为DataFrame"""
    return pd.DataFrame.from_records([to_dict(o) for o in objs])


@njit
def adjust_price0(is_buy: bool, priority: int, offset: int,
                  bid_1: float, ask_1: float,
                  last_price: float = 0.0, pre_close: float = 0.0,
                  tick: float = 0.01) -> float:
    """激进或保守，偏移下单

    Parameters
    ----------
    is_buy:bool
    priority:int
        0 现价
        1 对价
        -1 挂价。撤单的概率高
    offset:int
        偏移。正数激进，负数保守，0表示保持不变
    bid_1:float
        卖一价
    ask_1:float
        买一价
    last_price:float
        最新价
    pre_close:float
        昨收价
    tick:float
        最小变动单位

    Notes
    -----
    买一价加一跳不等于卖一价

    """
    if is_buy:
        maker, taker = bid_1, ask_1
    else:
        maker, taker = ask_1, bid_1

    if priority > 0:
        price = taker or maker or last_price or pre_close
    elif priority < 0:
        price = maker or taker or last_price or pre_close
    else:
        price = last_price or pre_close

    if is_buy:
        price += offset * tick
    else:
        price -= offset * tick
    return price


@njit
def adjust_price1(is_buy: bool, price: float,
                  bid_1: float, ask_1: float,
                  last_price: float = 0.0, pre_close: float = 0.0,
                  tick: float = 0.01) -> float:
    """价格笼子。集合竞价时没有价格笼子，可直接报涨跌停价

    （一）买入申报价格不得高于买入基准价格的102%和买入基准价格以上十个申报价格最小变动单位的孰高值；
    （二）卖出申报价格不得低于卖出基准价格的98%和卖出基准价格以下十个申报价格最小变动单位的孰低值。

    Parameters
    ----------
    is_buy:bool
    price
        申报价格
    bid_1
        买一价
    ask_1
        卖一价
    last_price
        最新价
    pre_close
        昨收价
    tick
        最小变动单位

    Returns
    -------
    float
        调整后的价格

    """
    tick10 = tick * 10
    if is_buy:
        base = ask_1 or bid_1 or last_price or pre_close
        return min(max(base * 1.02, base + tick10), price)
    else:
        base = bid_1 or ask_1 or last_price or pre_close
        return max(min(base * 0.98, base - tick10), price)


@njit
def adjust_price2(is_buy: bool, price: float,
                  limit_down: float = 0.0, limit_up: float = 99999.0,
                  ndigits: int = 100) -> float:
    """买卖价不能超过涨跌停价

    Parameters
    ----------
    is_buy:bool
    price
        申报价格
    limit_down
        跌停价
    limit_up
        涨停价
    ndigits
        价格精度，100表示0.01股票, 1000表示0.001基金, 5表示0.2股指期货

    Returns
    -------
    float
        调整后的价格

    Notes
    -----
    TODO 上市第一天的涨幅停牌，这里要再设计

    """
    if is_buy:
        # 买入时价格向下靠拢
        price = math.floor(price * ndigits) / ndigits
        return max(min(limit_up, price), limit_down)
    else:
        # 卖出时价格向上靠拢
        price = math.ceil(price * ndigits) / ndigits
        return min(max(limit_down, price), limit_up)


# @njit
def adjust_quantity(is_buy: bool, is_kcb: bool, quantity: int,
                    can_use_volume: int, tolerance: int = 10) -> int:
    """
    根据股票交易规则调整股票买卖数量

    Parameters
    ----------
    is_buy:bool
        是否买入
    is_kcb:bool
        是否科创板
    quantity:int
        申报数量
    can_use_volume:int
        可用数量. 已经排除了挂单卖出时的冻结frozen_volume
    tolerance:int
        容忍度。遇到数量差异时，是否调整数量。

    Returns
    -------
    int
        调整后的数量

    Notes
    -----
    科创板：单笔申报数量不小于200股，以1股为单位递增
    非科创板：买入时，数量向下取整到 100 的整数倍；卖出时，数量向上取整到 100 的整数倍

    """
    if is_buy:
        if is_kcb:
            # 科创板：单笔申报数量不小于200股，以1股为单位递增
            if quantity < 200 - tolerance:
                # 少于一定数量，不买了
                quantity = 0
            else:
                # 最低200股
                quantity = max(quantity, 200)
        else:
            # 买入时，数量向下取整到 100 的整数倍
            quantity = (quantity + tolerance) // 100 * 100
    else:
        # C  Q
        if quantity + tolerance >= can_use_volume:
            # 数量大于可用数量，全部卖出
            quantity = can_use_volume
        elif is_kcb:
            if quantity + tolerance >= 200:
                quantity = max(quantity, 200)
            else:
                quantity = 0
        else:
            # 1 Q 2 C 3
            if quantity + tolerance >= 100:
                quantity = (quantity + tolerance) // 100 * 100
            else:
                quantity = 0

    return quantity


def cancel_orders(trader, account, orders: pd.DataFrame, direction: int = 0) -> pd.DataFrame:
    """撤单

    Parameters
    ----------
    trader
    account

    orders: pd.DataFrame
        - order_id
        - order_status
        - direction

    direction
        0 全部
        1 买
        -1 卖

    Returns
    -------
    pd.DataFrame
        - seq

    References
    ----------
    https://dict.thinktrader.net/nativeApi/xttrader.html?id=x3GDHP#%E5%A7%94%E6%89%98%E7%8A%B6%E6%80%81-order-status

    """
    if 'order_status' in orders.columns:
        # 55部成 52部成待撤, 这两状态有什么区别
        orders = orders.query(f'(order_status==@ORDER_PART_SUCC) | (order_status<=@ORDER_PARTSUCC_CANCEL)')

    if direction != 0:
        if 'direction' in orders.columns:
            if direction > 0:
                orders = orders.query(f'direction==@DIRECTION_FLAG_BUY')
            else:
                orders = orders.query(f'direction==@DIRECTION_FLAG_SELL')

    # 记录请求序列号
    if 'seq' not in orders.columns:
        orders['seq'] = 0
    for i, row in orders.iterrows():
        orders.loc[i, 'seq'] = trader.cancel_order_stock_async(account, row.order_id)
    return orders


def send_orders(trader, account, transactions: pd.DataFrame, priority: int, offset: int, is_auction: bool, strategy_name: str, order_remark: str) -> pd.DataFrame:
    transactions['seq'] = 0
    transactions['order_type'] = np.where(transactions['is_buy'], xtconstant.STOCK_BUY, xtconstant.STOCK_SELL)
    transactions['price'] = transactions.apply(lambda x: adjust_price0(x.is_buy, priority, offset, x.bidPrice_1, x.askPrice_1, x.lastPrice, x.lastClose, tick=0.01), axis=1)
    if not is_auction:
        transactions['price'] = transactions.apply(lambda x: adjust_price1(x.is_buy, x.price, x.bidPrice_1, x.askPrice_1, x.lastPrice, x.lastClose, tick=0.01), axis=1)
    transactions['price'] = transactions.apply(lambda x: adjust_price2(x.is_buy, x.price, x.DownStopPrice, x.UpStopPrice, ndigits=100), axis=1)
    transactions['order_volume'] = transactions.apply(lambda x: adjust_quantity(x.is_buy, x.is_kcb, x.quantity, x.can_use_volume, tolerance=10), axis=1)  #
    for i, v in transactions.iterrows():
        transactions.loc[i, 'seq'] = trader.order_stock_async(account, v.stock_code, v.order_type, v.order_volume, xtconstant.FIX_PRICE, v.price, strategy_name, order_remark)
    return transactions
