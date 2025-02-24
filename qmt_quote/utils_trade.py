import math

import pandas as pd
from numba import njit
from xtquant.xtconstant import *  # noqa


def to_dict(obj):
    return {attr: getattr(obj, attr) for attr in dir(obj) if not attr.startswith(("__", "m_"))}


def objs_to_dataframe(obj):
    return pd.DataFrame.from_records([to_dict(o) for o in obj])


def price_plan(is_buy: bool, bid_1: float, ask_1: float, last_price: float = 0.0):
    # https://github.com/openctp/openctp/blob/master/demo/ctpping/ThostFtdcUserApiDataType.h#L747
    if is_buy:
        pass
    else:
        pass


@njit
def adjust_price1(is_buy: bool, price: float,
                  bid_1: float, ask_1: float,
                  last_price: float = 0.0, pre_close: float = 0.0,
                  ten_units: float = 0.1) -> float:
    """价格笼子。集合竞价时没有价格笼子，连续竞价时才有价格笼子。

    （一）买入申报价格不得高于买入基准价格的102%和买入基准价格以上十个申报价格最小变动单位的孰高值；
    （二）卖出申报价格不得低于卖出基准价格的98%和卖出基准价格以下十个申报价格最小变动单位的孰低值。

    """
    if is_buy:
        if ask_1 == 0:
            if bid_1 == 0:
                base_price = pre_close if last_price == 0 else last_price
            else:
                base_price = bid_1
        else:
            base_price = ask_1

        return min(max(base_price * 1.02, base_price + ten_units), price)
    else:
        if bid_1 == 0:
            if ask_1 == 0:
                base_price = pre_close if last_price == 0 else last_price
            else:
                base_price = ask_1
        else:
            base_price = bid_1

        return max(min(base_price * 0.98, base_price - ten_units), price)


@njit
def adjust_price2(is_buy: bool, price: float,
                  limit_down: float = 0.0, limit_up: float = 99999.0,
                  ndigits: int = 100) -> float:
    """买卖价不能超过涨跌停价

    Notes
    -----
    TODO 上市第一天的涨幅一定时停牌，这里要再设计

    """
    if is_buy:
        price = math.floor(price * ndigits) / ndigits
        return max(min(limit_up, price), limit_down)
    else:
        price = math.ceil(price * ndigits) / ndigits
        return min(max(limit_down, price), limit_up)


@njit
def adjust_quantity(is_buy: bool, is_kcb: bool, quantity: int,
                    can_use_volume: int, tolerance: int = 10) -> int:
    """
    根据股票交易规则调整股票买卖数量
    """
    if is_buy:
        if is_kcb:
            # 科创板：单笔申报数量不小于200股，以1股为单位递增
            if quantity < 200 - tolerance:
                # 少于一定数量，不买了
                quantity = 0
            else:
                quantity = max(quantity, 200)
        else:
            # 买入时，数量向下取整到 100 的整数倍
            quantity = (quantity + tolerance) // 100 * 100
    else:
        if is_kcb:
            if can_use_volume <= 200:
                quantity = can_use_volume
            else:
                quantity = max(quantity, 200)
                quantity = min(quantity, can_use_volume)
        else:
            if can_use_volume <= 100:
                quantity = can_use_volume
            else:
                quantity = quantity // 100 * 100
                quantity = min(quantity, can_use_volume)

        if can_use_volume - quantity < tolerance:
            quantity = can_use_volume

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
