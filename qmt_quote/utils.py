"""
与交易平台无关的工具函数

"""
import queue
import random
import string
import threading
from typing import Dict, Any

import numpy as np
import pandas as pd
import polars as pl
from polars import Expr


def ticks_to_dataframe(datas: Dict[str, Dict[str, Any]],
                       now: pd.Timestamp, index_name: str = 'stock_code',
                       level: int = 0, depths=["askPrice", "bidPrice", "askVol", "bidVol"],
                       type: int = -1,
                       ) -> pd.DataFrame:
    """嵌套字典 转 DataFrame

    在全推行情中，接收到的嵌套字典转成DataFrame

    Parameters
    ----------
    datas : dict
        字典数据
    now : pd.Timestamp
        当前时间
    index_name
        索引名，资产名
    level : int
        行情深度
    depths
        深度行情列名
    type:
        类型。0指数1股票


    Returns
    -------
    pd.DataFrame

    """
    df = pd.DataFrame.from_dict(datas, orient="index")
    df["now"] = now
    df["type"] = type

    # 行情深度
    for i in range(level):
        j = i + 1
        new_columns = [f'{c}_{j}' for c in depths]
        df[new_columns] = df[depths].map(lambda x: x[i])

    # 索引股票代码，之后要用
    df.index.name = index_name
    return df


def concat_dataframes_from_dict(datas: Dict[str, pd.DataFrame]) -> pl.DataFrame:
    """ 拼接DataFrame

    Parameters
    ----------
    datas : dict
        字典数据

    """
    return pl.concat([pl.from_dataframe(v).with_columns(stock_code=pl.lit(k)) for k, v in datas.items()])


def cast_datetime(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
    """ 转换时间列
    """
    return df.with_columns(col.cast(pl.Datetime(time_unit="ms", time_zone="Asia/Shanghai")))


def arr_to_pl(arr: np.ndarray, col: Expr = pl.col('time')) -> pl.DataFrame:
    """numpy数组转polars DataFrame"""
    return cast_datetime(pl.from_numpy(arr), col)


def concat_intraday(df1: pl.DataFrame, df2: pl.DataFrame, by1: str = 'stock_code', by2: str = 'time', by3: str = 'duration') -> pl.DataFrame:
    """日内分钟合并，需要排除重复

    数据是分批到来的，所以合成K线也是分批的，但很有可能出现不完整的数据，用duration来排除重复数据,只选最大的

    1. 前一DataFrame后期数据不完整
    2. 后一DataFrame前期数据不完整
    3. 前后DataFrame有重复数据

    """
    if df1 is None:
        return df2

    df = pl.concat([df1, df2], how='vertical')
    return df.sort(by1, by2, by3).unique(subset=[by1, by2], keep='last', maintain_order=True)


def get_common_elements(list1, list2):
    """获取两个列表的共同元素，保持原始顺序"""
    # 使用集合找到共同元素
    common_set = set(list1) & set(list2)
    # 保持原始顺序
    return [x for x in list1 if x in common_set]


def concat_interday(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """日间线合并，不会重复，但格式会有偏差"""
    if df1 is None:
        return df2
    # print(df1.columns)
    # print(df2.columns)
    cols = get_common_elements(df1.columns, df2.columns)
    return pl.concat([df1.select(*cols), df2.select(*cols)], how="vertical")


def calc_factor(df: pl.DataFrame,
                by1: str = 'stock_code', by2: str = 'time',
                close: str = 'close', pre_close: str = 'preClose') -> pl.DataFrame:
    """计算复权因子，使用交易所发布的昨收盘价计算

    Parameters
    ----------
    df : pl.DataFrame
        数据
    by1 : str
        分组字段
    by2 : str
        排序字段
    close : str
        收盘价字段
    pre_close : str
        昨收盘价字段

    Notes
    -----
    不关心是否真发生了除权除息过程，只要知道前收盘价和收盘价不等就表示发生了除权除息

    """
    df = (
        df
        .sort(by1, by2)
        .with_columns(factor1=(pl.col(close).shift(1) / pl.col(pre_close)).fill_null(1).round(8).over(by1, order_by=by2))
        .with_columns(factor2=(pl.col('factor1').cum_prod()).over(by1, order_by=by2))
    )
    return df


def generate_code(length=4):
    """生成验证码"""
    return ''.join(random.sample(string.digits, k=length))


def input_with_timeout(prompt, timeout=10):
    """带有超时的用户输入函数"""
    print(prompt, end='', flush=True)
    user_input = queue.Queue()

    def get_input():
        try:
            text = input()
            user_input.put(text)
        except:
            user_input.put(None)

    # 创建输入线程
    input_thread = threading.Thread(target=get_input)
    input_thread.daemon = True
    input_thread.start()

    # 等待输入或超时
    try:
        result = user_input.get(timeout=timeout)
        return result
    except queue.Empty:
        print()
        return None
