from typing import Dict

import pandas as pd
import polars as pl


def from_dict_dataframe(datas: Dict[str, pd.DataFrame]) -> pl.DataFrame:
    """ 字典转DataFrame
    Parameters
    ----------
    datas : dict
        字典数据

    """
    return pl.concat([pl.from_dataframe(v).with_columns(code=pl.lit(k)) for k, v in datas.items()])


def cast_datetime(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
    """ 转换时间列
    """
    return df.with_columns(col.cast(pl.Datetime(time_unit="ms", time_zone="Asia/Shanghai")))
