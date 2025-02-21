"""
测试因子生成示例
会生成因子计算文件
"""
import polars as pl
from expr_codegen import codegen_exec

from examples.config import HISTORY_STOCK_1d
from qmt_quote.utils import calc_factor
"""
在实盘中，基本只需要每个股票最后一天的因子值，cs_rank等只需要最后一天调用就能大提速
"""

def _code_block_1():
    CLOSE = close * factor2
    MA5 = ts_mean(CLOSE, 5)
    MA10 = ts_mean(CLOSE, 10)


df = pl.read_parquet(HISTORY_STOCK_1d)
df = calc_factor(df, by1='stock_code', by2='time', close='close', pre_close='preClose')
df = codegen_exec(df, _code_block_1, asset='stock_code', date='time', output_file='factor_calc.py')
print(df.tail())
