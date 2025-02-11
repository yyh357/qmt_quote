"""
订阅行情数据，保存到内存映射文件中
"""
import pandas as pd
from loguru import logger
from xtquant import xtdata

from config import FILE_INDEX, COUNT_INDEX
from config import FILE_STOCK, COUNT_STOCK
from config import TICK_STOCK, TICK_INDEX
from utils import dict_to_dataframe, get_mmap, update_array

# 开盘前需要先更新板块数据，因为会有新股上市
xtdata.download_sector_data()

A = Exception()
A.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
A.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")
A.沪深基金 = xtdata.get_stock_list_in_sector("沪深基金")

# in判断时，set比list快
A.沪深A股 = set(A.沪深A股)
A.沪深指数 = set(A.沪深指数)
A.沪深基金 = set(A.沪深基金)

stk1, stk2 = get_mmap(FILE_STOCK, TICK_STOCK, COUNT_STOCK, readonly=False)
idx1, idx2 = get_mmap(FILE_INDEX, TICK_INDEX, COUNT_INDEX, readonly=False)

# 索引上的名字，在to_records时会用到,所以这里要剔除
columns_stk = list(TICK_STOCK.names)[1:]
columns_idx = list(TICK_INDEX.names)[1:]


def func(datas):
    # 获取当前时间
    now = pd.Timestamp.now()
    # =======================
    d = {k: v for k, v in datas.items() if k in A.沪深A股}
    if len(d) > 0:
        df = dict_to_dataframe(d, level=5, now=now)
        start, step, end = update_array(stk1, stk2, df[columns_stk])
        logger.info("股票：进度 {:.8f}, 条数 {}", round(end / COUNT_STOCK, 9), step)
    # =======================
    d = {k: v for k, v in datas.items() if k in A.沪深指数}
    if len(d) > 0:
        df = dict_to_dataframe(d, level=0, now=now)
        start, step, end = update_array(idx1, idx2, df[columns_idx])
        logger.info("指数：进度 {:.8f}, 条数 {}", round(end / COUNT_INDEX, 9), step)


if __name__ == "__main__":
    print("注意：每天开盘前需要清理bin文件和idx文件")

    req = xtdata.subscribe_whole_quote(["SH", "SZ"], func)

    while True:
        x = input("输入`q`退出\n")
        if x == "q":
            break
