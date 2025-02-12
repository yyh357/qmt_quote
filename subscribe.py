"""
订阅行情数据，保存到内存映射文件中
"""
import pandas as pd
from loguru import logger
from xtquant import xtdata

from config import FILE_INDEX, TICK_INDEX, TOTAL_INDEX, OVERLAP_INDEX  # noqa
from config import FILE_STOCK, TICK_STOCK, TOTAL_STOCK, OVERLAP_STOCK  # noqa
from utils import dict_to_dataframe, get_mmap, update_array

# 开盘前需要先更新板块数据，因为会有新股上市
xtdata.download_sector_data()

G = Exception()
G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
G.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")
G.沪深基金 = xtdata.get_stock_list_in_sector("沪深基金")

# in判断set比list快
# 可以替换成选股票池，减少股票数后处理速度更快
G.沪深A股 = set(G.沪深A股)
G.沪深指数 = set(G.沪深指数)
G.沪深基金 = set(G.沪深基金)

stk1, stk2 = get_mmap(FILE_STOCK, TICK_STOCK, TOTAL_STOCK, readonly=False)
idx1, idx2 = get_mmap(FILE_INDEX, TICK_INDEX, TOTAL_INDEX, readonly=False)

# 索引上的名字，在to_records时会用到,所以这里要剔除
columns_stk = list(TICK_STOCK.names)[1:]
columns_idx = list(TICK_INDEX.names)[1:]


def func(datas):
    # 获取当前时间
    now = pd.Timestamp.now()
    # =======================
    # TODO 这里的set可以替换成日线选股票池，减少股票数后处理速度更快
    d = {k: v for k, v in datas.items() if k in G.沪深A股}
    if len(d) > 0:
        df = dict_to_dataframe(d, level=5, now=now)
        start, step, end = update_array(stk1, stk2, df[columns_stk])
        logger.info("股票：进度 {:.8f}, 条数 {}", round(end / TOTAL_STOCK, 9), step)
    # =======================
    d = {k: v for k, v in datas.items() if k in G.沪深指数}
    if len(d) > 0:
        df = dict_to_dataframe(d, level=0, now=now)
        start, step, end = update_array(idx1, idx2, df[columns_idx])
        logger.info("指数：进度 {:.8f}, 条数 {}", round(end / TOTAL_INDEX, 9), step)


if __name__ == "__main__":
    print("注意：每天开盘前需要清理bin文件和idx文件")

    req = xtdata.subscribe_whole_quote(["SH", "SZ"], func)

    while True:
        x = input("输入`q`退出\n")
        if x == "q":
            break
