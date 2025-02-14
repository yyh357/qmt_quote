"""
订阅行情数据，保存到内存映射文件中
"""
import pandas as pd
from tqdm import tqdm
from xtquant import xtdata

from config import FILE_INDEX, TICK_INDEX, TOTAL_INDEX  # noqa
from config import FILE_STOCK, TICK_STOCK, TOTAL_STOCK  # noqa
from qmt_quote.memory_map import get_mmap, update_array
from qmt_quote.utils_qmt import ticks_to_dataframe

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
        df = ticks_to_dataframe(d, level=5, now=now)
        start, step, end = update_array(stk1, stk2, df[columns_stk])
        pbar_stk.update(step)
    # =======================
    d = {k: v for k, v in datas.items() if k in G.沪深指数}
    if len(d) > 0:
        df = ticks_to_dataframe(d, level=0, now=now)
        start, step, end = update_array(idx1, idx2, df[columns_idx])
        pbar_idx.update(step)


if __name__ == "__main__":
    print("注意：每天开盘前需要清理bin文件和idx文件")
    print()
    print("**输入`q`退出**")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar_stk = tqdm(total=TOTAL_STOCK, desc="股票", initial=int(stk2[0]), bar_format=bar_format, ncols=80)
    pbar_idx = tqdm(total=TOTAL_INDEX, desc="指数", initial=int(idx2[0]), bar_format=bar_format, ncols=80)

    req = xtdata.subscribe_whole_quote(["SH", "SZ"], func)

    while True:
        x = input()
        if x == "q":
            break
