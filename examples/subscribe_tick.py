"""
订阅行情数据，保存到内存映射文件中
1. 启动前请先同步网络时间
2. 股票请在9点14分钟前启动
3. 启动前确保数据文件合适大小
4. 15点以后再关闭，否则错失中间数据
"""
from datetime import datetime

from tqdm import tqdm
from xtquant import xtdata

from examples.config import FILE_d1t, TOTAL_1t
from qmt_quote.dtypes import DTYPE_STOCK_1t
from qmt_quote.enums import InstrumentType
from qmt_quote.memory_map import get_mmap, update_array2
from qmt_quote.utils import ticks_to_dataframe, generate_code, input_with_timeout

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
print(f"沪深A股:{len(G.沪深A股)}, 沪深指数:{len(G.沪深指数)}, 沪深基金:{len(G.沪深基金)},")

d1t1, d1t2 = get_mmap(FILE_d1t, DTYPE_STOCK_1t, TOTAL_1t, readonly=False)

# 索引上的名字，在to_records时会用到,所以这里要剔除
columns = list(DTYPE_STOCK_1t.names)[1:]


def func(datas):
    # 获取当前时间转ms
    now = datetime.now().timestamp()
    now_ms = int(now * 1000)

    step_ = 0
    end_ = 0
    # =======================
    # TODO 这里的set可以替换成日线选股票池，减少股票数后处理速度更快
    d = {k: v for k, v in datas.items() if k in G.沪深A股}
    if len(d) > 0:
        df = ticks_to_dataframe(d, now=now_ms, index_name='stock_code', level=5,
                                depths=["askPrice", "bidPrice", "askVol", "bidVol"], type=InstrumentType.Stock)
        start, end, step = update_array2(d1t1, d1t2, df[columns], index=True)
        step_ += step
        end_ = end
    # =======================
    d = {k: v for k, v in datas.items() if k in G.沪深指数}
    if len(d) > 0:
        df = ticks_to_dataframe(d, now=now_ms, index_name='stock_code', level=5,
                                depths=["askPrice", "bidPrice", "askVol", "bidVol"], type=InstrumentType.Index)
        start, end, step = update_array2(d1t1, d1t2, df[columns], index=True)
        step_ += step
        end_ = end
    # =======================
    if step_ > 0:
        t = d1t1[end_ - 1]['time'] / 1000
        # 这里没有必要refresh这么快
        pbar.set_description(f"延时 {now - t:8.3f}s", refresh=False)
        pbar.update(step_)


if __name__ == "__main__":
    print("=" * 60)
    print("启动前请先同步网络时间")
    print(f"当前指针：{d1t2[0]}")
    print('注意：仅在早上开盘前**重置文件指针**，用于覆盖昨天旧数据。盘中使用会导致今日已收数据被覆盖')
    code1 = generate_code(4)
    code2 = input_with_timeout(f"20秒內输入验证码重置文件指针({code1}/回车忽略)：", timeout=20)
    if code2 == code1:
        d1t2[0] = 0
        print("!!!重置文件指针成功!!!")
    print()
    print("开始订阅行情，**输入`:q`退出**")
    print()

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    pbar = tqdm(total=len(d1t1), desc="股票+指数", initial=int(d1t2[0]), bar_format=bar_format, ncols=100)

    req = xtdata.subscribe_whole_quote(["SH", "SZ"], func)

    while True:
        x = input()
        if x == ":q":
            break
