"""
1. subscribe_tick.py 实时行情录制
2. subscribe_minute.py 转分钟日线
3. strategy.py 策略信号生成
4. trade_manual.py 读取信号手动下单
"""
import sys
from pathlib import Path

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

import time

import pandas as pd
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

from examples.config import FILE_d1d, TOTAL_1d, USERDATA_DIR, ACCOUNT, FILE_s1d
from examples.strategy_runner import STRATEGY_COUNT
from qmt_quote.dtypes import DTYPE_STOCK_1m, DTYPE_SIGNAL_1m
from qmt_quote.enums import SizeType
from qmt_quote.memory_map import get_mmap
from qmt_quote.trader_callback import MyXtQuantTraderCallback
from qmt_quote.utils_trade import to_dict, objs_to_dataframe, cancel_orders, before_market_open, send_orders_1, send_orders_2, send_orders_3, send_orders_4, send_orders_5

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# 取行情
d1d1, d1d2 = get_mmap(FILE_d1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=True)
# 取信号
s1d1, s1d2 = get_mmap(FILE_s1d, DTYPE_SIGNAL_1m, TOTAL_1d * STRATEGY_COUNT, readonly=True)

G = Exception()
details = before_market_open(G)
print("获取当天涨跌停价(含ST/退)：\n", details)

if __name__ == "__main__":
    print("demo test")
    callback = MyXtQuantTraderCallback()
    xt_trader = XtQuantTrader(USERDATA_DIR, int(time.time()), callback)
    acc = StockAccount(ACCOUNT)
    # 启动交易线程
    xt_trader.start()
    # 建立交易连接，返回0表示连接成功
    connect_result = xt_trader.connect()
    print("connect", connect_result)
    # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
    subscribe_result = xt_trader.subscribe(acc)
    print("subscribe", subscribe_result)

    debug = True

    while True:
        print(":q 退出/0. 切换debug")
        print(f"1. 查资金/2. 查持仓/3. 查委托/4. 撤单/5. 下单({debug=})")
        choice = input()
        if choice == ":q":
            break
        if choice == "0":
            debug = not debug
            continue
        if choice == "1":
            asset = xt_trader.query_stock_asset(acc)
            print(to_dict(asset))
            continue
        if choice == "2":
            positions = xt_trader.query_stock_positions(acc)
            df = objs_to_dataframe(positions)
            print(df)
            continue
        if choice == "3":
            orders = xt_trader.query_stock_orders(acc)
            df = objs_to_dataframe(orders)
            print(df)
            continue
        if choice == "4":
            order_remark = input("请输入order_remark:")
            orders = xt_trader.query_stock_orders(acc)
            df = objs_to_dataframe(orders)
            df = cancel_orders(xt_trader, acc, df, order_remark=order_remark, do_async=False)
            print(df)
            continue
        if choice == "5":
            order_remark = input("请输入order_remark:")

            df = send_orders_1(xt_trader, acc, details, d1d1=d1d1, d1d2=d1d2)

            # 等市值买入
            arr = s1d1[:int(s1d2[0])]
            # 条件过滤
            arr = arr[arr['boolean']]
            if arr.size == 0:
                print("没有符合条件的股票")
                continue

            df = send_orders_2(df, pd.DataFrame(arr), 0.05, or_volume=True)

            df = send_orders_3(xt_trader, acc, df, SizeType.TargetValuePercent)
            df = send_orders_4(df, -1, 0, False)
            df = send_orders_5(xt_trader, acc, df, order_remark, debug=debug)
