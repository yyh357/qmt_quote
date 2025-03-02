import pandas as pd
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

from examples.config import FILE_d1d, TOTAL_1d
from qmt_quote.dtypes import DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap
from qmt_quote.trader_callback import MyXtQuantTraderCallback
from qmt_quote.utils_trade import to_dict, objs_to_dataframe, cancel_orders, before_market_open, send_orders_1, send_orders_2, send_orders_3, send_orders_4

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

d1d1, d1d2 = get_mmap(FILE_d1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=True)

G = Exception()
details = before_market_open(G)
print("获取当天涨跌停价(含ST/退)：\n", details)

if __name__ == "__main__":
    print("demo test")
    # path为mini qmt客户端安装目录下userdata_mini路径
    path = rf"D:\e海方舟-量化交易版\userdata_mini"
    # path = rf"D:\迅投极速交易终端 睿智融科版\userdata_mini"
    # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
    session_id = 123456
    xt_trader = XtQuantTrader(path, session_id)
    # 创建资金账号为1000000365的证券账号对象
    acc = StockAccount("1300290817")
    # 创建交易回调类对象，并声明接收回调
    callback = MyXtQuantTraderCallback()
    xt_trader.register_callback(callback)
    # 启动交易线程
    xt_trader.start()
    # 建立交易连接，返回0表示连接成功
    connect_result = xt_trader.connect()
    print("connect", connect_result)
    # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
    subscribe_result = xt_trader.subscribe(acc)
    print("subscribe", subscribe_result)

    while True:
        print("1. 查资金")
        print("2. 查持仓")
        print("3. 查委托")
        print("4. 撤单")
        print("5. 下单")
        choice = input()
        if choice == ":q":
            break
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
            df = send_orders_1(xt_trader, acc, details, d1d1=d1d1, d1d2=d1d2)
            df['size'] = 3000
            df = send_orders_2(xt_trader, acc, df, 'Value')
            df = send_orders_3(df, 1, 1, False)
            # df = df.loc[['688238.SH']]
            df = send_orders_4(xt_trader, acc, df, 'ss', 'bb', True)

            # order_remark = input("请输入order_remark:")
            # stock_list = ['600000.SH', '000001.SZ', '000638.SZ', '002750.SZ']
            # df = send_orders_0(xt_trader, acc, details, d1d1=d1d1, d1d2=d1d2)
            # df['is_buy'] = True
            # df['quantity'] = 200
            # df = df.loc[stock_list]
            # send_orders(xt_trader, acc, df.reset_index(), -1, -5, False, "手动", order_remark, debug=False)
