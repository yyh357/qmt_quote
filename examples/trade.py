import pandas as pd
from loguru import logger
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount

from examples.config import FILE_d1d, TOTAL_1d
from qmt_quote.dtypes import DTYPE_STOCK_1m
from qmt_quote.memory_map import get_mmap
from qmt_quote.utils_trade import to_dict, objs_to_dataframe, send_orders, cancel_orders, before_market_open, before_send_orders

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

arr1d1, arr1d2 = get_mmap(FILE_d1d, DTYPE_STOCK_1m, TOTAL_1d, readonly=True)

G = Exception()
details = before_market_open(G)
print("获取当天涨跌停价(含ST/退)：\n", details)


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        logger.info("on_disconnected")

    def on_account_status(self, status):
        """
        :param status: XtAccountStatus 对象
        :return:
        """
        logger.info("on_account_status:{},{},{}", status.account_id, status.account_type, status.status)

    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        logger.info("on_stock_order:{},order_status={},order_sysid={}", order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        logger.info("on_stock_trade:{},{},{}", trade.account_id, trade.stock_code, trade.order_id)

    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        logger.info("on_order_error:{},{},{}", order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        logger.info("on_cancel_error:{},{},{}", cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        logger.info("on_order_stock_async_response:{},order_id={},seq={}", response.account_id, response.order_id, response.seq)

    def on_cancel_order_stock_async_response(self, response):
        """
        :param response: XtCancelOrderResponse 对象
        :return:
        """
        print(to_dict(response))
        logger.info("on_cancel_order_stock_async_response:{},order_id={},seq={}", response.account_id, response.order_id, response.seq)


if __name__ == "__main__":
    print("demo test")
    # path为mini qmt客户端安装目录下userdata_mini路径
    path = rf"D:\e海方舟-量化交易版\userdata_mini"
    # path = rf"D:\迅投极速交易终端 睿智融科版\userdata_mini"
    # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
    session_id = 123456
    xt_trader = XtQuantTrader(path, session_id)
    # 创建资金账号为1000000365的证券账号对象
    acc = StockAccount("290817")
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
            order_remark = input("请输入order_remark:")
            stock_list = ['600000.SH', '000001.SZ', '000638.SZ', '002750.SZ']
            df = before_send_orders(xt_trader, acc, details, arr1d1=None, arr1d2=None)
            df['is_buy'] = True
            df['quantity'] = 200
            df = df.loc[stock_list]
            send_orders(xt_trader, acc, df.reset_index(), -1, -5, False, "手动", order_remark, debug=False)
