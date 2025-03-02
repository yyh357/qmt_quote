from loguru import logger
from xtquant.xttrader import XtQuantTraderCallback


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
        # print(to_dict(response))
        logger.info("on_cancel_order_stock_async_response:{},order_id={},seq={}", response.account_id, response.order_id, response.seq)
