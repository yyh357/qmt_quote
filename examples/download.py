from datetime import datetime, timedelta

from loguru import logger
from xtquant import xtdata

from examples.config import HISTORY_STOCK_1d, HISTORY_INDEX_1d, HISTORY_STOCK_1m
from qmt_quote.utils_qmt import download_history_data2_task, get_market_data_ex_task

# 开盘前需要先更新板块数据，因为会有新股上市
xtdata.download_sector_data()

G = Exception()
G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
G.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")


def download_1d(start_time, end_time):
    period = '1d'
    print(start_time, end_time, period)
    download_history_data2_task("沪深A股_1d", G.沪深A股, period=period, start_time=start_time, end_time=end_time)
    download_history_data2_task("沪深指数_1d", G.沪深指数, period=period, start_time=start_time, end_time=end_time)


def download_1m(start_time, end_time):
    period = '1m'
    print(start_time, end_time, period)
    download_history_data2_task("沪深A股", G.沪深A股, period='period', start_time=start_time, end_time=end_time)


def save_1d(start_time, end_time):
    period = '1d'
    print(start_time, end_time, period)
    df = get_market_data_ex_task(G.沪深A股, period=period, start_time=start_time, end_time=end_time)
    df.write_parquet(HISTORY_STOCK_1d)
    df = get_market_data_ex_task(G.沪深指数, period=period, start_time=start_time, end_time=end_time)
    df.write_parquet(HISTORY_INDEX_1d)


def save_1m(start_time, end_time):
    period = '1m'
    print(start_time, end_time, period)
    df = get_market_data_ex_task(G.沪深A股, period=period, start_time=start_time, end_time=end_time)
    df.write_parquet(HISTORY_STOCK_1m)


if __name__ == "__main__":
    start_time = "20250101"
    # 下午3点半后才能下载当天的数据
    end_time = datetime.now() - timedelta(hours=15, minutes=30)
    end_time = end_time.strftime("%Y%m%d")
    end_time = "20250213"  # 测试用
    # ==========
    logger.info("开始下载")
    #download_1d(start_time, end_time)
    download_1m(start_time, "20250213")
    logger.info("下载完成")
    # ==========
    start_time = "20250101"
    save_1d(start_time, end_time)
    start_time = "20250212"
    save_1m(start_time, end_time)
