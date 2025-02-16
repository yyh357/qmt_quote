from datetime import datetime, timedelta

from loguru import logger
from xtquant import xtdata

from examples.config import HISTORY_STOCK_1d, HISTORY_INDEX_1d, HISTORY_STOCK_1m, DATA_DIR
from qmt_quote.utils_qmt import get_local_data_wrap

# 开盘前需要先更新板块数据，因为会有新股上市
xtdata.download_sector_data()

G = Exception()
G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
G.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")


def save_1d(start_time, end_time):
    period = '1d'
    print(start_time, end_time, period)
    df = get_local_data_wrap(G.沪深A股, period, start_time, end_time, data_dir=DATA_DIR)
    df.write_parquet(HISTORY_STOCK_1d)
    print('沪深A股===========')
    print(df)
    df = get_local_data_wrap(G.沪深指数, period, start_time, end_time, data_dir=DATA_DIR)
    df.write_parquet(HISTORY_INDEX_1d)
    print('沪深指数===========')
    print(df)


def save_1m(start_time, end_time):
    period = '1m'
    print(start_time, end_time, period)
    df = get_local_data_wrap(G.沪深A股, period, start_time, end_time, data_dir=DATA_DIR)
    df.write_parquet(HISTORY_STOCK_1m)
    print('沪深A股===========')
    print(df)


if __name__ == "__main__":
    print('1. 请先在QMT普通版中手动下数据')
    print('2. 然后在QMT极速版中运行本脚本')
    print('3. 请在合并后的数据中查看范围是否合适，以防上一步的数据下载不全')
    # 下午3点半后才能下载当天的数据
    end_time = datetime.now() - timedelta(hours=15, minutes=30)
    end_time = end_time.strftime("%Y%m%d")
    # end_time = "20250213"  # 测试用，以后要注释

    # ==========
    logger.info('开始转存数据。请根据自己策略预留一定长度的数据')
    start_time = "20250101"
    save_1d(start_time, end_time)
    start_time = "20250201"
    save_1m(start_time, end_time)
