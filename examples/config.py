# TODO 历史数据目录
DATA_DIR = r"D:\e海方舟-量化交易版\datadir"

# 5000多只股票,500多指数
TOTAL_ASSET = 7000

# 1分钟数据量。股票3秒更新一次
TICKS_PER_MINUTE = int(60 / 3 * TOTAL_ASSET)

# 一天分钟数
TOTAL_1m = TOTAL_ASSET * 60 * 4  # 60分钟*4个小时
TOTAL_5m = TOTAL_1m // 5
TOTAL_1d = TOTAL_ASSET
# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出
# 一小时3600秒，每3秒一条数据
TOTAL_1t = TICKS_PER_MINUTE * 60 * 4  # 60分钟*4个小时

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
FILE_1t = r"M:\1t"
FILE_1m = r"M:\1m"
FILE_5m = r"M:\5m"
FILE_1d = r"M:\1d"

# TODO: 历史数据文件
HISTORY_STOCK_1d = r"F:\stock_1d.parquet"
HISTORY_INDEX_1d = r"F:\index_1d.parquet"
HISTORY_STOCK_1m = r"F:\stock_1m.parquet"
HISTORY_INDEX_1m = r"F:\index_1m.parquet"
HISTORY_STOCK_5m = r"F:\stock_5m.parquet"
