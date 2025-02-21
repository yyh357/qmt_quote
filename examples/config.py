# TODO 历史数据目录
DATA_DIR = r"D:\e海方舟-量化交易版\datadir"

# TODO 1分钟数据量。股票3秒更新一次
MINUTE1_STOCK = int(60 / 3 * 6000)  # 5000多只股票
MINUTE1_INDEX = int(60 / 3 * 1000)  # 500多只指数

# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出
# 一小时3600秒，每3秒一条数据
TOTAL_STOCK_1t = MINUTE1_STOCK * 60 * 4  # 60分钟*4个小时
TOTAL_INDEX_1t = MINUTE1_INDEX * 60 * 4  # 60分钟*5个小时，港股指数时间时间长
TOTAL_STOCK_1m = 6000 * 60 * 4  # 60分钟*4个小时
TOTAL_INDEX_1m = 1000 * 60 * 4  # 60分钟*4个小时
TOTAL_STOCK_1d = 6000  # 60分钟*4个小时
TOTAL_INDEX_1d = 1000  # 60分钟*4个小时

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
FILE_STOCK_1t = r"M:\stock_1t"
FILE_INDEX_1t = r"M:\index_1t"
FILE_STOCK_1m = r"M:\stock_1m"
FILE_INDEX_1m = r"M:\index_1m"
FILE_STOCK_1d = r"M:\stock_1d"
FILE_INDEX_1d = r"M:\index_1d"

# TODO: 历史数据文件
HISTORY_STOCK_1d = r"M:\stock_1d.parquet"
HISTORY_INDEX_1d = r"M:\index_1d.parquet"
HISTORY_STOCK_1m = r"M:\stock_1m.parquet"
HISTORY_INDEX_1m = r"M:\index_1m.parquet"
