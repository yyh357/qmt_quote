# =====历史行情======
# TODO 历史数据目录，使用实盘QMT标准版，可以手工下载历史数据，然后在miniquant中读出,也可用第三方数据
DATA_DIR = r"D:\e海方舟-量化交易版\datadir"

# TODO: 历史数据文件
HISTORY_STOCK_1d = r"F:\stock_1d.parquet"
HISTORY_INDEX_1d = r"F:\index_1d.parquet"
HISTORY_STOCK_1m = r"F:\stock_1m.parquet"
HISTORY_INDEX_1m = r"F:\index_1m.parquet"
HISTORY_STOCK_5m = r"F:\stock_5m.parquet"

# =====实时行情======
# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出
# 5000多只股票,500多个指数
TOTAL_ASSET = 7000

# 1分钟数据量。股票3秒更新一次
TICKS_PER_MINUTE = int(60 / 3 * TOTAL_ASSET)

# 一天分钟数
TOTAL_1m = TOTAL_ASSET * 60 * 4  # 60分钟*4个小时
TOTAL_5m = TOTAL_1m // 5
TOTAL_1d = TOTAL_ASSET
TOTAL_1t = TICKS_PER_MINUTE * 60 * 4  # 60分钟*4个小时

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
# 数据
FILE_d1t_H1 = r"F:\backup\20250520\d1t.npy"  # 历史tick数据
FILE_d1t = r"M:\d1t.npy"
FILE_d1m = r"M:\d1m.npy"
FILE_d5m = r"M:\d5m.npy"
FILE_d1d = r"M:\d1d.npy"

# 备份目录
BACKUP_DIR = r"F:\backup"

# =====交易设置======
# 信号
FILE_s1t = r"M:\s1t.npy"
FILE_s1d = r"M:\s1d.npy"

# TODO 交易目录，miniquant版
USERDATA_DIR = r"D:\迅投极速交易终端 睿智融科版\userdata_mini"
# TODO: 交易账号
ACCOUNT = "2025727"
