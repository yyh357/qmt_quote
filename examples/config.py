import numpy as np

# TODO 历史数据目录
DATA_DIR = r"D:\e海方舟-量化交易版\datadir"

# TODO 1分钟数据量。股票3秒更新一次
MINUTE1_STOCK = int(60 / 3 * 6000)  # 5000多只股票
MINUTE1_INDEX = int(60 / 3 * 1000)  # 500多只指数

# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出
# 一小时3600秒，每3秒一条数据
TOTAL_STOCK = MINUTE1_STOCK * 60 * 4  # 60分钟*4个小时
TOTAL_INDEX = MINUTE1_INDEX * 60 * 4  # 60分钟*5个小时，港股指数时间时间长

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
FILE_STOCK = r"M:\stock"
FILE_INDEX = r"M:\index"

# TODO: 历史数据文件
HISTORY_STOCK_1d = r"M:\stock_1d.parquet"
HISTORY_INDEX_1d = r"M:\index_1d.parquet"
HISTORY_STOCK_1m = r"M:\stock_1m.parquet"
HISTORY_INDEX_1m = r"M:\index_1m.parquet"

# 股票行情结构体
TICK_STOCK = np.dtype([
    ("code", "U9"),
    ("now", "<M8[ns]"),  # 添加本地时间字段
    ("time", np.uint64),
    ("lastPrice", np.float32),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("lastClose", np.float32),
    ("amount", np.float64),
    ("volume", np.uint64),
    # ("pvolume", np.uint64),  # pvolume不维护，因为askVol/bidVol推送过来的都是手,计算vwap时要留意
    # ("stockStatus", np.uint8),  # 废弃了吗？
    # ("openInt", np.uint8),
    # ("transactionNum", np.uint32),
    # ("lastSettlementPrice", np.float32),
    # ("settlementPrice", np.float32),
    # ("pe", np.float32),
    # ("volRatio", np.float32),
    # ("speed1Min", np.float32),
    # ("speed5Min", np.float32),

    ("askPrice_1", np.float32),
    ("askPrice_2", np.float32),
    ("askPrice_3", np.float32),
    ("askPrice_4", np.float32),
    ("askPrice_5", np.float32),
    ("bidPrice_1", np.float32),
    ("bidPrice_2", np.float32),
    ("bidPrice_3", np.float32),
    ("bidPrice_4", np.float32),
    ("bidPrice_5", np.float32),
    ("askVol_1", np.uint32),
    ("askVol_2", np.uint32),
    ("askVol_3", np.uint32),
    ("askVol_4", np.uint32),
    ("askVol_5", np.uint32),
    ("bidVol_1", np.uint32),
    ("bidVol_2", np.uint32),
    ("bidVol_3", np.uint32),
    ("bidVol_4", np.uint32),
    ("bidVol_5", np.uint32),
],
    align=True,
)

# 指数行情结构体
TICK_INDEX = np.dtype([
    ("code", "U9"),
    ("now", "<M8[ns]"),  # 添加本地时间字段
    ("time", np.uint64),
    ("lastPrice", np.float32),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("lastClose", np.float32),
    ("amount", np.float64),
    ("volume", np.uint64),

],
    align=True,
)
