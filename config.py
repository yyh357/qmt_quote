import numpy as np

# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出
# 一小时3600秒，每3秒一条数据
TOTAL_STOCK = int(3600 / 3 * 6000 * 4)  # 5000多只股票，4小时
TOTAL_INDEX = int(3600 / 3 * 1000 * 5)  # 500多只指数，5小时。港股交易时间长

# TODO 1分数据合并，为了保证时间能有重叠，使用3分钟(其实2分多一点就可以了)
OVERLAP_STOCK = int(180 / 3 * 6000)
OVERLAP_INDEX = int(180 / 3 * 1000)

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
FILE_STOCK = rf"M:\stock"
FILE_INDEX = rf"M:\index"

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
    ("pvolume", np.uint64),
    # ("volume", np.uint64),
    # ("stockStatus", np.uint32),
    # ("openInt", np.uint32),
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
    ("pvolume", np.uint64),
],
    align=True,
)
