"""
保存文件时的格式
"""
import numpy as np

DTYPE_STOCK_1t = np.dtype([
    ("stock_code", "U9"),
    ("now", np.uint64),  # 添加本地时间字段
    ("time", np.uint64),
    ("lastPrice", np.float32),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("lastClose", np.float32),
    ("amount", np.float64),
    ("volume", np.uint64),
    # ("pvolume", np.uint64),  # pvolume不维护，因为askVol/bidVol推送过来的都是手,计算vwap时要留意
    # ("stockStatus", np.int8),  # 废弃了吗？
    ("openInt", np.int8),
    ("type", np.int8),
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

DTYPE_STOCK_1m = np.dtype([
    ("stock_code", "U9"),
    ("time", np.uint64),
    ("open_dt", np.uint64),
    ("close_dt", np.uint64),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("close", np.float32),
    ("preClose", np.float32),
    ("amount", np.float64),
    ("volume", np.uint64),
    ("type", np.int8),
    # 分钟数据加入的字段，方便下单时直接取价格
    ("askPrice_1", np.float32),
    ("bidPrice_1", np.float32),
    ("askVol_1", np.uint32),
    ("bidVol_1", np.uint32),
    ("askVol_2", np.uint32),
    ("bidVol_2", np.uint32),
],
    align=True,
)
