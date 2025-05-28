from datetime import datetime

import pandas as pd
from npyt import NPYT

from examples.config import FILE_d1t
from qmt_quote.dtypes import DTYPE_STOCK_1t

d1t = NPYT(FILE_d1t, dtype=DTYPE_STOCK_1t).load(mmap_mode="r")

print(d1t.tail(100))

df = pd.DataFrame(d1t.tail(100))
print(pd.to_datetime(df['now'] + 3600 * 8 * 1000, unit='ms'))  # 差8小时调整
print(pd.to_datetime(df['now'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai'))  # 差8小时调整
print((df['now'] + 3600 * 8 * 1000).astype("datetime64[ms]"))  # 差8小时调整
print((df['now'] + 3600 * 8 * 1000).astype("<M8[ms]"))  # 差8小时调整
print(datetime.fromtimestamp(float(df['now'].iloc[-1] / 1000)))  # 要缩小1000
