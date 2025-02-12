r"""
此文件一般已经放到了用户的项目目录下了，但qmt_quote由于过于简单，并没有发布到pypi。
有两种方式可以使用，选用一种即可。

1. 手动添加到sys.path中，简单粗暴。但代码运行中才添加，所以IDE无法识别会有警告
```
import sys
sys.path.insert(0, r"D:\GitHub\qmt_quote")
```

2. 到`D:\Users\Kan\miniconda3\envs\py312\Lib\site-packages`目录下，
   新建一个`qmt_quote.pth`文件，IDE可识别，内容为：
```
D:\GitHub\qmt_quote
```

"""
import time

from loguru import logger

from config import FILE_INDEX, TICK_INDEX, TOTAL_INDEX, OVERLAP_INDEX  # noqa
from config import FILE_STOCK, TICK_STOCK, TOTAL_STOCK, OVERLAP_STOCK  # noqa
from utils import arr_to_pl, get_mmap, filter__0930_1130__1300_1500, tick_to_minute, tick_to_day, RangeUpdater, concat_unique

stk1, stk2 = get_mmap(FILE_STOCK, TICK_STOCK, TOTAL_STOCK, readonly=True)
idx1, idx2 = get_mmap(FILE_INDEX, TICK_INDEX, TOTAL_INDEX, readonly=True)

ru = RangeUpdater(overlap=OVERLAP_STOCK, step=OVERLAP_STOCK * 10)

if __name__ == "__main__":
    print("注意：每天开盘前需要清理bin文件和idx文件")

    while True:
        x = input("输入`q`退出；输入其它键打印最新数据\n")
        if x == "q":
            break

        print("最新5条原始数据==================")
        df = stk1[:stk2[0]][-5:]
        print(df)

        print("转日线数据==================数据量少可整体转换")
        t1 = time.perf_counter()
        df = arr_to_pl(idx1[:idx2[0]])
        df = tick_to_day(df)
        t2 = time.perf_counter()
        logger.info(f"耗时{t2 - t1:.2f}秒")
        print(df)

        print("转分钟数据==================数据量大分批转换")
        t1 = time.perf_counter()
        start, end, current = ru.update(int(stk2[0]))
        print(start, end, current)
        df = arr_to_pl(stk1[ru.start:ru.end])
        df = filter__0930_1130__1300_1500(df, col="_time_")
        df = tick_to_minute(df, period="1m")
        ru.df = concat_unique(ru.df, df)
        t2 = time.perf_counter()
        logger.info(f"耗时{t2 - t1:.2f}秒")
        print(df)
