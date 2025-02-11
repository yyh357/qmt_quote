import time

from loguru import logger

from config import FILE_INDEX, COUNT_INDEX
from config import FILE_STOCK, COUNT_STOCK
from config import TICK_STOCK, TICK_INDEX
from utils import arr_to_pl, tick_to_day, tick_to_minute, get_mmap

stk1, stk2 = get_mmap(FILE_STOCK, TICK_STOCK, COUNT_STOCK, readonly=False)
idx1, idx2 = get_mmap(FILE_INDEX, TICK_INDEX, COUNT_INDEX, readonly=False)

if __name__ == "__main__":
    print("注意：每天开盘前需要清理bin文件和idx文件")

    while True:
        x = input("输入`q`退出；输入其它键打印最新数据\n")
        if x == "q":
            break

        print("转分钟数据")
        t1 = time.perf_counter()
        df = arr_to_pl(stk1[:stk2[0]])
        df = tick_to_minute(df, period="1m")
        t2 = time.perf_counter()
        logger.info(f"耗时{t2 - t1:.2f}秒")
        print(df.tail(5))

        print("转日线数据")
        t1 = time.perf_counter()
        df = arr_to_pl(idx1[:idx2[0]])
        df = tick_to_day(df)
        t2 = time.perf_counter()
        logger.info(f"耗时{t2 - t1:.2f}秒")
        print(df)
