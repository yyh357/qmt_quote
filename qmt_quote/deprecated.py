"""
已弃用的函数

弃用原因：
1. 分钟数据以前是按需取tick数据，然后转换成分钟，导致要取时花费时间
2. 现在改成有专用模块一只计算转存分钟线

"""
import polars as pl


def adjust_ticks_time_astock(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
    """调整时间边缘，方便生成与qmt历史数据一致整齐的1分钟K线

    标签打在右边界上

    9点25要调整到9点29
    11点30要调整到11点29
    """
    t = col.dt.time()
    df = df.filter(t >= pl.time(9, 25)).with_columns(
        # 9点25要调整到9点29
        time=pl.when(t < pl.time(9, 29)).then(col.dt.replace(minute=29)).otherwise(col)
    ).with_columns(
        # 11点30要调整到11点29
        time=pl.when((t >= pl.time(11, 30)) & (t < pl.time(11, 31))).then(col.dt.replace(minute=29, second=59, microsecond=100)).otherwise(col)
    )
    return df


def filter_suspend(df: pl.DataFrame) -> pl.DataFrame:
    """过滤停牌数据"""
    # return df.filter(pl.col('suspendFlag') == 0)
    return df.filter(pl.col('volume') > 0, pl.col('high') > 0)


# def process_day():
#     df = arr_to_pl(stk1[slice_1m.for_day()])
#     df = ticks_to_day(df)
#     df = filter_suspend(df)
#     slice_1m.df4 = concat_interday(slice_1m.df2, df)
#     slice_1m.df4 = calc_factor(slice_1m.df4, by1='stock_code', by2='time', close='close', pre_close='preClose')
#     return slice_1m.df4


# def process_min():
#     df = arr_to_pl(stk1[slice_1m.for_minute()])
#     df = adjust_ticks_time_astock(df, col=pl.col('time'))
#     df = ticks_to_minute(df, period="1m")
#     slice_1m.df3 = concat_intraday(slice_1m.df3, df, by1='stock_code', by2='time', by3='duration')
#     slice_1m.df5 = concat_interday(slice_1m.df1, slice_1m.df3)
#     slice_1m.df5 = calc_factor(slice_1m.df5, by1='stock_code', by2='time', close='close', pre_close='preClose')
#     return slice_1m.df5


# # 1分钟数据转5分钟数据，由于要用全量1m数据，感觉效率低,但可用在非实盘环境
# test_5m = False
# if test_5m:
#     start, end, cursor = slice_1m.update(int(arr1m2[0]))
#     arr1m = arr1m1[slice_1m.for_all()]
#     bm_5m.reset()
#     start, end, step = bm_5m.extend_bars(arr1m, get_label_stock_5m, 3600 * 8)