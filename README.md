# qmt_quote

迅投MiniQMT全推行情记录

1. 将底层的tick全推行情通过回调转发记录到内存映射文件(`subscribe_tick.py`)
2. 准实时读取并转换成K线数据，变相实现K线全推行情(`subscribe_minute.py`)
3. 分钟级别计算因子触发信号(`strategy_runner.py`)
4. 依据信号手动交易(`trade_manual.py`)

## 基础用法

1. 安装`QMT`
2. 安装`python/conda`。版本不能太低，截至2025年3月，推荐`python3.12`
3. 在虚拟环境中安装`xtquant`
4. 在虚拟环境中`pip install -r requirements.txt`
5. 修改`config.py`中的配置。如：
    - TOTAL_ASSET: 股票+指数 的数量
    - TICKS_PER_MINUTE: 股票1分钟收到的总TICK数量。一般要比实际的大一些，否则溢出报错
    - TOTAL_1t: Tick总记录条数，**一定要预留足够的空间**，否则溢出报错
    - FILE_1d: 数据文件路径。会维护2个文件。一个存数据，一个记录最新位置
    - HISTORY_STOCK_1d: 历史数据保存位置。用于盘前准备历史数据
6. 运行`QMT普通版`, 手动下载历史数据。一般在交易日收盘后16点以后运行
7. 运行`prepare_history.py`, 准备历史数据。会将历史数据转存到`HISTORY_STOCK_1d`等位置
8. 编辑运行`run_tick.bat`, 转存全推行情。需要在开盘前运行，否则错失数据
9. 编辑运行`run_minute.bat`, Tick转K线。可盘中再启动

## 进阶用法

1. `factor_codegen.py`因子计算函数代码生成，可自行编写因子函数。可关注`expr_codegen`和`polars_ta`这两个项目
2. `strategy_runner.py`可修改代码，使用更灵活的生成方式，或叠加更多策略逻辑
3. `trade_manual.py`可修改成自动下单

## 注意

1. 一定要开盘前做好网络时间同步，否则在策略定时触发时可能数据不全
2. 每天开盘前都需要先删除数据文件，否则数据是接后面添加的，会导致运行一段时间后溢出
3. 目前只能一次处理一天的数据，多天处理需要修改K线数据的逻辑
4. 开始运行是会接收一次全推数据，要过滤
5. 接收数据时，小节收盘时，会延迟几秒钟还收到数据，要处理

## 技巧

1. 运行`run_tick.bat`后非常担心不小心将窗口关闭。`Windows Terminal`可以再开一个选项卡，这样多个选项卡关闭时会提示

## 安装

此文件一般已经放到了用户的项目目录下了，但`qmt_quote`由于过于简单，并没有发布到pypi。(也许迭代多次后会发布)
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

## 其它用法

使用`qmt_quote.tools`中函数，对接`通达信条件预警`(get_block_members_tdx)和`同花顺动态板块`(get_block_members_ths)可获取股票列表。