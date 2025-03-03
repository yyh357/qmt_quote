# https://github.com/wukan1986/LightBT/blob/main/lightbt/enums.py
# 此枚举定义复制于LightBT项目
from typing import NamedTuple


class SizeTypeT(NamedTuple):
    # 空操作指令。通过此值比size全nan能减少代码执行
    NOP: int = 0
    # 下单数量和方向
    Amount: int = 1
    # 下单市值和方向
    Value: int = 2
    # 下单保证金和方向
    Margin: int = 3
    # 正数使用现金比例，负数卖出持仓比例
    Percent: int = 4
    # 目标数量和方向
    TargetAmount: int = 5
    # 目标市值和方向
    TargetValue: int = 6
    # 目标市值百分比。size绝对值之和范围[0,1]
    TargetValuePercent: int = 7
    # 目标市值比例。size值可能为1.5:1:-1等
    TargetValueScale: int = 8
    # 目标保证金和方向
    TargetMargin: int = 9
    # 目标保证金百分比。size绝对值之和范围[0,1]
    TargetMarginPercent: int = 10
    # 目标保证金比例。size值可能为1.5:1:-1等
    TargetMarginScale: int = 11


SizeType = SizeTypeT()
