from qmt_quote.enums import BoardType
from qmt_quote.utils_trade import adjust_quantity

for is_buy in [True, False]:
    for board_type in [BoardType.KCB, BoardType.SH]:
        print(f'{is_buy=}, {board_type=}', '=' * 60)
        for quantity in range(30, 300, 60):
            for can_use_volume in range(0, 300, 100):
                for tolerance in range(0, 30, 10):
                    print(f'{quantity=}, {can_use_volume=}, {tolerance=}', '>', adjust_quantity(is_buy, board_type, quantity, can_use_volume, tolerance))

print('-' * 60)

is_buy = False
board_type = BoardType.KCB
quantity = 270
can_use_volume = 200
tolerance = 20
print(f'{is_buy=},{board_type=}, {quantity=}, {can_use_volume=}, {tolerance=}')
print(adjust_quantity(is_buy, board_type, quantity, can_use_volume, tolerance))
