from qmt_quote.utils_trade import adjust_quantity

for is_buy in [True, False]:
    for is_kcb in [True, False]:
        print(f'{is_buy=}, {is_kcb=}', '=' * 60)
        for quantity in range(30, 300, 60):
            for can_use_volume in range(0, 300, 100):
                for tolerance in range(0, 30, 10):
                    print(f'{quantity=}, {can_use_volume=}, {tolerance=}', '>', adjust_quantity(is_buy, is_kcb, quantity, can_use_volume, tolerance))

print('-' * 60)

is_buy = False
is_kcb = True
quantity = 270
can_use_volume = 200
tolerance = 20
print(f'{is_buy=},{is_kcb=}, {quantity=}, {can_use_volume=}, {tolerance=}')
print(adjust_quantity(is_buy, is_kcb, quantity, can_use_volume, tolerance))
