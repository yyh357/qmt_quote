from datetime import datetime

curr_time = datetime(2025, 2, 28, 15, 0).timestamp() // 60 * 60
print("=" * 60)
print(datetime.fromtimestamp((curr_time - 3600 * 8) // 86400 * 86400))
print(datetime.fromtimestamp((curr_time // 86400 * 86400) - 3600 * 8))
print(datetime.fromtimestamp((curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8))

curr_time = datetime(2025, 2, 28, 6, 0).timestamp() // 60 * 60
print("=" * 60)
print(datetime.fromtimestamp((curr_time - 3600 * 8) // 86400 * 86400))
print(datetime.fromtimestamp((curr_time // 86400 * 86400) - 3600 * 8))
print(datetime.fromtimestamp((curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8))

curr_time = datetime(2025, 2, 28, 22, 0).timestamp() // 60 * 60
print("=" * 60)
print(datetime.fromtimestamp((curr_time - 3600 * 8) // 86400 * 86400))
print(datetime.fromtimestamp((curr_time // 86400 * 86400) - 3600 * 8))
print(datetime.fromtimestamp((curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8))

curr_time = datetime(2025, 2, 28, 0, 8).timestamp() // 60 * 60
print("=" * 60)
print(datetime.fromtimestamp((curr_time + 3600 * 0) // 300 * 300 - 3600 * 0 - 300))
print(datetime.fromtimestamp((curr_time + 3600 * 8) // 300 * 300 - 3600 * 8 - 300))
