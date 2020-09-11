from datetime import datetime

str = '2020-05-01'
dt = datetime.strptime(str,'%Y-%m-%d')

print(dt)

print(dt.year)
print(dt.month)
print(dt.day)
