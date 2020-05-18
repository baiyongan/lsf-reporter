from datetime import datetime
from dateutil.relativedelta import relativedelta


a = datetime(2020, 2, 1, 00, 00, 00, tzinfo=None) + relativedelta(months=+2)
b = datetime(2020, 3, 31, 23, 59, 59) + relativedelta(seconds=1)

print(a)
print(b)


print(datetime.today().replace(microsecond=0))
print(datetime.today() - relativedelta(months=+1))


import datetime
date = datetime.date(2018, 8, 23)



print(date)
print((date - relativedelta(months=9)).year)

import os
folder = 'tmp'
pathname = os.path.join(os.path.abspath('.'), folder)
print(pathname)

print(type(str(datetime.datetime.now())))
print(str(datetime.datetime.now().date()))