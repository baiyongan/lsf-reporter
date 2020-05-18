from datetime import datetime

timeArray1 = datetime.strptime('2020-01-06T01:18:44.000Z', "%Y-%m-%dT%H:%M:%S.000Z")
timeArray2 = datetime.strptime('2020-01-06T02:18:44.000Z', "%Y-%m-%dT%H:%M:%S.000Z")

print((timeArray2 > timeArray1))