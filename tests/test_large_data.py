# -*- coding: utf-8 -*-

import json
import random
from pprint import pprint
from faker import Faker

faker = Faker()

jsondata = json.load(open("tests.json", "r", encoding="utf-8"))
origin_json = jsondata[0]
pprint(origin_json)

# random_submit_time = faker.date_time_this_month(before_now=True, after_now=False, tzinfo=None)
# random_submit_time = faker.date(pattern="%Y-%m-%dT%H:%M:%S.000Z")
random_submit_time = faker.date(pattern="2019-10-%dT%H:%M:%S.000Z")
random_start_time = faker.date(pattern="2019-12-%dT%H:%M:%S.000Z")
random_end_time = faker.date(pattern="2019-12-%dT%H:%M:%S.000Z")
random_job_id = random.randint(100,100000)
random_num_exec_hosts = random.randint(1, 16)
random_queue_name = random.choice(['queue_' + str(x) for x in range(10)])
random_user_name = random.choice(['user_' + str(x) for x in range(100)])

# print(random_submit_time)
# print(random_start_time)
# print(random_end_time)
# print(random_job_id)
# print(random_num_exec_hosts)
# print(random_queue_name)
# print(random_user_name)

random_json = {
'event_type': 'JOB_FINISH',
'exit_reason': 'job finished normally FINISHED_JOB',
'job_status': 'DONE',
'job_name': 'sleep 10000',
'jobcost': 28.8,
'jobprice': 1.2,
'jobtime': 24,
'submit_time': random_submit_time,
'start_time': random_start_time,
'end_time': random_end_time,
'job_id': random_job_id,
'num_exec_hosts': random_num_exec_hosts,
'queue_name': random_queue_name,
'user_name': random_user_name
}

pprint(random_json)

with open('800w_json_test.json', 'w') as f:
    jsondatalist = []
    for i in range(8000000):
        jsondatalist.append(random_json)
    json.dump(jsondatalist, f)
    pprint(len(jsondatalist))

    print("finish...")

