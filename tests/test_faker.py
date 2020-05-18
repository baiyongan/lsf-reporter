# -*- coding: utf-8 -*-

from faker import Faker
import  random

faker = Faker()

random_job_name = faker.pystr()

print(random_job_name)
random_num_exec_hosts = random.randint(1, 16)

print(random_num_exec_hosts)
