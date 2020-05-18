# -*- coding: utf-8 -*-
"""
独立运行，用于生成大数据集，进行测试
"""

import time
import json
import requests
import random
from elasticsearch import Elasticsearch, helpers
from pprint import pprint
from faker import Faker
from multiprocessing import Pool
import lsf_reporter.config


class RandomLogGeneration():
    """
    模拟生成 800w 条数据，进行生成账单测试
    """

    def template_log(self):
        """
        获取 es 中的一条原始数据
        """
        url = URL + "_opendistro/_sql"
        headers = {'Content-Type': 'application/json'}
        request = {"query": "SELECT * FROM " + str(log_current_month) + " LIMIT 1"}
        print(request)
        logsource = requests.post(url, json=request, headers=headers)
        if logsource.status_code == 200:
            logsource = json.loads(logsource.text)
            logsource = logsource["hits"]["hits"][0]["_source"]
            pprint(logsource)
            return logsource


    def random_log(self):
        """
        构造一条 random_log_json 数据
        """
        # templatlog = self.template_log()
        faker = Faker()
        # origin_log_json = template_log
        origin_log_json = {'@timestamp': '2020-03-12T16:24:46.000Z',
                             '@version': '1',
                             'app_profile': '',
                             'begin_time': '1970-01-01T00:00:00.000Z',
                             'command': 'sleep 5',
                             'cpu_time': 0.023103,
                             'cwd': '/root',
                             'depend_cond': '',
                             'end_time': '2020-03-12T16:24:46.000Z',
                             'event_type': 'JOB_FINISH',
                             'except_mask': 0,
                             'exec_hosts': [{'host_name': 'node-arm02', 'num_slots': 1}],
                             'exit_info': 0,
                             'exit_reason': 'job finished normally FINISHED_JOB',
                             'exit_status': 0,
                             'job_arr_idx': 0,
                             'job_description': '',
                             'job_group': '',
                             'job_id': 3682,
                             'job_name': 'sleep 5',
                             'job_status': 'DONE',
                             'job_status_code': 64,
                             'max_mem': 0,
                             'max_swap': 0,
                             'num_exec_hosts': 1,
                             'num_processors': 0,
                             'out_file': '',
                             'project_name': 'default',
                             'queue_name': 'arm',
                             'req_num_procs_max': 1,
                             'res_req': '',
                             'runlimit': -1,
                             'sla': '',
                             'start_time': '2020-03-12T16:24:40.000Z',
                             'stime': 0.015477,
                             'submission_host_name': 'master',
                             'submit_time': '2020-03-12T16:24:39.000Z',
                             'user_name': 'test01',
                             'utime': 0.007626,
                             'version': '10.108'}


        random_job_id = random.randint(100, 100000)
        random_job_name = faker.pystr()
        random_submit_time = faker.date(pattern="2019-10-%dT%H:%M:%S.000Z")
        random_start_time = faker.date(pattern="2019-12-%dT%H:%M:%S.000Z")
        random_end_time = faker.date(pattern="2019-12-%dT%H:%M:%S.000Z")
        random_num_exec_hosts = random.randint(1, 16)
        random_queue_name = random.choice(['queue_' + str(x) for x in range(10)])  # 在 consul 中做相应匹配 queue_0 ~ queue_9
        random_user_name = random.choice(['user_' + str(x) for x in range(200)])  # user_0 ~ user_199

        KEYS = ['job_id', 'job_name', 'submit_time', 'start_time', 'end_time',  'num_exec_hosts', 'queue_name', 'user_name']
        VALUES = [random_job_id, random_job_name, random_submit_time, random_start_time, random_end_time, random_num_exec_hosts, random_queue_name, random_user_name]
        random_dict = dict(zip(KEYS, VALUES))

        for k, v in random_dict.items():
            origin_log_json[k] = v
        random_log_json = origin_log_json
        # pprint(random_log_json)
        return random_log_json


    # @TimeCounter  # 多进程用不了，貌似
    def store_log(self, number, frequency, job_test_index, URL):
        """
        将 random_log_json 的日志，批量存入 es 中
        """
        start = time.time()

        jobindex = job_test_index
        indexlist = []  # 存放目前es数据库里所有的索引
        es = Elasticsearch(hosts=URL)
        indices = es.indices.get_alias()
        for k, v in indices.items():
            indexlist.append(k)

        # if jobindex in indexlist:
        #     es.delete_by_query(index=jobindex, body={'query': {'match_all': {}}})  # 若存在，则删除索引下全部数据
        if jobindex not in indexlist:
            es.indices.create(index=jobindex)  # 若不存在，新建索引

        es_data = []
        for frq in range(frequency):
            random_log_json = self.random_log()  # 将random_log() 放在这里，可以Random frequency 次
            for i in range(number):
                es_data.append(random_log_json)  # 每次 number 个一样的random_log_json，所以，要是想足够随机，就使number值小一些
            print("构造 " + str(number) + "条随机日志数据。")
            actions = [
                {
                    '_index': jobindex,  # index
                    '_type': "_doc",  # type
                    '_source': es_i
                }
                for es_i in es_data
            ]
            helpers.bulk(es, actions, index=jobindex, raise_on_error=False, raise_on_exception=False)  # 往es里导入数据
            print("累计第 " + str(frq + 1) + " 次向 " + str(jobindex) + " 中新增随机作业日志" + "\n")
            es_data.clear()

        end = time.time()
        print('插入 {0} 条随机日志, 总计耗时 {1:.2f} seconds.'.format(number * frequency, end - start))


if __name__ == '__main__':

    conf = lsf_reporter.config.CONF

    URL = "http://16.16.18.157:9200/"
    log_current_month = 'lsf-acct-raw-' + str(time.strftime("%Y-%m", time.localtime()))
    job_test_index = 'lsf-fake_data-800w_'

    rlg = RandomLogGeneration()


    # 多进程模式
    p = Pool()
    for i in range(8):
        p.apply_async(rlg.store_log, args=(10, 1000, job_test_index + str(i + 1), URL, ))
    print("Waiting for all subprocesses done...")
    p.close()
    p.join()
    print("All subprocesses done...")

    #  普通模式
    # rlg.store_log(10, 10, job_test_index, URL)
