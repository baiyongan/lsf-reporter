import time
from datetime import datetime
import math
import json
import requests
import cmd.yaml_conf
import connect.consul
import schedule
from elasticsearch import Elasticsearch
from elasticsearch import helpers

### 实时更新计费信息，每一分钟执行任务

def timeConsumption(t):
    """
    转换时间戳
    """
    timeArray = time.strptime(t, "%Y-%m-%dT%H:%M:%S.000Z")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp


def getConsul():
    """
    获取 consulKV --- lsf/queue 队列的计费信息
    """
    consul_queue_list = connect.consul.Consul().ConsulClient.kv.get(consulKV, keys=True)[1]
    queue_keys = []
    queue_values = []
    for consul_queue in consul_queue_list:
        consul_queue_key = consul_queue.split('/')[-1]
        consul_queue_tmp = connect.consul.Consul().ConsulClient.kv.get(consul_queue)[1]
        consul_queue_value = json.loads(str(consul_queue_tmp['Value'], encoding="utf-8"))
        queue_keys.append(consul_queue_key)
        queue_values.append(consul_queue_value)
    consul_queue_dict = dict(zip(queue_keys, queue_values))
    return consul_queue_dict


def getRow(index_name):
    url = URL + "_opendistro/_sql"
    headers = {'Content-Type': 'application/json'}
    request_row = {"query": "SELECT COUNT(*) FROM " + str(index_name)}
    current_row = requests.post(url, json=request_row, headers=headers)
    if current_row.status_code == 200:
        row_num = json.loads(current_row.text)["hits"]["total"]["value"]
        print(str(index_name) + " 的当前总数据行数为： " + str(row_num))
        return row_num


def getLog():
    """
    获取 es 中新增的日志数据
    """
    url = URL + "_opendistro/_sql"
    headers = {'Content-Type': 'application/json'}

    log_row_num = getRow(log_current_month)
    job_row_num = getRow(job_current_month)

    logdata = []
    if job_row_num < log_row_num:     #获取新增的日志数据
        request = {"query": "SELECT @timestamp, user_name, queue_name, exec_hosts, command," #作业提交信息
                            "job_name, job_id, job_status, job_group, "  #作业状态信息
                            "submit_time, start_time, end_time, " #作业时间信息
                            "cpu_time, utime, stime,"
                            "event_type, exit_status, exit_reason " #作业结束信息
                            "FROM " + str(log_current_month) + " LIMIT " + str(job_row_num) + " , " + str(log_row_num)}
        logsource = requests.post(url, json=request, headers=headers)
        if logsource.status_code == 200:
            logsource = json.loads(logsource.text)
            loglength = len(logsource["hits"]["hits"])
            for i in range(loglength):
                logdata_i = logsource["hits"]["hits"][i]['_source']
                logdata.append(logdata_i)
            return logdata
    elif job_row_num == log_row_num:
        print("截止至 " + str(datetime.now().replace(microsecond=0)) + " 为止，当月日志数据条目未新增。")
        return logdata


def jobResult():
    """
    作业日志的计费
    """
    jobconsul = getConsul()
    joblogdata = getLog()
    jobresult = []
    for i in range(len(joblogdata)):
        job_status_i = joblogdata[i].get('job_status')
        jobqueue_i = joblogdata[i].get('queue_name') # 为了匹配 consul中的 队列名
        if job_status_i == 'DONE':
            num_slots_i = joblogdata[i].get('exec_hosts')[0].get('num_slots')
            start_time_i = timeConsumption(joblogdata[i].get('start_time'))
            end_time_i = timeConsumption(joblogdata[i].get('end_time'))
            jobtime_i = int(math.ceil((end_time_i - start_time_i) / 3600)) * num_slots_i
            jobprice_i = jobconsul[jobqueue_i].get('price')
            jobcost_i = float(jobprice_i * jobtime_i)
        elif job_status_i == 'EXIT': # 需要分情况——是没运行退出，还是运行出错退出
            jobtime_i = 0.0
            jobcost_i = 0.0
            jobprice_i = 0.0
        #构造新的json
        keys = ['jobprice', 'jobtime', 'jobcost']
        values = [jobprice_i, jobtime_i, jobcost_i]
        jobtmp_i = dict(zip(keys, values))
        jobresult_i = dict(list(joblogdata[i].items()) + list(jobconsul[jobqueue_i].items()) + list(jobtmp_i.items()))
        jobresult_i = json.dumps(jobresult_i)
        print(jobresult_i)
        jobresult.append(jobresult_i)
    return jobresult


def storeJob():
    """
    将重组的计费日志存入 es 中
    """
    jobindex = job_current_month
    indexlist = []  # 存放目前es数据库里所有的索引
    # es = Elasticsearch(hosts="http://16.16.18.157", port=9200)
    es = Elasticsearch(hosts=URL)
    indices = es.indices.get_alias()
    for k, v in indices.items():
        indexlist.append(k)

    # if jobindex in indexlist:
    #     es.delete_by_query(index=jobindex, body={'query': {'match_all': {}}})  # 若存在，则删除索引下全部数据

    if jobindex not in indexlist:
        es.indices.create(index=jobindex)  # 若不存在，新建索引
    es_data = jobResult()
    actions = [
        {
            '_index': jobindex,  # index
            '_type': "_doc",  # type
            '_source': es_i
        }
        for es_i in es_data
    ]
    helpers.bulk(es, actions, index=jobindex, raise_on_error=False, raise_on_exception=False)  # 往es里导入数据
    print("在 " + str(jobindex) + " 中新增 " + str(len(es_data)) + " 条计费作业数据" + "\n")


if __name__ == '__main__':
    with open("conf.yaml") as conf:
        cmd.yaml_conf.YamlConf(conf)

    URL = "http://16.16.18.157:9200/"
    consulKV = "lsf/queue"
    log_current_month = 'lsf-acct-raw-' + str(time.strftime("%Y-%m", time.localtime()))
    job_current_month = 'lsf-acct-jobresult-' + str(time.strftime("%Y-%m", time.localtime()))

    schedule.every(1).minutes.at(":30").do(storeJob)  # 每隔 10 分钟运行一次 job 函数
    while True:
        schedule.run_pending()  # 运行所有可以运行的任务
        time.sleep(1)