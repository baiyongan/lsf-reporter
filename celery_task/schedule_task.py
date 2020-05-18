# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from celery_task import cel
from threading import Timer
from lsf_reporter.bill_management.administrator_monthly_report import MonthlyReport
from lsf_reporter.consul_register_parser import parse_queue_consul
from lsf_reporter.elasticsearch_client import init_conn
from lsf_reporter.storage_definition import bill_storage_location
from celery_task.custom_task import user_bill_transfer
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def loop_func(second, func):
    while True:
        timer = Timer(second, func)
        timer.start()
        timer.join()


@cel.task(bind=True)
def add(self, x, y):
    # logger.info(self.request.__dict__)
    time.sleep(3)
    self.update_state(state='PROGRESS')
    return x + y


@cel.task(bind=True)
def all_bill_report(self, storage_list, consul_queue_dict, es_url):
# def all_bill_report(self):
    """
    每月1号晚 03：00定时执行, 在celery_config里面传不了方法的返回值，就在这里重新加载一下配置文件，每月一次，还行。
    """
    # from lsf_reporter import config
    # from lsf_reporter import consul_registration
    # config.load("conf.yaml")
    # consul_registration.init()

    query_end_date = str(datetime.now().date())
    query_start_date = str(datetime.now().date() - relativedelta(months=1))

    mr = MonthlyReport(index_name=storage_list[0], start_date=query_start_date, end_date=query_end_date, storage_list=storage_list, es_url=es_url)
    user_name_list = mr.query_user()
    queue_name_list = []
    bill_task_id = []
    for user_name in user_name_list:
        bill_task = user_bill_transfer.apply_async((user_name, queue_name_list, query_start_date, query_end_date,
                                                    storage_list, consul_queue_dict, es_url))
        bill_task_id.append(bill_task.id)


    def task_status_monitering():
        task_state = [user_bill_transfer.AsyncResult(bill_task_id[i]).state for i in range(len(bill_task_id))]
        print(task_state)
        filterd_task_state = list(filter(lambda x: task_state.count(x) == 1, task_state))
        print(filterd_task_state)
        try:
            if len(filterd_task_state) == 1 and filterd_task_state[0] == 'SUCCESS':
                mr.zip_bills()  # 全部正确，才进行打包操作
                exit(0)
            else:
                pass
        except Exception as e:
            print(e)
            exit(1)

    loop_func(20, task_status_monitering())