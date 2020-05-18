# -*- coding: utf-8 -*-

from __future__ import absolute_import
from celery_task import cel
from lsf_reporter.bill_management.user_bill_generation import CustomJobBilling


@cel.task(bind=True)
def user_bill_transfer(self, user_name, queue_name_list, query_start_date, query_end_date, storage_list, consul_queue_dict, es_url):
    """
    依据用户请求，生成相应时间跨度的作业，多线程执行
    """
    cjb = CustomJobBilling(user_name=user_name, queue_name_list=queue_name_list, start_date=query_start_date, end_date=query_end_date,
                       storage_list=storage_list, consul_queue_dict=consul_queue_dict, es_url=es_url)

    if len(queue_name_list) == 0:  # 前端没有传递 队列列表参数
        queue_name_list = cjb.query_queue(user_name)
    cjb.generate_bill_template(queue_name_list)
    for q in range(len(queue_name_list)):
        cjb.json_to_excel(queue_name_list[q])
    cjb.brief_bill_info_gather()
    cjb.excel_ready_to_zip()
