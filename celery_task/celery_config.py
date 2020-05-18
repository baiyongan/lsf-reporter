# -*- coding: utf-8 -*-

from __future__ import absolute_import
from datetime import timedelta, datetime
from celery.schedules import crontab
from lsf_reporter import config
from lsf_reporter import consul_register_parser
from lsf_reporter.celery_parameter import celery_url
from lsf_reporter.elasticsearch_client import init_conn
from lsf_reporter.storage_definition import bill_storage_location
from lsf_reporter.consul_register_parser import parse_queue_consul


config.load("conf.yaml")
consul_register_parser.init()
cel_url = celery_url()
es_url = init_conn()
storage_list = bill_storage_location()
# consul_queue_dict = parse_queue_consul(storage_list[1])   ## here exists the disgusting problems about consul
consul_queue_dict = {'admin': {'QueueName': 'admin', 'Price': 1.21, 'ResourceName': 'cpu', 'TimeUnit': 'H', 'CreatedAt': '2020-04-16T09:00:32.563Z', 'Desc': ''}, 'arm': {'QueueName': 'arm', 'Price': 1.12, 'ResourceName': 'cpu', 'TimeUnit': 'H', 'CreatedAt': '2020-04-16T09:00:32.563Z', 'Desc': ''}, 'normal': {'QueueName': 'normal', 'Price': 2, 'ResourceName': 'cpu', 'TimeUnit': 'H', 'CreatedAt': '2020-04-16 17:01:27', 'Desc': 'dg', 'edit': True, 'originalQueueName': 'normal', 'originalPrice': 2, 'deleteVisible': False}, 'owners': {'QueueName': 'owners', 'Price': 5, 'ResourceName': 'cpu', 'TimeUnit': 'H', 'CreatedAt': '2020-04-16T10:07:26.341Z', 'Desc': ''}, 'priority': {'QueueName': 'priority', 'ResourceName': 'cpu', 'Price': 4, 'TimeUnit': 'H', 'Desc': 's', 'CreatedAt': '2020-04-16T09:01:39.343Z'}, 'short': {'QueueName': 'short', 'ResourceName': 'cpu', 'Price': 3, 'TimeUnit': 'H', 'CreatedAt': '2020-04-16T10:07:38.037Z'}}

CELERY_BROKER_URL = cel_url[0]
CELERY_BACKEND_URL = cel_url[1]
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = cel_url[2]

CELERY_IMPORTS = (  # 指定导入的任务模块
    'celery_task.schedule_task',
    'celery_task.custom_task'
)

CELERYBEAT_SCHEDULE = {
    'add-every-10-seconds': {
         'task': 'celery_task.schedule_task.add',
         'schedule': timedelta(seconds=5),       # 每 30 秒执行一次
         'args': (3, 7)                           # 任务函数参数
    },
    'monthly-user-bill-report': {
        'task': 'celery_task.schedule_task.all_bill_report',
        'schedule': crontab(hour=3, minute=0, day_of_month=1),
        # 'schedule': timedelta(minutes=1),
        'args': (storage_list, consul_queue_dict, es_url)
    }
}


