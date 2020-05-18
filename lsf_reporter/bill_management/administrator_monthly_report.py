# -*- coding: utf-8 -*-

"""
管理员角度：
    定时任务：每月月初生成总用户账单 ——压缩为一个 zip包
    单用户月度作业账单
    月度账单简表
    作业统计报告
"""


import os
import logging
import zipfile
from datetime import datetime
from dateutil import rrule
from elasticsearch import Elasticsearch


class MonthlyReport():
    """
    每月检查活跃用户，进行账单生成
    """


    def __init__(self, index_name, start_date, end_date, storage_list, es_url):  # 这里的 year, month, day 是起始查询日期

        self.index_name = index_name
        self.storage_list = storage_list
        self.ES = Elasticsearch(hosts=es_url)

        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.year = self.start_date.year
        self.month = self.start_date.month
        self.day = self.start_date.day
        self.timespan = rrule.rrule(freq=rrule.DAILY, dtstart=self.start_date, until=self.end_date).count() - 1
        self.zipPath = "{0}/{1}/{2}/{3}_{1}-{2}.zip".format(self.storage_list[2], self.year, self.month, self.index_name)


    def query_user(self):
        """
        通过es的聚合查询，获取某用户在一定时间，有提交记录的用户名列表
        """
        es = self.ES
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "@timestamp": {
                                "from": self.start_date,
                                "to": self.end_date
                            }
                        }
                    }
                }
            },
            "aggs": {
                "user_name": {
                    "terms": {
                        "field": "user_name"
                    }
                }
            }
        }
        query_user = es.search(index=self.index_name, body=body)
        query_user = query_user["aggregations"]["user_name"]["buckets"]
        user_name_list = []
        if len(query_user) > 0:
            user_name_list = sorted([item['key'] for item in query_user])
            user_name_record_list = [{item['key']: item['doc_count']} for item in query_user]
            logging.info("检索到自 {0} 到 {1} 期间, 在 {2} 日志中，有记录的用户列表为：{3}"
                         .format(self.start_date, self.end_date, self.index_name, user_name_record_list))
        elif len(query_user) == 0:
            logging.warning("未检测到在 {0} 日志中, 存在任何用户提交作业".format(self.index_name))
        return user_name_list


    def zip_bills(self):
        """
        遍历当月的文件夹中的 excel报表，然后压缩为一个zip包
        """
        zip_file = zipfile.ZipFile(self.zipPath, mode='w')
        for file in os.listdir(os.path.dirname(self.zipPath)):
            if file.endswith((".xlsx", ".pdf")):
                zip_file.write("{0}/{1}".format(os.path.dirname(self.zipPath), file), file, compress_type=zipfile.ZIP_DEFLATED)
        zip_file.close()
