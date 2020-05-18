# -*- coding: utf-8 -*-

import json
import logging
from consul import Consul, Check
from lsf_reporter import config


_consul = Consul()


def init():
    conf = config.CONF
    global _consul
    _consul = Consul(
        host=conf["consul"]["address"],
        port=conf["consul"]["port"],
        dc=conf["consul"]["datacenter"]
    )


def register():
    """注册进入consul中"""
    # 读取配置文件
    conf = config.CONF
    # 生成健康检查配置
    check = Check().tcp(
        host=conf["consul"]["service"]["address"],
        port=conf["consul"]["service"]["port"],
        interval=conf["consul"]["service"]["interval"],
        timeout=conf["consul"]["service"]["timeout"],
        deregister=conf["consul"]["service"]["deregister"]
    )
    # 设置健康检查
    _consul.agent.service.register(
        name=conf["consul"]["service"]["name"],
        service_id=conf["consul"]["service"]["service_id"],
        address=conf["consul"]["service"]["address"],
        port=conf["consul"]["service"]["port"],
        check=check
    )
    logging.info("服务注册进入consul")


def parse_queue_consul(queue_kv):
    """
    获取 consulKV --- lsf/queue 队列的计费信息
    """
    consul_queue_list = _consul.kv.get(queue_kv, keys=True)[1]

    queue_keys = []
    queue_values = []
    for consul_queue in consul_queue_list:
        consul_queue_key = consul_queue.split('/')[-1]
        consul_queue_tmp = _consul.kv.get(consul_queue)[1]
        consul_queue_value = json.loads(str(consul_queue_tmp['Value'], encoding="utf-8"))
        queue_keys.append(consul_queue_key)
        queue_values.append(consul_queue_value)
    consul_queue_dict = dict(zip(queue_keys, queue_values))
    return consul_queue_dict


