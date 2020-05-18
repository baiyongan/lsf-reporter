# -*- coding: utf-8 -*-

from lsf_reporter import config


def init_conn():
    conf = config.CONF
    es_url = conf["elasticsearch"]["url"]
    return es_url
