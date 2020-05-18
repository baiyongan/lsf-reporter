# -*- coding: utf-8 -*-

from lsf_reporter import config


def celery_url():
    conf = config.CONF

    broker_url = conf['celery']['broker_url']
    backend_url = conf['celery']['backend_url']
    timezone = conf['celery']['timezone']
    return broker_url, backend_url, timezone