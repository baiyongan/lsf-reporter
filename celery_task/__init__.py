# -*- coding: utf-8 -*-

from __future__ import absolute_import
from celery import Celery
import celery_task.celery_config
from lsf_reporter.celery_parameter import celery_url


# cel_url = celery_url()
# cel = Celery("celery_task", broker=cel_url[0], backend=cel_url[1])
cel = Celery("celery_task", broker='redis://16.16.18.186:6379/5', backend='redis://16.16.18.186:6379/2')
cel.config_from_object(celery_config, namespace='CELERY')