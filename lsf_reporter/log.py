# -*- coding: utf-8 -*-

import logging
from lsf_reporter import config

_logger = logging.getLogger()


def init():
    conf = config.CONF
    logging.basicConfig(
        datefmt=conf["log"]["datefmt"],
        format=conf["log"]["format"],
        filename=conf["log"]["log_file"],
        filemode="a"
    )
    global _logger
    _logger = logging.getLogger(conf["log"]["logger_name"])
    _logger.setLevel(logging.DEBUG)


def logger():
    return _logger