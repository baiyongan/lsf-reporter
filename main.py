# -*- coding: utf-8 -*-

import logging
from lsf_reporter import config
from lsf_reporter import consul_register_parser
from lsf_reporter.web import web


def start():
    config.load("conf.yaml")
    consul_register_parser.init()
    consul_register_parser.register()

    app = web.create_flask_app()
    app.run("0.0.0.0", 8081, False)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    start()
