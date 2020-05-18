# -*- coding: utf-8 -*-
import yaml

CONF = {}

def load(conf):
    with open(conf, "r", encoding="utf-8") as file:
        file_data = file.read()
    global CONF
    CONF = yaml.safe_load(file_data)
    return CONF