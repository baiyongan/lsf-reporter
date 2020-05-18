# -*- coding: utf-8 -*-

import os
from lsf_reporter import config


def bill_storage_location():
    conf = config.CONF

    index_name = conf["elasticsearch"]["index_name"]
    queue_KV = conf["consul"]["queue_kv"]
    
    root_dir = conf["storage"]["directory"]["root_dir"]
    bill_dir = conf["storage"]["directory"]["bill_dir"]
    template_dir = conf["storage"]["directory"]["template_dir"]
    detailed_bill = conf["storage"]["bill_type"]["detailed"]
    brief_bill = conf["storage"]["bill_type"]["brief"]
    admin = conf["storage"]["user_category"]["admin"]
    user = conf["storage"]["user_category"]["user"]

    store_path_for_admin = os.path.join(root_dir, bill_dir, admin)
    store_path_for_user = os.path.join(root_dir, bill_dir, user)
    detailed_bill_path = os.path.join(root_dir, template_dir, detailed_bill)
    brief_bill_path = os.path.join(root_dir, template_dir, brief_bill)

    return index_name, queue_KV, store_path_for_admin, store_path_for_user, detailed_bill_path, brief_bill_path