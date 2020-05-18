# -*- coding: utf-8 -*-

import os
import json
import sys
import logging
from flask import send_from_directory, request, jsonify
from flask import Flask, url_for, redirect
from celery import Celery
from celery.result import AsyncResult
import flask_cors
from celery_task.custom_task import user_bill_transfer

from lsf_reporter.consul_register_parser import parse_queue_consul
from lsf_reporter import elasticsearch_client
from lsf_reporter.storage_definition import bill_storage_location


def create_flask_app():
    app = Flask(__name__, instance_relative_config=True)
    flask_cors.CORS(app, supports_credentials=True)


    @app.route("/bill_task", methods=['POST'])
    def bill_transfer():

        param_dict = json.loads(request.get_data(as_text=True))
        user_name_list = param_dict['user_name_list']
        queue_name_list = param_dict["queue_name_list"]
        query_start_date = param_dict["start_date"]
        query_end_date = param_dict["end_date"]

        storage_list = bill_storage_location()
        bill_task_id = []
        bill_task_path = []

        for user_name in user_name_list:
            query_bill_name = "{0}_{1}_from_{2}_to_{3}".format(storage_list[0], user_name, query_start_date, query_end_date)
            logging.info("请求的账单名称为 {0}".format(query_bill_name))
            query_billPath_for_user = "{0}/{1}/{2}.xlsx".format(storage_list[3], user_name, query_bill_name)
            bill_path = os.path.join("{}".format(sys.path[0]), query_billPath_for_user)
            # bill_path = os.path.join("{}".format(sys.path[0]), query_billPath_for_user)

            if os.path.exists(query_billPath_for_user):
                logging.info("The requested bill already exists for {0}".format(user_name))
                bill_task_id.append('No_distribution_task_{0}'.format(query_bill_name))
                bill_task_path.append(bill_path)
            else:
                consul_queue_dict = parse_queue_consul(storage_list[1])
                es_url = elasticsearch_client.init_conn()
                bill_task = user_bill_transfer.apply_async((user_name, queue_name_list, query_start_date, query_end_date,
                                                            storage_list, consul_queue_dict, es_url))
                bill_task_id.append(bill_task.id)
                bill_task_path.append(bill_path.replace('\\', '\\\\'))
        response = {"ID": bill_task_id, "Path": bill_task_path}
        return jsonify(response), 200


    @app.route('/task_state', methods=['POST'])
    def task_state():

        task_info = json.loads(request.get_data(as_text=True))
        print(task_info)
        task_id = task_info["ID"]
        task_path = task_info["Path"]
        task_state = []

        for i in range(len(task_id)):
            print(task_id[i][:20])
            if task_id[i][:20] == "No_distribution_task" and os.path.exists(task_path[i]):
                task_state.append('SUCCESS')
            else:
                task_i = user_bill_transfer.AsyncResult(task_id[i])
                task_state.append(task_i.state)
        response = {
            'State': task_state,
            'ID': task_id,
            'Path': task_path
        }
        print(response)
        return jsonify(response)

    @app.route('/monthly_report', methods=['GET'])
    def monthly_report():
        pass
        return 'URL'

    return app
