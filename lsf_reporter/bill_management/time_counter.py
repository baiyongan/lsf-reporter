# -*- coding: utf-8 -*-
"""
计时装饰器，用于评估方法的时间花销
"""

import time
import logging


def TimeCounter(func):
    def wrapper(*args,**kwargs):
        print('Program start running...')
        start = time.time()
        try:
            result = func(*args,**kwargs)
            end = time.time()
            runtime = end - start
            print('Total running time is {0} seconds, which is {1} minutes'.format(runtime, runtime/60))
            return result
        except:
            logging.error('Program running error!')
        finally:
            print('Program stop running!\n')
    return wrapper


