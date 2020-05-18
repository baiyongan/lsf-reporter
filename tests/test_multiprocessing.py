# -*- coding: utf-8 -*-

import multiprocessing as mp
from multiprocessing import Pool
import time
import os


def get_html(n):
    time.sleep(n)
    print('sub process %s' % n)
    return n


def time_test(process_num):
    start_time = time.time()
    time.sleep(2)
    end_time = time.time()
    print(end_time - start_time)


def long_time_task(name):
    # print('Run task {0} ({1})'.format(name, os.getpid()))
    start = time.time()
    time.sleep(3)
    end = time.time()
    print(end - start)
    # print('Task {0} runs {1:.2f} seconds.'.format(name, end - start))


if __name__ == '__main__':
    # 多进程编程
    # process = mp.Process(target=get_html, args=(2,))
    # process.start()
    # print(process.pid)  # 进程号
    # process.join()
    # print('main process success!')

    # 使用多进程池编程
    # pool = mp.Pool(mp.cpu_count())
    # result =pool.apply_async(get_html, (3,))
    # # 关闭pool
    # pool.close()
    # # 等待所有任务完成
    # pool.join()
    # print(result.get())

    # 使用imap方法, 有序执行,且直接返回结果值
    # for result in pool.imap(get_html, [1, 5, 3]):
    #   print('{} sleep success'.format(result))
    # pool.close()

    # imap_unordered 与imap相似,但是谁先执行完成,谁先返回结果
    # for result in pool.imap_unordered(get_html, [1, 5, 3]):
    #   print('{} sleep success'.format(result))
    # pool.close()

    print('Parent process ({0})'.format(os.getpid()))
    p = Pool()
    for i in range(8):
        # p.apply_async(long_time_task, args=(i, ))
        p.apply_async(time_test, args=(i, ))
    print("Waiting for all subprocesses done...")
    p.close()
    p.join()
    print("All subprocesses done...")

