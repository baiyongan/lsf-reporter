# -*- coding: utf-8 -*-

import asyncio
import random

# async def MyCoroutine(id):
#     process_time = random.randint(1, 5)
#     await asyncio.sleep(process_time)
#     print("协程: {}, 执行完毕。用时： {} 秒".format(id, process_time))
#
# async def main():
#     tasks = [asyncio.ensure_future(MyCoroutine(i)) for i in range(10)]
#     await asyncio.gather(*tasks)
#
# loop = asyncio.get_event_loop()
# try:
#     loop.run_until_complete(main())
# finally:
#     loop.close()




import threadpool
import asyncio
import time


async def do_work_one(name):
    await asyncio.sleep(4)
    print(name, 'do_work_one')


async def do_work_two(name):
    await asyncio.sleep(2)
    print(name, 'do_work_two')


def task_do_work(name):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    coroutine1 = do_work_one(name)
    coroutine2 = do_work_two(name)

    tasks = [asyncio.ensure_future(coroutine1), asyncio.ensure_future(coroutine2)]  # 多个tasks
    loop.run_until_complete(asyncio.wait(tasks))
    return 'success'


def call_back(param, result):
    print('回调', param, result)


if __name__ == '__main__':
    start_time = time.time()
    jobs = []
    pool = threadpool.ThreadPool(2)
    work_requests = []
    for i in range(2):
        work_requests.append(threadpool.WorkRequest(task_do_work, args=('线程-{0}'.format(i), ), callback=call_back, exc_callback=call_back))
    [pool.putRequest(req) for req in work_requests]
    pool.wait()
    print('end')

    end_time = time.time()

    print("total time is :" + str(end_time - start_time))
