# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2019 yutiansut/QUANTAXIS modified by Eric 20190814
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime
import queue
import time
import click
from concurrent.futures import ThreadPoolExecutor,as_completed
from threading import Thread, Timer,enumerate

import pandas as pd
from pytdx.hq import TdxHq_API
import tushare as ts
# from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
dfcode=ts.get_stock_basics().index.tolist()
code = []
for i in dfcode:
    if i[:6] not in ['603755', '603992', '300789', '002959', '003816', '300787', '688321', '300028']:
        code.append(i[:6])

# code = code[:100]
stock_ip_list = ['47.103.48.45','47.103.86.229','47.103.88.146','120.79.60.82','47.112.129.66','39.98.234.173','39.98.198.249'
            ,'39.100.68.59']

"""
准备做一个多连接的连接池执行器Executor
当持续获取数据/批量数据的时候,可以减小服务器的压力,并且可以更快的进行并行处理
"""
class Tdx_Hq_API(TdxHq_API):
    def get_k_data(self, code, start_date, end_date):
        # 具体详情参见 https://github.com/rainx/pytdx/issues/5
        # 具体详情参见 https://github.com/rainx/pytdx/issues/21
        def __select_market_code(code):
            code = str(code)
            if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
                return 1
            return 0
        # 新版一劳永逸偷懒写法zzz
        market_code = 1 if str(code)[0] == '6' else 0
        # https://github.com/rainx/pytdx/issues/33
        # 0 - 深圳， 1 - 上海


        data=[self.get_security_bars(9, __select_market_code(
            code), code, (9 - i) * 800, 800) for i in range(10)]

        return (code,data)

class QA_Tdx_Executor():
    def __init__(self, thread_num=2, timeout=1, sleep_time=1, *args, **kwargs):
        # super().__init__(name='QATdxExecutor')
        self.thread_num = thread_num
        self._queue = queue.Queue(maxsize=50)
        self.api_no_connection = TdxHq_API()
        # self._api_worker = Thread(
        #     target=self.api_worker, args=(), name='API Worker')
        # self._api_worker.start()
        # self._api_worker.join()
        # time.sleep(10)
        self.timeout = timeout
        self.executor = ThreadPoolExecutor(self.thread_num)
        self.sleep_time = sleep_time

    def __getattr__(self, item):
        try:
            api = self.get_available()
            func = api.__getattribute__(item)

            def wrapper(*args, **kwargs):
                res = self.executor.submit(func, *args, **kwargs)
                self._queue.put(api)
                return res

            return wrapper
        except:
            return self.__getattr__(item)

    def _queue_clean(self):
        self._queue = queue.Queue(maxsize=50)

    def _test_speed(self, ip, port=7709):

        api = TdxHq_API(raise_exception=True, auto_retry=False)
        _time = datetime.datetime.now()
        # print(self.timeout)
        # print('in testspeed')
        try:
            with api.connect(ip, port):
                res = api.get_security_list(0, 1)
                print('res',ip,res)
                # print(len(res))
                if len(res) > 800:
                    return (datetime.datetime.now() - _time).total_seconds()
                else:
                    return datetime.timedelta(9, 9, 0).total_seconds()
        except Exception as e:
            return datetime.timedelta(9, 9, 0).total_seconds()

    def get_market(self, code):
        code = str(code)
        if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
            return 1
        return 0

    def get_frequence(self, frequence):
        if frequence in ['day', 'd', 'D', 'DAY', 'Day']:
            frequence = 9
        elif frequence in ['w', 'W', 'Week', 'week']:
            frequence = 5
        elif frequence in ['month', 'M', 'm', 'Month']:
            frequence = 6
        elif frequence in ['Q', 'Quarter', 'q']:
            frequence = 10
        elif frequence in ['y', 'Y', 'year', 'Year']:
            frequence = 11
        elif str(frequence) in ['5', '5m', '5min', 'five']:
            frequence = 0
        elif str(frequence) in ['1', '1m', '1min', 'one']:
            frequence = 8
        elif str(frequence) in ['15', '15m', '15min', 'fifteen']:
            frequence = 1
        elif str(frequence) in ['30', '30m', '30min', 'half']:
            frequence = 2
        elif str(frequence) in ['60', '60m', '60min', '1h']:
            frequence = 3

        return frequence

    @property
    def ipsize(self):
        return len(self._queue.qsize())

    @property
    def api(self):
        return self.get_available()

    def get_available(self):
        print('getting avaliable api')
        if self._queue.empty() is False:
            return self._queue.get_nowait()
        else:
            # Timer(0, self.api_worker).start()
            # print('start timer threading')
            # return self._queue.get()
            print('tdx api pool in empty')


    def close_api_connection(self):
        while self._quene.empty() is False:
            self._queue.get().disconnect()
            print('lenth of queue',self._queue.qsize())


    def api_worker(self):
        data = []
        if self._queue.qsize() < 20:
            for item in stock_ip_list:
                print('item, api_worker',item)
                if self._queue.full():
                    print('apiworkfull')
                    break
                _sec = self._test_speed(ip=item, port=7709)
                # print('testspeed',_sec)
                if _sec < self.timeout * 3:
                    print('put api in queue',item)
                    try:
                        self._queue.put(TdxHq_API(heartbeat=False).connect(
                            ip=item, port=7709))

                    except:
                        pass
        # print('api_work_sleep')
        # time.sleep(10)
        # print('api_work_continue')
        # else:
        #     self._queue_clean()
        #     Timer(0, self.api_worker).start()
        # Timer(300, self.api_worker).start()
        # print('api_worker',self.api_worker,self._api_worker)

    def _singal_job(self, context, id_, time_out=0.7):
        try:
            _api = self.get_available()

            __data = context.append(self.api_no_connection.to_df(_api.get_security_quotes(
                [(self._select_market_code(x), x) for x in code[80 * id_:80 * (id_ + 1)]])))
            __data['datetime'] = datetime.datetime.now()
            self._queue.put(_api)  # 加入注销
            return __data
        except:
            return self.singal_job(context, id_)

    def get_realtime(self, code):
        context = pd.DataFrame()

        code = [code] if isinstance(code, str) is str else code
        try:
            for id_ in range(int(len(code) / 80) + 1):
                context = self._singal_job(context, id_)

            data = context[['datetime', 'last_close', 'code', 'open', 'high', 'low', 'price', 'cur_vol',
                            's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
                            'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
                            'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5']]
            data['datetime'] = data['datetime'].apply(lambda x: str(x))
            return data.set_index('code', drop=False, inplace=False)
        except:
            return None

    def get_realtime_concurrent(self, code):
        code = [code] if isinstance(code, str) is str else code

        try:
            data = {self.get_security_quotes([(self.get_market(
                x), x) for x in code[80 * pos:80 * (pos + 1)]]) for pos in range(int(len(code) / 80) + 1)}
            return (pd.concat([self.api_no_connection.to_df(i.result()) for i in data]), datetime.datetime.now())
        except:
            pass

    def get_security_bar_concurrent(self, code, _type, lens):
        try:

            all_task = {self.get_security_bars(self.get_frequence(_type), self.get_market(
                str(code)), str(code), 0, lens) for code in code}
            data = [task.result() for task in as_completed(all_task)]
            # print("result",data)

            return data

        except:
            raise Exception

    def get_k_data_concurrent(self, code, start, end):
        try:

            all_task = {self.get_k_data(str(code), start, end) for code in code}
            data = [task.result() for task in as_completed(all_task)]
            # print("result",data)

            return data

        except:
            raise Exception

    def _get_security_bars(self, context, code, _type, lens):
        try:
            _api = self.get_available()
            for i in range(1, int(lens / 800) + 2):
                context.extend(_api.get_security_bars(self.get_frequence(
                    _type), self.get_market(str(code)), str(code), (i - 1) * 800, 800))
                print(context)
            self._queue.put(_api)
            return context
        except Exception as e:
            return self._get_security_bars(context, code, _type, lens)

    # def get_security_bar(self, code, _type, lens):
    #     code = [code] if isinstance(code, str) is str else code
    #     context = []
    #     try:
    #         for item in code:
    #             context = self._get_security_bars(context, item, _type, lens)
    #         return context
    #     except Exception as e:
    #         raise e



if __name__ == '__main__':
    api = QA_Tdx_Executor(thread_num=4)
    api.api_worker()
    starttime = time.time()
    # data = api.get_security_bar_concurrent(code,'d',100)
    # data = api.get_security_bar(code, 'd', 100)
    data = api.get_security_bar_concurrent(code,9,300)
    print(data)
    print(len(data),len(code))
    print(time.time()-starttime)
    print(enumerate())
    # api.close_api_connection()